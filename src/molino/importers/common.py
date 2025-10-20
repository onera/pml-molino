import re
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from itertools import islice
from pathlib import Path
from typing import Any, Callable, Protocol, Iterator, Iterable

from peewee import EXCLUDED, fn, chunked
from tqdm import tqdm

from molino import Resource
# FIXME Common importer API should use the existing create_or_fetch_transactions and create_or_fetch_multitransactions methods
from molino.transactions import (
    AtomicTransaction,
    MultiToTransactions,
    MultiTransaction,
    Resource,
    Transaction,
    transactions_db, Location, Observation,
)


@contextmanager
def open_compressed_file(filename: Path) -> Any:
    """Open a (compressed) file."""
    if not filename.exists():
        raise FileNotFoundError(filename)
    match [s.lower() for s in reversed(filename.suffixes)]:
        case [".bz2", *_]:
            import bz2

            with bz2.open(filename, "rt") as file:
                yield file
        case _:
            with filename.open() as file:
                yield file


def create_or_fetch_transactions(
    names: Iterable[str],
    *,
    create_missing: bool = True,
    watermarks: dict[tuple[str, str], tuple[float, float]] | None = None,
    parse_transaction_name: Callable[[str,], list[tuple[str, str, bool, bool, str | None]]] = None,
) -> dict[str, Transaction]:
    """Get or create a batch of transaction atoms by names.

    `parse_transaction_name` is a function that takes a transaction name and
    returns a list of atomic transactions. Each atomic transaction captures
    - the name of the initiator
    - the name of the target
    - if the atomic transaction is a load
    - if the atomic transaction is a store
    - the page hit/miss status of the atomic transaction (resp. "Ph"/"Pm")
    """
    # Access fields: initiator, target, is load, is store, page access

    names = list(names)

    if watermarks is None:
        watermarks = {}
    else:
        watermarks = {(n, n): watermarks[(n, n)] for n in names if (n, n) in watermarks}

    # Identify existing transaction
    db_atoms = {n.name for n in Transaction if n.name in names}

    # Create missing transactions, and update watermarks
    pending_transactions = {}
    for n in names:
        if n in db_atoms or create_missing:
            pending_transactions[n] = parse_transaction_name(n)

    # Create all required resources
    resources = set()
    for accesses in pending_transactions.values():
        for a in accesses:
            resources.add((a[0], "I"))
            resources.add((a[1], "E"))
    db_resources = {}
    for r, t in resources:
        db_resources[r] = Resource.get_or_create(name=r, defaults={"role": t})[0]

    # Create all required transactions
    created_transactions = {
        n: (
            n,
            watermarks.get((n, n), (None, None))[0],
            watermarks.get((n, n), (None, None))[1],
        )
        for n in pending_transactions
    }
    Transaction.insert_many(
        created_transactions.values(),
        fields=[Transaction.name, Transaction.lwm, Transaction.hwm],
    ).on_conflict(
        conflict_target=[
            Transaction.name,
        ],
        update={
            Transaction.lwm: fn.MIN(
                fn.IFNULL(Transaction.lwm, EXCLUDED.lwm),
                fn.IFNULL(EXCLUDED.lwm, Transaction.lwm),
            ),
            Transaction.hwm: fn.MAX(
                fn.IFNULL(Transaction.hwm, EXCLUDED.hwm),
                fn.IFNULL(EXCLUDED.hwm, Transaction.hwm),
            ),
        },
        where=(
            (EXCLUDED.lwm.is_null(False))
            & (
                (Transaction.lwm > EXCLUDED.lwm)
                | Transaction.lwm.is_null()
                | (Transaction.hwm < EXCLUDED.hwm)
                | Transaction.hwm.is_null()
            )
        ),
    ).execute()

    # Create all required atomic transactions
    created_atoms = []
    for n in pending_transactions:
        # Skip existing transactions
        if n in db_atoms:
            continue
        for i, e, r, w, p in pending_transactions[n]:
            # Fix to get created atoms ids (see bulk_create documentation)
            created_transactions[n] = Transaction.get(Transaction.name == n)
            created_atoms.append(
                AtomicTransaction(
                    transaction=created_transactions[n],
                    initiator=db_resources[i],
                    target=db_resources[e],
                    is_load=r,
                    is_store=w,
                    page_access=p,
                ),
            )
    AtomicTransaction.bulk_create(created_atoms, batch_size=1024)

    # Collect transaction ids from the database
    # (related to a PEEWEE issue on bulk_create calls)
    return {t.name: t for t in Transaction if t.name in names}


def _batched(iterable: Iterable, n: int) -> Iterator[Iterator]:
    if n < 1:
        raise ValueError("n must be at least one")
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        yield batch


def create_or_fetch_multitransactions(
    names: Iterable[str],
    *,
    create_missing: bool = True,
    batch: int = 2048,
    watermarks: dict[tuple[str, str], tuple[float, float]] | None = None,
    parse_transaction_name: Callable[[str,], list[tuple[str, str, bool, bool, str | None]]] = None,
) -> None:
    """Create or retrieve the entries for the named Multi-transactions."""
    if watermarks is None:
        watermarks = {}

    # Collect transaction names per multi-transaction
    mtr_transactions: dict[str, list[str]] = {}
    for n in names:
        trs = sorted(t.strip() for t in n.split("||"))
        mtr = "||".join(trs)
        mtr_transactions[mtr] = trs

    cnt_mtr = len(mtr_transactions)
    cnt_ctx = sum(map(len, mtr_transactions.values()))

    # Collect, and fetch or create all transactions (indexed by name)
    with transactions_db.atomic():
        tr_names: set[str] = set()
        for trs in mtr_transactions.values():
            tr_names.update(trs)
        tr_entries: dict[str, Transaction] = create_or_fetch_transactions(
            tr_names,
            create_missing=create_missing,
            watermarks=watermarks,
            parse_transaction_name=parse_transaction_name,
        )

    # Create missing multi-transaction and contexts
    with transactions_db.atomic():
        if create_missing:
            progress = tqdm(
                desc="Create multi-transactions",
                total=cnt_mtr,
            )
            for names_batch in _batched(mtr_transactions, batch):
                MultiTransaction.insert_many(
                    ((n,) for n in names_batch),
                    fields=[MultiTransaction.name],
                ).on_conflict_ignore().execute()
                progress.update(batch)
            progress.update(progress.total % batch)
            progress.close()

        # Collect multi-transactions
        mtr_entries: dict[str, int] = dict(
            tqdm(
                MultiTransaction.select(
                    MultiTransaction.name,
                    MultiTransaction.id,
                ).tuples(),
                desc="Collect multi-transactions",
            ),
        )

        # Create missing transaction contexts
        if create_missing:
            progress = tqdm(desc="Create transaction contexts", total=cnt_ctx)
            for tr_batch in _batched(
                (
                    (n, mtr_entries[n], t)
                    for n in mtr_transactions
                    for t in mtr_transactions[n]
                ),
                n=batch,
            ):
                MultiToTransactions.insert_many(
                    (
                        (
                            m,
                            tr_entries[t].id,
                            t,
                            len(mtr_transactions[n]),
                            *tuple(watermarks.get((n, t), (None, None))),
                        )
                        for n, m, t in tr_batch
                    ),
                    fields=[
                        MultiToTransactions.multi_transaction,
                        MultiToTransactions.transaction,
                        MultiToTransactions.tr_name,
                        MultiToTransactions.arity,
                        MultiToTransactions.lwm,
                        MultiToTransactions.hwm,
                    ],
                ).on_conflict(
                    conflict_target=[
                        MultiToTransactions.multi_transaction,
                        MultiToTransactions.transaction,
                    ],
                    update={
                        MultiToTransactions.lwm: fn.MIN(
                            fn.IFNULL(MultiToTransactions.lwm, EXCLUDED.lwm),
                            fn.IFNULL(EXCLUDED.lwm, MultiToTransactions.lwm),
                        ),
                        MultiToTransactions.hwm: fn.MAX(
                            fn.IFNULL(MultiToTransactions.hwm, EXCLUDED.hwm),
                            fn.IFNULL(EXCLUDED.hwm, MultiToTransactions.hwm),
                        ),
                    },
                    where=(
                            (EXCLUDED.lwm.is_null(False))
                            & (
                                    (MultiToTransactions.lwm > EXCLUDED.lwm)
                                    | MultiToTransactions.lwm.is_null()
                                    | (MultiToTransactions.hwm < EXCLUDED.hwm)
                                    | MultiToTransactions.hwm.is_null()
                            )
                    ),
                ).execute()
                progress.update(batch)
            progress.update(progress.total % batch)
            progress.close()

    # Update transaction contexts low and high watermarks
    if not create_missing and len(watermarks) > 0:
        with transactions_db.atomic():
            mtt_entries: list[MultiToTransactions] = []
            for n in tqdm(mtr_entries.values(), desc="Update watermarks"):
                mtt = list(
                    MultiToTransactions.filter(
                        MultiToTransactions.multi_transaction == n,
                    ),
                )
                for m in mtt:
                    lwm, hwm = watermarks.get(
                        (m.multi_transaction.name, m.tr_name), (None, None)
                    )
                    if lwm is not None and m.lwm is not None and lwm < m.lwm:
                        m.lwm = lwm
                    if hwm is not None and m.hwm is not None and hwm < m.hwm:
                        m.hwm = hwm
            MultiToTransactions.bulk_update(
                mtt_entries,
                [MultiToTransactions.lwm, MultiToTransactions.hwm],
                batch_size=2048,
            )


class AtomEntry(Protocol):
    initiator: str
    target: str

    service: str
    is_load: bool
    is_store: bool


class TransactionEntry(Protocol):
    name: str
    atoms: list[AtomEntry]


class ObservationEntry(Protocol):
    """Observation information for data entry."""
    mtr: str
    victim: str
    transactions: list[TransactionEntry]
    value: float

    location: Path
    location_line: int


def register_observations(
    observations: Iterator[ObservationEntry],
    chunk_size: int = 10000,
) -> None:
    """Load observations from ALF trace into database."""
    with transactions_db.atomic():
        for batch in chunked(observations, chunk_size):
            obs_batch: list[ObservationEntry] = list(batch)
            # Collect and create locations
            locations: list[Location] = []
            for observation in obs_batch:
                location: Location = Location.get_or_create(url=str(observation.location))[0]
                locations.append(location)
            #  Collect and create resources
            resources: dict[str, str] = {}
            db_resources: dict[str, Resource]
            roles = Resource.roles()
            for observation in obs_batch:
                for transaction in observation.transactions:
                    for atom in transaction.atoms:
                        # FIXME Fail if resources already exists with a different role
                        resources[atom.initiator] = roles["Initiator"]
                        resources[atom.target] = roles["Target"]
            db_resources = {
                r.name: r
                for r in create_or_fetch_resources(
                    (name, role) for name, role in resources.items()
                )
            }
            # Create missing transactions
            transactions: set[str] = set()
            for observation in obs_batch:
                transactions.update(t.name for t in observation.transactions)
            db_transactions = {t.name: t for t in create_transactions(transactions)}
            # Create transaction atoms for newly created transactions
            atoms: set[
                tuple[Transaction, Resource, Resource, bool, bool, str | None]
            ] = set()
            for observation in obs_batch:
                for transaction in observation.transactions:
                    db_transaction = db_transactions.get(transaction.name)
                    if db_transaction is not None:
                        for atom in transaction.atoms:
                            atoms.add(
                                (
                                    db_transactions[transaction.name],
                                    db_resources[atom.initiator],
                                    db_resources[atom.target],
                                    atom.is_load,
                                    atom.is_store,
                                    atom.service,
                                )
                            )
            db_atoms = list(create_atoms(atoms))
            # Fetch previously existing transactions
            for t in transactions:
                if t not in db_transactions:
                    db_transactions[t] = Transaction.get(Transaction.name == t)
            # Create new multi-transactions
            mtrs: set[str] = {i.mtr for i in obs_batch}
            db_mtrs: dict[str, MultiTransaction] = {
                m.name: m for m in create_multitransactions(iter(mtrs))
            }
            # Collect existing multi-transactions
            for m in mtrs:
                if m not in db_mtrs:
                    db_mtrs[m] = MultiTransaction.get(MultiTransaction.name == m)
            # Create Contexts
            contexts: set[tuple[MultiTransaction, Transaction, int]] = set()
            for observation in obs_batch:
                contexts.add(
                    (
                        db_mtrs[observation.mtr],
                        db_transactions[observation.victim],
                        len(observation.transactions),
                    )
                )
            db_contexts: dict[tuple[str, str], MultiToTransactions] = {
                (c.multi_transaction.name, c.transaction.name): c
                for c in create_contexts(iter(contexts))
            }
            # Collect existing contexts
            for observation in obs_batch:
                k = (observation.mtr, observation.victim)
                if k not in db_contexts:
                    db_contexts[k] = MultiToTransactions.get(
                        (
                                MultiToTransactions.multi_transaction
                                == db_mtrs[observation.mtr]
                        )
                        & (
                                MultiToTransactions.transaction
                                == db_transactions[observation.victim]
                        ),
                        )
            # Record observations
            db_obs: list[Observation] = list(
                create_observations(
                    (
                        (
                            db_mtrs[observation.mtr],
                            db_transactions[observation.victim],
                            float(observation.value),
                            locations[i],
                            observation.location_line,
                        )
                        for i, observation in enumerate(obs_batch)
                    ),
                )
            )


def create_or_fetch_resources(
        resources: Iterator[tuple[str, str]],
        chunk_size: int = 1024,
) -> Iterator[Resource]:
    """Create or fetch existing resources from the database.

    Creation should fail if the resource already exists with a different role.
    """
    for batch in chunked(resources, chunk_size):
        q = Resource.insert_many(
            batch,
            fields=[Resource.name, Resource.role],
        # FIXME Find a way to fail on existing resource with a different role
        # - ).on_conflict(
        # - action="abort",
        # - conflict_target=[Resource.name],
        # - where=(Resource.role != EXCLUDED.role),
        ).on_conflict_replace().returning(
            Resource,
        )
        yield from q.execute()


def create_atoms(
        atoms: Iterable[tuple[Transaction, Resource, Resource, bool, bool, str | None]],
        chunk_size: int = 1024,
) -> Iterator[Transaction]:
    """Create a batch of atoms.

    Note that there is no way to detect conflicts upon atom creations. Only
    atoms for newly created transactions should be created.
    """
    for batch in chunked(atoms, chunk_size):
        q = (
            AtomicTransaction.insert_many(
                batch,
                fields=[
                    AtomicTransaction.transaction,
                    AtomicTransaction.initiator,
                    AtomicTransaction.target,
                    AtomicTransaction.is_load,
                    AtomicTransaction.is_store,
                    AtomicTransaction.page_access,
                ],
            ).returning(AtomicTransaction)
        )
        yield from q.execute()


def create_transactions(
        names: Iterable[str],
        chunk_size: int = 1024,
) -> Iterator[Transaction]:
    """Create a batch of transactions by names."""
    for batch in chunked(names, chunk_size):
        q = Transaction.insert_many(
            ((n, None, None) for n in batch),
            fields=[Transaction.name, Transaction.lwm, Transaction.hwm],
        ).on_conflict_ignore().returning(Transaction)
        yield from q.execute()


def create_multitransactions(
        names: Iterator[str],
        chunk_size: int = 1024,
) -> Iterator[MultiTransaction]:
    """Create existing multi-transactions from the database."""
    for batch in chunked(names, chunk_size):
        q = MultiTransaction.insert_many(
            ((n,) for n in batch),
            fields=[MultiTransaction.name],
        ).on_conflict_ignore().returning(MultiTransaction)
        yield from q.execute()


def create_contexts(
        mttr: Iterator[tuple[MultiTransaction, Transaction, int]],
        chunk_size: int = 1024,
) -> Iterator[MultiToTransactions]:
    """Create multi-to-transactions in the database."""
    for batch in chunked(mttr, chunk_size):
        q = MultiToTransactions.insert_many(
            ((m, t, t.name, a) for (m, t, a) in batch),
            fields=[
                MultiToTransactions.multi_transaction,
                MultiToTransactions.transaction,
                MultiToTransactions.tr_name,
                MultiToTransactions.arity,
            ],
        ).on_conflict_ignore().returning(MultiToTransactions)
        yield from q.execute()


def create_observations(
        observations: Iterator[tuple[MultiTransaction, Transaction, float, Location, int]],
        chunk_size: int = 1024,
) -> Iterator[Observation]:
    """Create a batch of observations in the database."""
    for batch in chunked(observations, chunk_size):
        q = Observation.insert_many(
            batch,
            fields=[
                Observation.multi_transaction,
                Observation.transaction,
                Observation.value,
                Observation.location,
                Observation.location_line,
            ],
        ).returning(Observation)
        yield from q.execute()
