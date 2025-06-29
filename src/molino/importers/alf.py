"""Importer for ALF traces."""
import io
from collections.abc import Generator, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path

from peewee import chunked

from molino.transactions import (
    AtomicTransaction,
    Location,
    MultiToTransactions,
    MultiTransaction,
    Observation,
    Resource,
    Transaction,
    prepare_transactions_db,
    transactions_db,
)


@dataclass
class AlfTransaction:
    """ALF-style transaction definition."""

    initiator: str
    service: str
    target: str

    @property
    def name(self) -> str:
        """Get transaction unique name."""
        return f"{self.initiator}_{self.service}_{self.target}"

    def __str__(self) -> str:
        """Get transaction str representation."""
        return self.name


@dataclass
class AlfObservation:
    """ALF-style observation."""

    bid: int
    lid: int
    transactions: tuple[AlfTransaction, ...]
    measurements: frozenset[tuple[str, int]]

    @property
    def mtr(self) -> str:
        """Get multi-transaction name."""
        return "||".join(sorted(t.name for t in self.transactions))

    @property
    def victim(self) -> str:
        """Get victim transaction name."""
        return self.transactions[0].name

    @property
    def cycles(self) -> int:
        """Get execution time in cycles for observation."""
        return dict(self.measurements)["mcycle"]

    @property
    def normalised_cycles(self) -> float:
        """Compute cycles per transaction."""
        m = None
        match self.transactions[0].service:
            case "Load":
                m = "Integer load inst ret"
            case "Store":
                m = "Integer store inst ret"
            case _:
                msg = f"Unknown service type {self.transactions[0].service}"
                raise ValueError(msg)
        return self.cycles / dict(self.measurements)[m]



@contextmanager
def open_alf(filename: Path) -> io.TextIOWrapper:
    """Open a (compressed) ALF file."""
    match [s.lower() for s in reversed(filename.suffixes)]:
        case [".alf", *_] | [".txt", *_]:
            with filename.open() as alf_file:
                yield alf_file
        case [".bz2", ".alf", *_] | [".bz2", *_]:
            import bz2

            with bz2.open(filename, "rt") as alf_file:
                yield alf_file
        case _:
            msg = f"Unknown ALF format for {filename}"
            raise ValueError(msg)


def _read_alf_blocks(alf_path: Path) -> Iterator[Iterator[tuple[int, str]]]:
    """Split an ALF file into groups of lines in the same context."""
    with (open_alf(alf_path) as alf_file):
        i, m = 1, next(alf_file)
        m = m.strip()
        assert m == "Number of transactions", f"Got: '{m}'"

        def _read_block(
                f: io.TextIOWrapper,
                i: int,
        ) -> Generator[tuple[int, str], None, tuple[int, str|None]]:
            for (j, m) in enumerate(f, start=i):
                line = m.strip()
                if line != "Number of transactions":
                    yield j, line
                else:
                    break
            else:
                return j, None
            return j, line

        while m is not None:
            def _get_block_contents() -> Iterator[tuple[int, str]]:
                nonlocal i
                nonlocal m
                yield i, m
                (i, m) = yield from _read_block(alf_file, i + 1)
            yield _get_block_contents()


class AlfParserState(Enum):
    PENDING_TAG = auto()
    PENDING_TRANSACTION_TYPE = auto()
    PENDING_TRANSACTION_INIT = auto()
    PENDING_TRANSACTION_TARGET = auto()
    PENDING_EVENT_NAME = auto()
    PENDING_MEASUREMENTS = auto()


def _parse_alf_block_observations(bid, contents: Iterator[tuple[int, str]]) -> Iterator[AlfObservation]:
    status: AlfParserState = AlfParserState.PENDING_TAG
    events: dict[int, str] = {}
    transaction_id = -1
    transaction_type = ""
    transaction_init = ""
    transaction_tgt = ""
    transactions: list[AlfTransaction] = []
    event_id = -1
    for i, line in contents:
        match status:
            case AlfParserState.PENDING_TAG:
                if line.startswith("Transaction "):
                    transaction_id = int(line.rsplit(" ", 1)[1])
                    status = AlfParserState.PENDING_TRANSACTION_TYPE
                elif line.startswith("Event "):
                    event_id = int(line.rsplit(" ", 1)[1])
                    status = AlfParserState.PENDING_EVENT_NAME
                elif line == "Measurements":
                    status = AlfParserState.PENDING_MEASUREMENTS
                # TODO Do something if unknown tag/unexpected line
            case AlfParserState.PENDING_TRANSACTION_TYPE:
                transaction_type = line
                status = AlfParserState.PENDING_TRANSACTION_INIT
            case AlfParserState.PENDING_TRANSACTION_INIT:
                transaction_init = line
                status = AlfParserState.PENDING_TRANSACTION_TARGET
            case AlfParserState.PENDING_TRANSACTION_TARGET:
                transaction_tgt = line
                assert transaction_id == len(transactions)
                # FIX Remove unsupported characters for PML transaction names
                transaction_init = transaction_init.replace("$", "")
                transaction_type = transaction_type.replace("$", "")
                transaction_tgt = transaction_tgt.replace("$", "")
                transactions.append(
                    AlfTransaction(
                        transaction_init,
                        transaction_type,
                        transaction_tgt,
                    ),
                )
                status = AlfParserState.PENDING_TAG
            case AlfParserState.PENDING_EVENT_NAME:
                event_name = line
                events[event_id] = event_name
                status = AlfParserState.PENDING_TAG
            case AlfParserState.PENDING_MEASUREMENTS:
                obs = line.split(" ", 1)[1]
                yield AlfObservation(
                    bid,
                    i,
                    tuple(transactions),
                    frozenset(
                        (events[e], int(o))
                        for e, o in enumerate(obs.split(" "), start=0)
                    ),
                )


def parse_alf_observations(alf_path: Path) -> Iterator[AlfObservation]:
    """Parse ALF-style observations from file."""
    bid = None
    for i, b in enumerate(_read_alf_blocks(alf_path)):
        observations = _parse_alf_block_observations(i, b)
        next(observations) # Skip first observation of block
        yield from observations


def _create_or_fetch_resources(
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


def _create_transactions(
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


def _create_atoms(
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


def _create_multitransactions(
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


def _create_contexts(
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


def _create_observations(
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



def load_observations(
        *traces: str | Path,
        chunk_size: int  = 10000,
) -> None:
    """Load observations from ALF trace into database."""
    for trace_path in map(Path, traces):
        with (transactions_db.atomic()):
            location: Location = Location.get_or_create(url=str(trace_path))[0]
            for batch in chunked(parse_alf_observations(trace_path), chunk_size):
                observations: list[AlfObservation] = list(batch)
                #  Collect and create resources
                resources: dict[str, str] = {}
                db_resources: dict[str, Resource]
                roles = Resource.roles()
                for observation in observations:
                    for transaction in observation.transactions:
                        # FIXME Fail if resources already exists with a different role
                        resources[transaction.initiator] = roles["Initiator"]
                        resources[transaction.target] = roles["Target"]
                db_resources = {
                    r.name: r
                    for r in _create_or_fetch_resources(
                        (name, role) for name, role in resources.items()
                    )
                }
                # Create missing transactions
                transactions: set[str] = set()
                for observation in observations:
                    transactions.update(t.name for t in observation.transactions)
                db_transactions = {
                    t.name: t
                    for t in _create_transactions(transactions)
                }
                # Create transaction atoms for newly created transactions
                atoms: set[tuple[Transaction, Resource, Resource, bool, bool, str|None]] = set()
                for observation in observations:
                    for transaction in observation.transactions:
                        db_transaction = db_transactions.get(transaction.name)
                        if db_transaction is not None:
                            atoms.add((
                                db_transactions[transaction.name],
                                db_resources[transaction.initiator],
                                db_resources[transaction.target],
                                transaction.service == "Load",
                                transaction.service == "Store",
                                None,
                            ))
                db_atoms = list(_create_atoms(atoms))
                # Fetch previously existing transactions
                for t in transactions:
                    if t not in db_transactions:
                        db_transactions[t] = Transaction.get(Transaction.name == t)
                # Create new multi-transactions
                mtrs: set[str] = {i.mtr for i in observations}
                db_mtrs: dict[str, MultiTransaction] = {
                    m.name: m
                    for m in _create_multitransactions(iter(mtrs))
                }
                # Collect existing multi-transactions
                for m in mtrs:
                    if m not in db_mtrs:
                        db_mtrs[m] = MultiTransaction.get(MultiTransaction.name == m)
                # Create Contexts
                contexts: set[tuple[MultiTransaction, Transaction, int]] = set()
                for observation in observations:
                    contexts.add((
                        db_mtrs[observation.mtr],
                        db_transactions[observation.victim],
                        len(observation.transactions),
                    ))
                db_contexts: dict[tuple[str, str], MultiToTransactions] = {
                    (c.multi_transaction.name, c.transaction.name): c
                    for c in _create_contexts(iter(contexts))
                }
                # Collect existing contexts
                for observation in observations:
                    k = (observation.mtr, observation.victim)
                    if k not in db_contexts:
                        db_contexts[k] = MultiToTransactions.get(
                            (MultiToTransactions.multi_transaction == db_mtrs[observation.mtr])
                            & (MultiToTransactions.transaction == db_transactions[observation.victim]),
                        )
                # Record observations
                db_obs: list[Observation] = list(_create_observations(
                    (
                        (
                            db_mtrs[observation.mtr],
                            db_transactions[observation.victim],
                            float(observation.normalised_cycles),
                            location,
                            observation.lid,
                        )
                        for observation in observations
                    ),
                ))
            print(trace_path.absolute(), "imported.")


if __name__ == "__main__":
    OBS_ROOT = Path("../../../samples-riscv/obs/")
    # - alf_path = OBS_ROOT / "example_of_trace.txt"
    # - alf_path = OBS_ROOT / "example_of_trace_batch0_aggressors_short_u7_locking.txt"
    alf_path = OBS_ROOT / "example_of_trace_batch0_aggressors_short_u7.txt"

    db_path = alf_path.parent.parent / "mol" / f"{alf_path.stem}.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Prepare empty database
    db_path.unlink(missing_ok=True)
    prepare_transactions_db(str(db_path))

    # Insert observations
    load_observations(alf_path, chunk_size=1024)

    # Insert classification
    # from molino.importers.pml import load_pml_classifications
    # pml_root = Path("../../../samples-riscv/pml")
    # itf_paths = list(pml_root.glob("FU740Benchmark*_itf_[0-9].txt"))
    # free_paths = list(pml_root.glob("FU740Benchmark*_free_[0-9].txt"))
    # load_pml_classifications(*(free_paths + itf_paths), create_missing=False)
