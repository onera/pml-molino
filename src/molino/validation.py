import logging
from collections.abc import Iterable, Iterator
from functools import reduce
from itertools import combinations, product
from math import isclose

from peewee import JOIN

from molino.transactions import (
    Classification,
    MultiToTransactions,
    MultiTransaction,
    prepare_transactions_db,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
debug = logger.debug


def split_transaction(name: str) -> set[str]:
    """Split multi-transaction into its component transactions."""
    return {t.strip() for t in name.split("||")}


def name_transaction(transactions: Iterable[str]) -> str:
    """Get unique multi-transaction name from its transactions."""
    return "||".join(sorted(transactions))


def is_transaction_redundant(name: str, threshold: float = 0.05) -> bool:
    """Assess if a multi-transaction is redundant as per the observations."""
    trs = split_transaction(name)
    mtr = name_transaction(trs)
    debug("Check if multi-transaction %s is redundant.", mtr)

    # Skip free or interfering transactions
    if not _classified_as_redundant(mtr):
        return False

    # Explore candidate partitions from the largest
    for candidate in _combinations_of(trs, least=2, most=len(trs) - 1):
        neutral = trs - candidate
        debug(
            "...Check candidate partition %s against %s",
            name_transaction(candidate),
            name_transaction(neutral),
        )
        if is_transaction_redundant_with(trs, candidate, threshold=threshold):
            return True

    return False


def _classified_as(transaction: str) -> Classification | None:
    """Fetch classification for a transaction."""
    t = (
        MultiTransaction.select(MultiTransaction.id)
        .where(MultiTransaction.name == transaction)
        .scalar()
    )
    return Classification.get_or_none(Classification.transaction == t)


def _classified_as_free(transaction: str) -> bool:
    """Check if the transaction is free of interference, as classified PML."""
    return (c := _classified_as(transaction)) is not None and c.interfering == 0


def _classified_as_redundant(transaction: str) -> bool:
    """Check if the transaction is classified as redundant by PML."""
    return _classified_as(transaction) is None


def _classified_as_interfering(transaction: str) -> bool:
    """Check if the transaction is classified as interfering by PML."""
    return (c := _classified_as(transaction)) is not None and c.interfering == 1


def _observed_as_interfering(
    victim: set[str],
    aggressor: set[str],
    *,
    strict: bool = False,
    rtol: float = 0.05,
) -> bool:
    """Check if the victim multi-transaction is impacted by the aggressor one."""
    mtr_v = name_transaction(victim)
    mtr_a = name_transaction(aggressor)
    mtr = name_transaction(victim | aggressor)
    f_name = MultiToTransactions.tr_name
    f_mtr = MultiTransaction.name
    f_mid = MultiTransaction.id
    f_tid = MultiToTransactions.multi_transaction
    f_hwm = MultiToTransactions.hwm
    f_lwm = MultiToTransactions.lwm
    w_victim = MultiToTransactions.tr_name << victim
    # Get reference observations for the victim on its own
    q = (
        MultiToTransactions.select(f_name, f_hwm)
        .join(MultiTransaction, on=(f_mid == f_tid))
        .where(w_victim & (f_mtr == mtr_v))
    )
    obs_iso: dict[str, float] = dict(q.tuples())
    # Get comparison observations for victim || aggressor
    q = (
        MultiToTransactions.select(f_name, f_lwm)
        .join(MultiTransaction, on=(f_mid == f_tid))
        .where(w_victim & (f_mtr == mtr))
    )
    obs_itf: dict[str, float] = dict(q.tuples())
    # Compare observations
    for v in victim:
        if (iso := obs_iso.get(v)) is not None and (itf := obs_itf.get(v)) is not None:
            if isclose(iso, itf, rel_tol=rtol):
                debug(
                    ".........%s is not impacted by %s between %s (%f) and %s (%f)",
                    v,
                    mtr_a,
                    mtr_v,
                    iso,
                    mtr,
                    itf,
                )
            else:
                debug(
                    ".........%s impacted by %s between %s (%f) and %s (%f)",
                    v,
                    mtr_a,
                    mtr_v,
                    iso,
                    mtr,
                    itf,
                )
                return True
        else:
            debug(
                ".........Cannot validate %s is not impacted between %s and %s",
                v,
                mtr_v,
                mtr,
            )
            if strict:
                raise KeyError(v)
    return False


def _combinations_of(
    *sets: set[str],
    least: int = 1,
    most: int | None = None,
) -> Iterator[set[str]]:
    """Yield all combinations of elements of the specified sets which include at least one element from each set."""

    def _combinations_in_set(contents: set[str]) -> Iterator[set[str]]:
        """Yield combinations of all lengths in the specified set."""
        m = min(most, len(contents)) if most is not None else len(contents)
        for s in range(least, m + 1):
            yield from map(set, combinations(contents, s))

    for c in product(*[_combinations_in_set(t) for t in sets]):
        yield reduce(lambda a, b: a | b, c, set())


def is_transaction_redundant_with(
    transaction: set[str],
    candidate: set[str],
    *,
    strict: bool = False,
    threshold: float = 0.05,
    ignore_classification: bool = False,
) -> bool:
    """Assess if multi-transaction is redundant w.r.t. a candidate multi-transaction.

    The check ensures that no combinations of transactions from the candidate and
    neutral sets interfere with each other. The algorithm iterates over combinations of
    candidate and neutral transactions, that is multi-transactions included in the
    original and featuring both candidate and neutral transactions.

    For each combination, it checks that the combined multi-transaction is not
    classified as interfering, and that measurements are consistent with the
    classification. For each transaction in the combination, measurements in its
    reference scenario must match the ones in the combined multi-transaction. The
    reference scenario for a transaction is the multi-transaction composed of only
    transactions of the same kind type, candidate or neutral.

    As an example, consider multi-transaction A || B || C || D. To check if it is
    redundant with multi-transaction A || B, the following checks will be performed:
    - Measurements for A and C in isolation are not impacted in A || C, and similarly
      for A and D, B and C, and B and D.
    - Measurements for A and B in reference scenario A || B are not impacted in
      A || B || C, A || B || D, or A || B || C || D. And conversely, measurements for C
      and D, in reference scenario C || D, are not impacted in A || C || D, etc.

    TODO How to trim the search space?
    """
    candidate_mtr = name_transaction(candidate)
    mtr = name_transaction(transaction)

    # Check the candidate is a subset of the target
    if not candidate.issubset(transaction):
        debug("...Candidate %s is not a subset of %s", candidate_mtr, mtr)
        return False

    # Check the candidate is interfering
    if not ignore_classification and not _classified_as_interfering(candidate_mtr):
        debug("...Candidate %s is not classified as interfering", candidate_mtr)
        return False
    debug("...Candidate %s is classified as interfering", candidate_mtr)

    # Explore the combinations of transactions from the candidate and neutral sets.
    neutral = transaction - candidate
    for s in _combinations_of(candidate, neutral):
        tr_candidate, tr_neutral = s - neutral, s - candidate
        n_candidate = name_transaction(tr_candidate)
        n_neutral = name_transaction(tr_neutral)
        n_combination = name_transaction(tr_candidate | tr_neutral)
        debug("......Check %s is not impacted by %s", n_candidate, n_neutral)
        # Check not classified as interfering
        if not ignore_classification and _classified_as_interfering(n_combination):
            return False
        # Check if neutral impacts candidate
        if _observed_as_interfering(
            tr_candidate,
            tr_neutral,
            strict=strict,
            rtol=threshold,
        ):
            return False
        # Check if candidate impacts neutral
        if _observed_as_interfering(
            tr_neutral,
            tr_candidate,
            strict=strict,
            rtol=threshold,
        ):
            return False
    return True


def _find_classified_as_redundant() -> Iterator[str]:
    f_name = MultiTransaction.name
    f_tid = MultiTransaction.id
    f_cid = Classification.transaction
    f_mid = MultiToTransactions.multi_transaction
    f_itf = Classification.interfering
    f_arity = MultiToTransactions.arity
    q = (
        MultiTransaction.select(
            f_name.distinct(),
        )
        .join(MultiToTransactions, JOIN.LEFT_OUTER)
        .join(
            Classification,
            JOIN.LEFT_OUTER,
            on=(f_cid == f_mid),
        )
        .where(
            f_itf.is_null() & (f_arity > 2),
        )
        .tuples()
    )
    yield from (t[0] for t in q)


if __name__ == "__main__":
    prepare_transactions_db("tests/transactions.sqlite")

    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    # Check all candidate redundant transactions
    for mtr in _find_classified_as_redundant():
        r = is_transaction_redundant(mtr, threshold=0.05)
        print(f"Is {mtr} redundant? {r}")
        input()

    mtr = "C0_CoPh_BK2||C1_CoPh_BK4||C2_CoPh_BK2"
    r = is_transaction_redundant(mtr)
    print(f"Is {mtr} redundant? {r}")
