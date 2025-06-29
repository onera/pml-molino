import logging
from dataclasses import dataclass, field
from functools import reduce
from logging import Logger
from pathlib import Path

from peewee import (
    JOIN,
    BooleanField,
    CharField,
    FixedCharField,
    FloatField,
    TextField,
    fn,
)

from molino.transactions import (
    AtomicTransaction,
    Location,
    MultiToTransactions,
    Observation,
    Tool,
    Transaction,
    ViewGuiTransaction,
)


@dataclass
class Justification:
    """Justification for an observation, classification..."""

    tool: str|None = field(default=None)
    file: str|None = field(default=None)
    line: int = field(default=-1)


@dataclass
class Measurement:
    """Timing measurement."""

    value: float|None = field(default=None)
    justifications: list[Justification] = field(default_factory=list)


@dataclass
class TimingBounds:
    """Observed timing bounds."""

    lwm: Measurement = field(default_factory=Measurement)
    hwm: Measurement = field(default_factory=Measurement)


@dataclass
class AccessModel:
    """Transaction access model for Web GUI."""

    uid: int
    name: str
    initiator: str
    target: str
    is_load: bool
    is_store: bool
    page_access: str
    timing_isolation: TimingBounds = field(default_factory=TimingBounds)
    timing_interference: TimingBounds = field(default_factory=TimingBounds)


@dataclass
class ClassificationModel:
    """Transaction interference classification."""

    interfering: bool = field(default=False)
    tool: str|None = field(default=None)
    justification: Justification = field(default_factory=Justification)


@dataclass
class AtomModel:
    """Transaction model for Web GUI."""

    uid: int
    mtri: int
    aid: int
    mtr: str
    size: int
    tr: str
    interfering: ClassificationModel = field(default_factory=ClassificationModel)
    timing: TimingBounds = field(default_factory=TimingBounds)
    timing_isolation: TimingBounds = field(default_factory=TimingBounds)


def fetch_transactions(
    page: int,
    page_size: int,
    sort_by: str = "id",
    filters: list[tuple[str, str]] | None = None,
    *,
    descending: bool = False,
    logger: Logger | None = None,
):
    if logger is None:
        logger = logging.getLogger()

    # FIXME There should be a rule, or rules, to select classification when multiple tools exist
    tool, _ = Tool.get_or_create(name="reclassifier")
    tool, _ = Tool.get_or_create(name="pml_importer-0.0.0+dev1")
    #
    logger.debug(
        f"Fetch transactions (page: {page}, size: {page_size}, sort: {sort_by})"
    )

    sort_fields = {
        "id": ViewGuiTransaction.id,
        "mtr": ViewGuiTransaction.mtr,
        "tr": ViewGuiTransaction.tr,
        "size": ViewGuiTransaction.arity,
        "interfering": ViewGuiTransaction.interfering,
        "hwm": ViewGuiTransaction.hwm,
        "lwm": ViewGuiTransaction.lwm,
        "ref_hwm": ViewGuiTransaction.ref_hwm,
        "ref_lwm": ViewGuiTransaction.ref_lwm,
        "penalty_hwm": ViewGuiTransaction.penalty_hwm,
        "penalty_lwm": ViewGuiTransaction.penalty_lwm,
    }
    filter_fields = {
        "id": ViewGuiTransaction.id,
        "mtr": ViewGuiTransaction.mtr,
        "tr": ViewGuiTransaction.tr,
        "size": ViewGuiTransaction.arity,
        "hwm": ViewGuiTransaction.hwm,
        "lwm": ViewGuiTransaction.lwm,
        "interfering": ViewGuiTransaction.interfering,
        "penalty_hwm": ViewGuiTransaction.penalty_hwm,
        "penalty_lwm": ViewGuiTransaction.penalty_lwm,
    }
    if filters is None:
        filters = []
    # Check parameter values
    if page < 1:
        raise ValueError
    if page_size <= 0:
        raise ValueError
    if sort_by not in sort_fields:
        raise ValueError
    if any(f[0] not in filter_fields for f in filters):
        unknown_filters = [f[0] for f in filters if f[0] not in filter_fields]
        msg = f"Unknown filter '{unknown_filters}'"
        raise ValueError(msg)
    logger.debug(f"Fetch, built filters")

    # Build selection query
    atom_query = ViewGuiTransaction.select(
        ViewGuiTransaction.id,
        ViewGuiTransaction.mtri,
        ViewGuiTransaction.tri,
        ViewGuiTransaction.mtr,
        ViewGuiTransaction.tr,
        ViewGuiTransaction.arity,
        ViewGuiTransaction.interfering,
        ViewGuiTransaction.hwm,
        ViewGuiTransaction.lwm,
        ViewGuiTransaction.ref_hwm,
        ViewGuiTransaction.ref_lwm,
        ViewGuiTransaction.tool_id,
    ).paginate(page, page_size)

    # Add filter conditions
    where_clauses = []
    value_per_filter = {}
    for ffield, value in filters:
        if ffield in ["mtr", "tr"]:
            value_per_filter.setdefault(ffield, []).extend(v.strip() for v in value.split("||"))
        else:
            value_per_filter.setdefault(ffield, []).append(value)
    print("Query filters:", value_per_filter)
    for ffield, values in value_per_filter.items():
        # FIXME Behaviour should depend on nullable
        match filter_fields[ffield]:
            case BooleanField():
                w = False
                if any(len(v) > 0 for v in values):
                    w = filter_fields[ffield] << [
                        (v == "1") for v in values if len(v) > 0
                    ]
                if any(len(v) == 0 for v in values):
                    w = w | (filter_fields[ffield].is_null())
                where_clauses.append(w)
            case FixedCharField() | CharField() | TextField():
                def _filter_on(field, value):
                    return (~ field.contains(value[1:])) if value.startswith("~") else field.contains(value)
                where_clauses.append(
                    reduce(
                        lambda a, b: _filter_on(filter_fields[ffield], b) & a,
                        values[1:],
                        _filter_on(filter_fields[ffield], values[0]),
                    ),
                )
            case FloatField():
                where_clauses.append(
                    (values[0] <= filter_fields[ffield])
                    & (filter_fields[ffield] <= values[1])
                )
            case _:
                if "" in values:
                    where_clauses.append(
                        filter_fields[ffield].is_null() | (filter_fields[ffield] << values)
                    )
                else:
                    where_clauses.append(filter_fields[ffield] << values)

    # Add sort clauses
    if sort_by in sort_fields:
        if descending:
            atom_query = atom_query.order_by(-sort_fields[sort_by])
        else:
            atom_query = atom_query.order_by(+sort_fields[sort_by])
    logger.debug(f"Fetch, atom query: {atom_query}")

    # Count corresponding accesses
    count_query = ViewGuiTransaction.select(ViewGuiTransaction.mtri, ViewGuiTransaction.tool_id)
    for w in where_clauses:
        count_query = count_query.where(w)
        atom_query = atom_query.where(w)
    logger.debug(f"Fetch, count query: {count_query}")
    atom_count = count_query.count()
    logger.debug(f"Fetch, {atom_count} atoms")
    print(atom_query)

    atoms = []
    for a in atom_query.tuples():
        m = AtomModel(
            uid=a[0],
            mtri=a[1],
            aid=a[2],
            mtr=a[3],
            size=a[5],
            tr=a[4],
        )
        hwm, lwm, ref_hwm, ref_lwm = a[7], a[8], a[9], a[10]
        m.interfering.justification.tool = a[11]
        m.interfering.interfering = a[6]
        m.timing.hwm.value = hwm
        m.timing.lwm.value = lwm
        m.timing_isolation.hwm.value = ref_hwm
        m.timing_isolation.lwm.value = ref_lwm
        logger.debug(f"Fetch, atom {m.mtr} details")
        # Collect justification for timings
        for t, justifications in [
            (lwm, m.timing.lwm.justifications),
            (hwm, m.timing.hwm.justifications),
            (ref_lwm, m.timing_isolation.lwm.justifications),
            (ref_hwm, m.timing_isolation.hwm.justifications),
        ]:
            justification_query = (
                Observation.select(
                    Location.url,
                    Observation.location_line,
                )
                .join(Location, on=(Location.id == Observation.location))
                .where(Observation.multi_transaction == a[1])
                .where(Observation.transaction == a[2])
                .where(Observation.value == t)
                .limit(3)
                .distinct()
            )
            for f, i in justification_query.tuples():
                justifications.append(Justification(None, Path(f).name, i))
        # TODO Add justification for interfering and collect it here
        # classification_query = (
        #     Classification
        #     .select()
        #     .where(Classification.transaction == m.uid)
        #     .where(Classification.tool == m.classification.tool)
        # )
        atoms.append(m)

    logger.debug(f"Fetch complete")
    return atom_count, atoms


def fetch_accesses(
    page: int,
    page_size: int,
    sort_by: str = "id",
    filters: list[tuple[str, str]] | None = None,
    *,
    descending: bool = False,
) -> tuple[int, list[AccessModel]]:
    """Return a sorted/paginated list of accesses."""
    isolation_obs = Observation.alias("isolt")
    interference_obs = Observation.alias("itert")
    sort_fields = {
        "id": AtomicTransaction.id,
        "name": Transaction.name,
        "initiator": AtomicTransaction.initiator,
        "target": AtomicTransaction.target,
        "is_load": AtomicTransaction.is_load,
        "is_store": AtomicTransaction.is_store,
        "page_access": AtomicTransaction.page_access,
        "timing_isolation": fn.MAX(Observation.value),
        "timing_interference": fn.MIN(Observation.value),
        "delta": fn.ABS(
            (fn.MAX(isolation_obs.value) - fn.MIN(interference_obs.value))
            / fn.MAX(isolation_obs.value)
        ),
    }
    filter_fields = {
        "name": Transaction.name,
        "initiator": AtomicTransaction.initiator,
        "target": AtomicTransaction.target,
        "is_load": AtomicTransaction.is_load,
        "is_store": AtomicTransaction.is_store,
        "page_access": AtomicTransaction.page_access,
    }
    if filters is None:
        filters = []
    # Check parameter values
    if page < 1:
        raise ValueError
    if page_size <= 0:
        raise ValueError
    if sort_by not in sort_fields:
        raise ValueError
    if any(f[0] not in filter_fields for f in filters):
        unknown_filters = [f[0] for f in filters if f[0] not in filter_fields]
        raise ValueError(f"Unknown filter '{unknown_filters}'")
    # Build access selection query
    access_query = (
        AtomicTransaction.select()
        .join(Transaction, on=(Transaction.id == AtomicTransaction.transaction))
        .paginate(page, page_size)
    )

    # Add filter conditions
    where_clauses = []
    value_per_filter = {}
    for ffield, value in filters:
        value_per_filter.setdefault(ffield, []).append(value)
    for ffield, values in value_per_filter.items():
        match filter_fields[ffield]:
            case BooleanField():
                where_clauses.append(
                    filter_fields[ffield] << [(v == "1") for v in values]
                )
            case FixedCharField() | CharField():
                where_clauses.append(
                    reduce(
                        lambda a, b: filter_fields[ffield].contains(a) | b,
                        values[1:],
                        filter_fields[ffield].contains(values[0]),
                    )
                )
            case _:
                where_clauses.append(filter_fields[ffield] << values)

    # Add sort conditions
    if sort_by == "timing_isolation":
        access_query = (
            access_query.join(
                Observation,
                on=(Observation.transaction == Transaction.id),
            )
            .join(
                MultiToTransactions,
                on=(
                    (
                            MultiToTransactions.multi_transaction
                            == Observation.multi_transaction
                    )
                    & (MultiToTransactions.transaction == Observation.transaction)
                ),
            )
            .where(MultiToTransactions.arity == 1)
            .group_by(AtomicTransaction.id)
        )
    if sort_by == "timing_interference":
        access_query = (
            access_query.join(
                Observation,
                on=(Observation.transaction == Transaction.id),
            )
            .join(
                MultiToTransactions,
                on=(
                    (
                            MultiToTransactions.multi_transaction
                            == Observation.multi_transaction
                    )
                    & (MultiToTransactions.transaction == Observation.transaction)
                ),
            )
            .where(MultiToTransactions.arity > 1)
            .group_by(AtomicTransaction.id)
        )
    if sort_by == "delta":
        isolation_mtr = MultiToTransactions.alias("isoltrn")
        interference_mtr = MultiToTransactions.alias("itftrn")
        access_query = (
            access_query.join(
                isolation_obs, on=(isolation_obs.transaction == Transaction.id)
            )
            .join(
                isolation_mtr,
                on=(
                    (isolation_mtr.multi_transaction == isolation_obs.multi_transaction)
                    & (isolation_mtr.transaction == isolation_obs.transaction)
                ),
            )
            .join(interference_obs, on=(interference_obs.transaction == Transaction.id))
            .join(
                interference_mtr,
                on=(
                    (
                        interference_mtr.multi_transaction
                        == interference_obs.multi_transaction
                    )
                    & (interference_mtr.transaction == interference_obs.transaction)
                ),
            )
            .where(isolation_mtr.arity == 1)
            .where(interference_mtr.arity > 1)
            .group_by(AtomicTransaction.id)
        )
    if sort_by in sort_fields:
        if descending:
            access_query = access_query.order_by(-sort_fields[sort_by])
        else:
            access_query = access_query.order_by(+sort_fields[sort_by])

    # Count corresponding accesses
    count_query = AtomicTransaction.select().join(
        Transaction, on=(Transaction.id == AtomicTransaction.transaction)
    )
    for w in where_clauses:
        count_query = count_query.where(w)
        access_query = access_query.where(w)
    access_count = count_query.count()

    # Build observation selection query
    observation_query = (
        Observation.select(
            fn.MIN(Observation.value),
            fn.MAX(Observation.value),
        )
        .join(Transaction, on=(Transaction.id == Observation.transaction))
        .group_by(Transaction.id)
    )
    justification_query = Observation.select(
        Observation.value,
        Location.url,
        Observation.location_line,
    ).join(Location, on=(Location.id == Observation.location))

    accesses: list[AccessModel] = []
    for a in access_query:
        m = AccessModel(
            a.id,
            a.transaction.name,
            a.initiator.name,
            a.target.name,
            a.is_load,
            a.is_store,
            a.page_access,
        )
        accesses.append(m)

        # Collect timing information and justification
        for timing, arity in [
            (m.timing_isolation, MultiToTransactions.arity == 1),
            (m.timing_interference, MultiToTransactions.arity > 1),
        ]:
            bounds = (
                observation_query.where(Observation.transaction == a.transaction.id)
                .where(arity)
                .scalar(as_tuple=True)
            )
            if bounds is None:
                continue

            lwm, hwm = bounds
            justifications = (
                justification_query.where(
                    Observation.transaction == a.transaction.id
                )
                .where(Observation.value << (lwm, hwm))
                .tuples()
            )

            timing.lwm.value = lwm
            timing.hwm.value = hwm

            for t, f, i in justifications:
                if t == lwm:
                    timing.lwm.justifications.append(
                        Justification("", Path(f).name, int(i))
                    )
                if t == hwm:
                    timing.hwm.justifications.append(
                        Justification("", Path(f).name, int(i))
                    )

    return access_count, accesses
