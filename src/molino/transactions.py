from peewee import (
    SQL,
    BooleanField,
    FixedCharField,
    FloatField,
    ForeignKeyField,
    IntegerField,
    Model,
    SqliteDatabase,
    TextField,
)

transactions_db = SqliteDatabase(None)


def prepare_transactions_db(url: str) -> None:
    """Initialise the transactions database connection."""
    transactions_db.init(
        url,
        pragmas={
            "journal_mode": "wal",
            "cache_size": -1 * 8000,  # 8MB
            "foreign_keys": 0,  # Do not check foreign key constraints (recommended 1)
            "ignore_check_constraints": 0,
            "synchronous": 0,  # Let the OS manage synchronisation
        },
    )
    transactions_db.create_tables(
        [
            Transaction,
            AtomicTransaction,
            Resource,
            Observation,
            MultiToTransactions,
            MultiTransaction,
            Tool,
            Classification,
            Location,
        ],
    )

    # Create views if required
    existing_views = {v.name for v in transactions_db.get_views()}
    if ViewGuiTransaction.view_name not in existing_views:
        transactions_db.execute_sql(ViewGuiTransaction.creation_query)
    # Create triggers for watermarks
    transactions_db.execute_sql(
        """
        CREATE TRIGGER
        IF NOT EXISTS
        update_mtr_watermarks
        AFTER
        INSERT ON observation
        FOR EACH ROW
        BEGIN
            UPDATE multitotransactions
            SET
                lwm = min(
                        ifnull(NEW.value, lwm),
                        ifnull(lwm, NEW.value)
                ),
                hwm = max(
                        ifnull(NEW.value, hwm),
                        ifnull(hwm, NEW.value)
                      )
            WHERE
                multi_transaction_id == NEW.multi_transaction_id
                and transaction_id == NEW.transaction_id
                and (
                    lwm > NEW.value
                    or lwm is null
                    or hwm < NEW.value
                    or hwm is null
                )
            ;
        END
        """
    )
    transactions_db.execute_sql(
        """
        CREATE TRIGGER
        IF NOT EXISTS
        update_tr_watermarks
        AFTER
        INSERT ON observation
        FOR EACH ROW
        BEGIN
            UPDATE "transaction"
            SET
                lwm = min(
                        ifnull(NEW.value, "transaction".lwm),
                        ifnull("transaction".lwm, NEW.value)
                ),
                hwm = max(
                        ifnull(NEW.value, "transaction".hwm),
                        ifnull("transaction".hwm, NEW.value)
                      )
            FROM
                multitotransactions AS m
            WHERE
                -- Match and filter observation context
                m.multi_transaction_id == NEW.multi_transaction_id
                AND m.transaction_id == NEW.transaction_id
                AND m.arity == 1
                -- Match updated transaction
                AND "transaction".id == NEW.transaction_id
                -- Check watermark update required
                AND (
                    "transaction".lwm > NEW.value
                    or "transaction".lwm is null
                    or "transaction".hwm < NEW.value
                    or "transaction".hwm is null
                )
            ;
        END
        """
    )


class TransactionModel(Model):
    """Base Model for Transaction model."""

    class Meta:
        """Transaction model configuration."""

        database = transactions_db


class Resource(TransactionModel):
    """PML Hardware Resource."""

    RESOURCE_ROLES = (
        ("I", "Initiator"),
        ("T", "Transporter"),
        ("E", "Target"),
    )

    name = TextField(unique=True, primary_key=True)
    role = FixedCharField(max_length=1, choices=RESOURCE_ROLES)

    @classmethod
    def roles(cls) -> dict[str, str]:
        """Get supported resource roles dictionary."""
        return {k: v for v, k in cls.RESOURCE_ROLES}


class Transaction(TransactionModel):
    """PML Atomic Transaction."""

    name = TextField(unique=True)
    lwm = FloatField(null=True)
    hwm = FloatField(null=True)


class AtomicTransaction(TransactionModel):
    """PML Atomic access from Initiator to Target."""

    PAGE_ACCESS_TYPES = (
        ("Ph", "Page Hit"),
        ("Pm", "Page Miss"),
    )

    transaction = ForeignKeyField(Transaction, backref="atoms")
    initiator = ForeignKeyField(Resource, backref="initiates")
    target = ForeignKeyField(Resource, backref="terminates")
    is_load = BooleanField(null=False)
    is_store = BooleanField(null=False)
    page_access = FixedCharField(max_length=2, choices=PAGE_ACCESS_TYPES, null=True)


class MultiTransaction(TransactionModel):
    """Cache Multi-transaction information."""

    name = TextField(index=True, unique=True)


class MultiToTransactions(Model):
    """Through table from multi-transactions to composing transactions."""

    class Meta:
        """Metaclass for configuring MultiToTransactions."""

        database = transactions_db
        indexes = (
            (("transaction_id", "arity", "hwm", "lwm"), False),
            # (("multi_transaction_id", "transaction_id", "arity"), False),
        )
        constraints =(
            SQL("UNIQUE (multi_transaction_id, transaction_id)"),
        )

    multi_transaction = ForeignKeyField(MultiTransaction)
    transaction = ForeignKeyField(Transaction)
    tr_name = TextField()
    hwm = FloatField(null=True)
    lwm = FloatField(null=True)
    arity = IntegerField()


class Location(TransactionModel):
    """Location for traceability."""

    url = TextField(unique=True)


class Observation(TransactionModel):
    """Observation for an Atomic Transaction."""

    multi_transaction = ForeignKeyField(
        MultiToTransactions,
        MultiToTransactions.multi_transaction,
        on_delete="CASCADE",
    )
    transaction = ForeignKeyField(
        Transaction,
        backref="observations",
        on_delete="CASCADE",
    )
    value = FloatField()
    location = ForeignKeyField(Location)
    location_line = IntegerField()


class Tool(TransactionModel):
    """Tool used to collection analysis-relevant information."""

    name = TextField()


class Classification(Model):
    """Interference classification as interfering/free."""

    class Meta:
        """Meta class for configuring Classification."""

        database = transactions_db
        indexes = (
            # (("transaction_id", "tool_id", "interfering"), False),
            (("transaction_id", "tool_id"), True),
        )

    transaction = ForeignKeyField(
        MultiToTransactions,
        MultiToTransactions.multi_transaction,
    )
    tool = ForeignKeyField(Tool)
    interfering = BooleanField()


class ViewGuiTransaction(TransactionModel):
    """Transaction view for GUI queries."""

    id = IntegerField(primary_key=True)
    mtri = IntegerField()
    tri = IntegerField()
    mtr = TextField()
    tr = TextField()
    arity = IntegerField()
    hwm = FloatField(null=True)
    lwm = FloatField(null=True)
    ref_hwm = FloatField(null=True)
    ref_lwm = FloatField(null=True)
    penalty_hwm = FloatField(null=True)
    penalty_lwm = FloatField(null=True)
    tool_id = IntegerField(null=True)
    interfering = IntegerField(null=True)

    @classmethod
    @property
    def view_name(cls):
        return "viewguitransaction"

    @classmethod
    @property
    def creation_query(cls) -> str:
        """Prepare SQL query to create view."""
        return f"""
        CREATE VIEW "{cls.view_name}" AS 
        select 
            mtr.id as id, 
            mtr.multi_transaction_id as mtri, 
            mtr.transaction_id as tri, 
            mt.name as mtr, 
            mtr.tr_name as tr, 
            mtr.hwm as hwm, 
            mtr.lwm as lwm, 
            mtr.arity as arity,
            tr.hwm as ref_hwm, 
            tr.lwm as ref_lwm,
            ((tr.hwm - mtr.lwm) / tr.hwm) as penalty_hwm, 
            ((tr.lwm - mtr.hwm) / tr.lwm) as penalty_lwm, 
            cl.tool_id as tool_id,
            cl.interfering as interfering
        from multitotransactions as mtr
        left join multitransaction as mt on mt.id == mtr.multi_transaction_id
        left join classification as cl on cl.transaction_id == mtr.multi_transaction_id
        left join "transaction" as tr on tr.id == mtr.transaction_id
        """  # noqa: S608
