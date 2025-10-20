import re
from pathlib import Path

from tqdm import tqdm

from molino.importers.common import _batched, create_or_fetch_multitransactions
from molino.transactions import Classification, MultiTransaction, Tool, transactions_db


def load_pml_classifications(
    *traces: str | Path,
    create_missing: bool = False,
    batch_size: int = 256,
) -> None:
    """Load transaction classification from PML analysis files."""
    # FIXME Add chunk size to PML trace reading. _batched() should do the trick
    tool, _ = Tool.get_or_create(name="pml_importer-0.0.0+dev1")  # Use @<commit-hash>
    for trace in map(Path, traces):
        with transactions_db.atomic():
            name = trace.name.lower()
            # Identify classification from filename
            classification = None
            if (match := re.match(".*_free_([0-9]+).txt$", name)) is not None:
                classification = False
            elif (match := re.match(".*_itf_([0-9]+).txt$", name)) is not None:
                classification = True
            else:
                raise ValueError(f"Unknown classification for trace {name}")
            print(f"Load classifications from '{name}' as 'itf: {classification}'")
            # Record classification for all transactions in file
            with trace.open() as trace_file:
                # Fetch/Create all multi-transaction by name
                names = set()
                for entry in tqdm(trace_file, desc=f"Reading PML trace {name}"):
                    if len(entry.strip()) == 0:
                        continue
                    transaction_name = re.split("[><]", entry)[1].strip()
                    # Fix PML transactions have spaces around the ||, not some imports
                    transaction_name = "||".join(
                        sorted(a.strip() for a in transaction_name.split("||")),
                    )
                    names.add(transaction_name)
                create_or_fetch_multitransactions(names, create_missing=create_missing)
                mtr_ids: set[int] = {
                    i
                    for i, n in MultiTransaction.select(
                        MultiTransaction.id,
                        MultiTransaction.name,
                    ).tuples()
                    if n in names
                }
                # Update known transaction classifications
                with tqdm(desc="Update classification", total=len(mtr_ids)) as progress:
                    for q_ids in _batched(mtr_ids, batch_size):
                        Classification.insert_many(
                            ((i, classification, tool.id) for i in q_ids),
                            fields=[
                                Classification.transaction,
                                Classification.interfering,
                                Classification.tool,
                            ],
                        ).on_conflict(
                            conflict_target=[
                                Classification.transaction,
                                Classification.tool,
                            ],
                            preserve=[Classification.interfering],
                        ).execute()
                        progress.update(len(q_ids))
                progress.close()
