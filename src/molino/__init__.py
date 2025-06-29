from dataclasses import asdict
from pathlib import Path

from molino.ajax import fetch_accesses, fetch_transactions
from molino.transactions import Resource, prepare_transactions_db