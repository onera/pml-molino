# PML Molino

A prototype Graphical User Interface to explore PML analysis results and trace data. 

## Setup

- From the repository, install all required Python dependencies using [poetry](https://python-poetry.org/):
  ```shell
  poetry install --with dev
  ```

- From a package, install using Python pip
  ```shell
  pip install molino-0.4-py3-none-any.whl
  ```

## Importing data

The GUI requires a compliant database in the root of the project named `transactions.sqlite`. This implies importing
observation traces, collected on target, and corresponding PML interference analysis results.
This is service is provided by the `importer.ipynb` view.
Alternatively, example importers are included for both under the `molino/importers` package, 
with their usage under `molino/tests/test_db.py`.

### Observations traces

The default observation trace importer (`molino/importers/alf.py`) supports the alf` trace format.
The `alf` trace format captures for each multi-transaction and initiator the number of cycles 
and the number of load/store instructions as a transaction count. The reference measurement for 
each transaction, in isolation, is provided in the file.

- [ ] TODO: Document the trace format


### PML Interference analysis

The default PML analysis importer (`molino/importers/pml.py`) supports the `txt` output format.
The `txt` format captures a list of transactions with their classification and arity captured in the filename.
A file matching `*_free_<arity:int>.txt` (respectively `*_itf_<arity:int>.txt`) captures multi-transactions free from 
interference (respectively suffering from interference).

Entries are formatted as `||`-separated (multi-)transaction names, one per line, each surrounded by `<>`. As an example:
```
< C0_RdCo_BK1 || C1_Rd_MPIC >
```

## Usage

You can start a server using the `molino_dashboard` command which will serve the different views.

- [ ] TODO Document importer view
- [ ] TODO Document transactions view
