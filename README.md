# NetMHCpan Automation Engine

An automated, fault-tolerant wrapper for `netMHCpan` designed for high-throughput peptide binding prediction. This engine manages parallel execution, dynamic file handling, and robust state management, making it easy to process thousands of samples efficiently.

## Features

* **Parallel Processing:** Automatically detects system RAM and CPU cores to scale the number of worker processes, maximizing speed without crashing your machine.
* **Dynamic Hot-Swapping:** Add new file pairs to the input folder *while* the script is running. The engine detects and processes them automatically.
* **Smart Checkpointing:** Maintains a state file (`processed_jobs.json`). If the process is interrupted, it resumes exactly where it left off.
* **Clean Output:** Automatically captures `netMHCpan` text output and converts it into clean, tab-separated `.xls` files ready for analysis.

## Project Structure

Your directory should look like this:

```text
.
├── netmhc_engine.py       # Core logic (BatchManager, Parallel Workers)
├── Run_Analysis.ipynb     # Jupyter Notebook (The Dashboard/Interface)
├── processed_jobs.json    # Auto-generated memory file (tracks finished jobs)
├── batch_run.log          # Auto-generated log file
├── source/                # [INPUT] Drop your CSV pairs here
│   ├── CRC023_Unique_Peptides.csv
│   └── CRC023_Alleles.csv
└── results/               # [OUTPUT] Finished .xls files appear here
```

## Prerequisites

* Python 3.x
* [NetMHCpan 4.1 or 4.2](https://services.healthtech.dtu.dk/service.php?NetMHCpan-4.1) installed and executable.
* Jupyter Notebook

## Usage

1. **Setup:**
* Clone this repository.
* Ensure you have a `source` folder created in the root directory.
* Open `Run_Analysis.ipynb` and update the `BINARY_PATH` variable to point to your local `netMHCpan` executable.


2. **Add Data:**
* Place your sample files in the `source` folder.
* **Naming Convention:** Each sample needs a pair of files:
* `{SampleID}_Unique_Peptides.csv`
* `{SampleID}_Alleles.csv`




3. **Run:**
* Open `Run_Analysis.ipynb` and run all cells.
* The system will auto-scale workers and begin processing.


4. **Monitor & Manage:**
* **Logs:** Check `batch_run.log` for detailed progress.
* **Restarting:** To re-run specific files, delete `processed_jobs.json` (or remove specific IDs from it) and restart the notebook.



## Customization

* **Memory Usage:** If you are running on a machine with very limited RAM, you can adjust the `est_mem_per_job_gb` parameter in the `Run_Analysis.ipynb` notebook (default is 1.5GB per worker).

