import os
import subprocess
import time
import logging
import re
import json
import multiprocessing
from abc import ABC, abstractmethod
from typing import List, Set
# We use 'wait' and 'FIRST_COMPLETED' for smart polling
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED


# --- 1. Logger Setup ---
def setup_logging(log_file="process.log"):
    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.handlers = []

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
    )
    return logging.getLogger(__name__)


logger = logging.getLogger(__name__)


# --- 2. Interfaces ---
class IPredictionRunner(ABC):
    @abstractmethod
    def execute(self) -> str:
        pass


class IBatchManager(ABC):
    @abstractmethod
    def run_batch(self):
        pass


class IStateManager(ABC):
    @abstractmethod
    def is_completed(self, sample_id: str) -> bool:
        pass

    @abstractmethod
    def mark_completed(self, sample_id: str):
        pass


# --- 3. State Manager Implementation ---
class JSONStateManager(IStateManager):
    def __init__(self, state_file="processed_jobs.json"):
        self.state_file = state_file
        self.completed_jobs = self._load_state()

    def _load_state(self) -> Set[str]:
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    return set(data)
            except (json.JSONDecodeError, ValueError):
                return set()
        return set()

    def is_completed(self, sample_id: str) -> bool:
        return sample_id in self.completed_jobs

    def mark_completed(self, sample_id: str):
        self.completed_jobs.add(sample_id)
        try:
            with open(self.state_file, 'w') as f:
                json.dump(list(self.completed_jobs), f)
        except Exception as e:
            logger.error(f"❌ Failed to save state for {sample_id}: {e}")


# --- 4. Individual Runner (CLEANED) ---
class NetMHCPanRunner(IPredictionRunner):
    """
    Runs NetMHCpan and converts output to Excel-friendly tab-separated format.
    """

    def __init__(self, sample_id: str, input_dir: str = "source", output_dir: str = "results", binary_path: str = None):
        self.sample_id = sample_id
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.binary_path = binary_path if binary_path else "netMHCpan"
        self.peptide_file = os.path.join(self.input_dir, f"{self.sample_id}_Unique_Peptides.csv")
        self.allele_file = os.path.join(self.input_dir, f"{self.sample_id}_Alleles.csv")
        os.makedirs(self.output_dir, exist_ok=True)

    def is_valid(self) -> bool:
        if not os.path.exists(self.peptide_file):
            # No logging here to avoid spamming during dynamic scans
            return False
        if not os.path.exists(self.allele_file):
            return False
        return True

    def _read_alleles(self) -> str:
        with open(self.allele_file, 'r') as f:
            content = f.read().strip()
            return content.replace('\n', '').replace('"', '').replace("'", "")

    def execute(self) -> str:
        if not self.is_valid():
            return None

        alleles = self._read_alleles()
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_filename = f"{self.sample_id}_result_{timestamp}.xls"
        output_path = os.path.join(self.output_dir, output_filename)

        # Command with -xls flag
        cmd = [self.binary_path, "-p", self.peptide_file, "-a", alleles, "-BA", "-s", "-xls"]

        logger.info(f"🚀 STARTING analysis for ID: {self.sample_id}")
        start_time = time.time()

        try:
            with open(output_path, "w") as outfile:
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)

                # Loop to read stdout line by line and convert spaces to tabs
                while True:
                    line = process.stdout.readline()
                    if not line and process.poll() is not None:
                        break

                    if line:
                        # # Pythonic 'sed': Replace 1+ spaces with a tab
                        # clean_line = re.sub(r' +', '\t', line)
                        # outfile.write(clean_line)
                        # --- THE CHANGE ---
                        # r' {2,}' matches 2 or more spaces.
                        # Single spaces (like in "HLA A02") will be preserved.
                        clean_line = re.sub(r' {5,}', '\t', line)
                        outfile.write(clean_line)

                # Check for errors after completion
                if process.returncode != 0:
                    raise RuntimeError(process.stderr.read())

            elapsed = time.time() - start_time
            logger.info(f"✅ FINISHED {self.sample_id} in {elapsed:.2f}s. Saved: {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"❌ ERROR for {self.sample_id}: {e}")
            raise e


# --- 5. Batch Manager (DYNAMIC VERSION) ---
class BatchManager(IBatchManager):
    """
    Dynamic Batch Manager.
    Refreshes the job list after every single run to allow adding files on the fly.
    """

    def __init__(self, state_manager: IStateManager, input_dir="source", output_dir="results", binary_path=None, est_mem_per_job_gb=0.2):
        self.state_manager = state_manager
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.binary_path = binary_path
        self.est_mem_per_job_gb = est_mem_per_job_gb
        self.max_workers = self._autoscale_workers()

    def _get_total_ram_gb(self):
        """Returns total system RAM in GB. Works on Linux/Mac."""
        try:
            # Standard Unix way to get memory stats
            mem_bytes = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
            return mem_bytes / (1024 ** 3)
        except:
            # Fallback for weird environments - assume 8GB to be safe
            logger.warning("⚠️ Could not detect RAM. Assuming 8GB safe default.")
            return 8.0

    def _autoscale_workers(self):
        """
        The Safety Logic:
        Calculates how many workers we can run without crashing.
        """

        # 1. Get Resources
        total_cpus = multiprocessing.cpu_count()
        total_ram_gb = self._get_total_ram_gb()

        # 2. Calculate RAM Bottleneck
        # We reserve 25% of RAM for the OS and other apps (Safety Factor)
        safe_ram_gb = total_ram_gb * 0.75
        max_workers_by_ram = int(safe_ram_gb / self.est_mem_per_job_gb)

        # 3. Calculate CPU Bottleneck
        # We usually leave 1 core free for the BatchManager/OS if we have many cores
        max_workers_by_cpu = total_cpus - 1 if total_cpus > 2 else 1

        # 4. The Final Decision (The Bottleneck)
        final_workers = max(1, min(max_workers_by_ram, max_workers_by_cpu))
        # print(f"--- AUTO-SCALE: {total_ram_gb:.1f}GB RAM / {total_cpus} CPUs -> Running {final_workers} Workers ---")

        print(f"--- 🖥️ SYSTEM RESOURCES DETECTED ---")
        print(f"   • Total RAM: {total_ram_gb:.1f} GB")
        print(f"   • Total CPUs: {total_cpus}")
        print(f"   • Safe RAM Limit: {safe_ram_gb:.1f} GB (running {self.est_mem_per_job_gb} GB/job)")
        print(f"   • SCALING DECISION: Setting workers to {final_workers}")
        print(f"----------------------------------------")

        return final_workers

    def scan_files(self) -> List[str]:
        """
        Scans the folder and returns ALL valid IDs found on disk.
        """
        valid_ids = []
        if not os.path.exists(self.input_dir):
            logger.error(f"❌ Source folder not found: {self.input_dir}")
            return []

        files = os.listdir(self.input_dir)
        pattern = re.compile(r"^(.*?)_Unique_Peptides\.csv$")

        for f in files:
            match = pattern.match(f)
            if match:
                sample_id = match.group(1)
                expected_allele_file = os.path.join(self.input_dir, f"{sample_id}_Alleles.csv")

                if os.path.exists(expected_allele_file):
                    valid_ids.append(sample_id)
                    # Note: We do NOT log warnings about orphans here anymore,
                    # because this function runs repeatedly. We don't want to spam the logs.

        return sorted(valid_ids)

    def run_batch(self):
        print("--- Starting Dynamic Batch Processing ---")
        print("Tip: You can add new files to the 'source' folder while this runs.")

        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_id = {}

            while True:
                # 1. SCAN & SUBMIT
                # We check for new work at the start of every loop
                all_ids = self.scan_files()

                for sample_id in all_ids:
                    # Logic: If NOT done AND NOT currently running -> Submit it
                    if (not self.state_manager.is_completed(sample_id) and
                            sample_id not in future_to_id.values()):
                        print(f"Submitting: {sample_id}")
                        future = executor.submit(run_worker_task,sample_id, self.input_dir, self.output_dir, self.binary_path)
                        future_to_id[future] = sample_id

                # 2. SMART WAIT (The replacement for sleep)
                # If we have running jobs, we wait for *at least one* to finish.
                # timeout=2.0 means: "Wait up to 2s. If one finishes sooner, wake up immediately."
                if future_to_id:
                    done_futures, _ = wait(future_to_id.keys(), timeout=2.0, return_when=FIRST_COMPLETED)

                    if not done_futures:
                        # Timeout hit (2s passed, no one finished).
                        # Loop back to top to scan for new files.
                        continue

                    # Process the ones that finished
                    for future in done_futures:
                        sample_id = future_to_id.pop(future)  # Remove from active list
                        try:
                            future.result()  # Check for errors
                            self.state_manager.mark_completed(sample_id)
                        except Exception as e:
                            logger.error(f"❌ WORKER FAILED: {sample_id} - {e}")
                            self.state_manager.mark_completed(sample_id)


                else:
                    # 3. IDLE STATE
                    # If NO jobs are running, we just sleep and scan.
                    # This happens when the folder is empty or everything is done.
                    time.sleep(2)


# --- 6. Standalone Worker Function ---
def run_worker_task(sample_id, input_dir, output_dir, binary_path):
    runner = NetMHCPanRunner(
        sample_id=sample_id,
        input_dir=input_dir,
        output_dir=output_dir,
        binary_path=binary_path)
    return runner.execute()

#
#
# # Your specific path to the NetMHCpan tool
# BINARY_PATH = "/home/dimitris/netMHCpan42/netMHCpan"
#
# # Define where your data lives and where results should go
# INPUT_FOLDER = "source"
# OUTPUT_FOLDER = "results"
# LOG_FILE = "batch_run.log"
# STATE_FILE = "processed_jobs.json" # <--- New configuration
#
# try:
#     # 1. Setup Logging (prevents duplicate logs in Jupyter)
#     setup_logging(LOG_FILE)
#
#     print("Initializing Batch Manager...")
#
#     # 1. Create the State Manager (The Memory)
#     state_manager = JSONStateManager(state_file=STATE_FILE)
#
#     # 2. Initialize the Manager
#     # We use the BatchManager class we just updated (which follows the IBatchManager interface)
#
#     # If you know your job is heavy, you can tune 'est_mem_per_job_gb' inside the class,
#     # but the default (0.2) is usually fine.
#     manager = BatchManager(
#         state_manager=state_manager,  # <--- Injecting the dependency
#         input_dir=INPUT_FOLDER,
#         output_dir=OUTPUT_FOLDER,
#         binary_path=BINARY_PATH
#     )
#
#     # 3. Run the Batch
#     # This will scan 'source', find all valid pairs, and process them one by one.
#     manager.run_batch()
#
# except Exception as e:
#     print(f"\n❌ CRITICAL ERROR: The batch run stopped unexpectedly.\nDetails: {e}")