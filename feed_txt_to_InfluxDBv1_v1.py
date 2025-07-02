"""
File:        feed_txt_to_InfluxDBv1.py
Created:     2025 by Alexandr Strekalovski
Description: 
  This script parses "*.txt log files" for the {MEASUREMENT},
  line by line,
  starting with "starting_file.txt" through the last file in the same directory (ordered by mtime)
  and sends data to InfluxDB v1.11.8

Usage:
  python3 feed_txt_to_InfluxDBv1.py /path/to/starting_file.txt

Attention:
  Redefine the "process_line" function!
    The line parser is described in "def process_line".
    It was written for my custom measurement format, so just redefine the "process_line" function for your format.
"""

import argparse
import sys
import calendar, time
from datetime import datetime, timezone
import subprocess
from pathlib import Path
from typing import List, Optional

# ---------------------------------------------------------------------------#
# Configuration constants
# ---------------------------------------------------------------------------#

HOST = 'hostname'
PORT = '8086'
INFLUXDB_DB_NAME = 'influxdb_name'
MEASUREMENT = 'mydatameasurement'
LOG_FILE = f'feed_{MEASUREMENT}_to_InfluxDBv1.log'
CHECK_INTERVAL = 60  # seconds
INFLUXDB_URL = f'http://{HOST}:{PORT}/write?db={INFLUXDB_DB_NAME}'

"""
In the case with user authorization:
INFLUXDB_USERNAME = 'your_user'
INFLUXDB_PASSWORD = 'your_password'
INFLUXDB_URL = f"http://{HOST}:{PORT}/write?db={INFLUXDB_DB_NAME}&u={INFLUXDB_USERNAME}&p={INFLUXDB_PASSWORD}"
"""

# ---------------------------------------------------------------------------#
# Functions / classes
# ---------------------------------------------------------------------------#

def log(message: str) -> None:
    """Append a timestamped message to the local log file."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S GMT")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{timestamp} {message}\n")


def list_txt_files(directory: Path) -> List[Path]:
    """Return *.txt files in dir_path sorted by mtime (oldest first)."""
    return sorted(directory.glob("*.txt"), key=lambda p: p.stat().st_mtime)


def find_next_file(current: Path, files: List[Path]) -> Optional[Path]:
    """Return the file after 'current' file, else None."""
    try:
        idx = files.index(current)
    except ValueError:
        return files[-1] if files else None
    return files[idx + 1] if idx + 1 < len(files) else None


def iso_z_to_ns(iso_z: str) -> int:
    """Convert an ISO-8601 timestamp as 2025-06-09T21:58:12Z to epoch-nanoseconds, UTC."""
    return calendar.timegm(time.strptime(iso_z, "%Y-%m-%dT%H:%M:%SZ")) * 1_000_000_000


class Sender:
    """Wrapper around a 'curl' to post a line-protocol string."""
    def __init__(self, url):
        self.url = url

    def send(self, line_protocol: str, timeout: int = 10) -> None:
        """It's a 'curl' with '--fail', '--silent', '--show-error' for the line_protocol."""
        cmd = ['curl', '-f', '-sS', '-XPOST', self.url, '--data-binary', line_protocol]
        try:
            subprocess.run(
                cmd, 
                check=True, 
                capture_output=True, 
                text=True, 
                timeout=timeout
                )
        except Exception as e:
            if isinstance(e, subprocess.CalledProcessError):
                msg = (e.stderr or e.stdout or "").strip()
                log(f"[ERROR] curl exited with code {e.returncode}: {msg}")
                print(f"[ERROR] curl exited with code {e.returncode}: {msg}")
            elif isinstance(e, subprocess.TimeoutExpired):
                log(f"[ERROR] curl timeout after {timeout}s: {e}")
                print(f"[ERROR] curl timeout after {timeout}s: {e}")
            elif isinstance(e, FileNotFoundError):
                log("[ERROR] curl not found on PATH")
                print("[ERROR] curl not found on PATH")
            else:
                log(f"[ERROR] Unexpected error during curl: {e}")
                print(f"[ERROR] Unexpected error during curl: {e}")
            raise
            

def process_line(line: str, sender: Sender, line_number: int) -> None:
    """
    Parse one {MEASUREMENT} line and forward it as line_protocol to InfluxDB via sender.

    The MYDATA schema:
        mydatameasurement,<host>,
            <T1>,<T2>,<T3>,<T4>,
            <Pwr1>,<Pwr2>,<Pwr3>,<Pwr4>,
            <LEDamp>,<LEDwidth>,<Threshold>,
            <V1>,<V2>,<V3>,<V4>,
        <ISO-8601-timestamp-Z>
    """
    try:
        parts = line.split(',')
        expected_len = 2 + 15 + 1 # (mydatameasurement + <host>) + 15 + <timestamp>
        if len(parts) != expected_len:
            log(f"[SKIP] Unexpected 'expected_len' in line {line_number} : {line}")
            return

        host = parts[1]
        measurement_time_str = parts[-1]
        measurement_time = iso_z_to_ns(measurement_time_str)

        t1  = parts[2]
        t2 = parts[3]
        t3  = parts[4]
        t4 = parts[5]

        pwr1  = parts[6]
        pwr2 = parts[7]
        pwr3  = parts[8]
        pwr4 = parts[9]

        LEDamp  = parts[10]
        LEDwidth = parts[11]
        Threshold  = parts[12]

        v1  = parts[13]
        v2 = parts[14]
        v3  = parts[15]
        v4 = parts[16]

        tags = f"host={host}"
        fields = (f"T1={t1},T2={t2},T3={t3},T4={t4},Pwr1={pwr1},Pwr2={pwr2},Pwr3={pwr3},Pwr4={pwr4},LEDamp={LEDamp},LEDwidth={LEDwidth},Threshold={Threshold},V1={v1},V2={v2},V3={v3},V4={v4}")
        line_protocol = f"{MEASUREMENT},{tags} {fields} {measurement_time}"
        print(f"[PARSED] Line {line_number} : host={host}, {fields}, time={measurement_time}")
        
        sender.send(line_protocol)

    except Exception as e:
        log(f"[ERROR] Parse failed on Line {line_number} : {line} | Reason: {e}")
        # stop script if error
        raise
    
    else:
        log(f"[PARSED] Line {line_number} : {line}")

# ---------------------------------------------------------------------------#
# The main logic
# ---------------------------------------------------------------------------#

def process_files(start_file: Path, sender: Sender, wait_time: int = 60) -> None:

    directory = start_file.parent
    current = start_file

    # external infinite loop
    while True:
        try:
            # open file
            with current.open("r", encoding="utf-8", errors="replace") as f:
                log(f"[OPEN] Processing file: {current}")
                line_number = 0
                # internal loop
                while True:
                    # read line up to the EOF
                    line = f.readline()
                    if line:
                        line_number += 1
                        line = line.strip()
                        if line.startswith(MEASUREMENT):
                            process_line(line, sender, line_number)
                            continue

                    # EOF
                    time.sleep(wait_time)

                    # Try once more in a 'wait_time'
                    line = f.readline()
                    if line:
                        line_number += 1
                        line = line.strip()
                        if line.startswith(MEASUREMENT):
                            process_line(line, sender, line_number)
                            continue

                    # Still no data -> check the file's directory for a new file.
                    newer = find_next_file(current, list_txt_files(directory))
                    if newer:
                        current = newer
                        # Break inner loop to open the newer file. Otherwise, readline() again
                        break

        except KeyboardInterrupt:
            raise
        except Exception: # propagate curl
            raise

# ---------------------------------------------------------------------------#
# main
# ---------------------------------------------------------------------------#
            
def main() -> None:
    parser = argparse.ArgumentParser(
        description=f"Parse the *.txt log files for the {MEASUREMENT} and send it line by line to InfluxDB v1.11.8"
    )
    parser.add_argument(
        "start_file",
        type=Path,
        help="Path to the *.txt file to start with.",
    )
    parser.add_argument(
        "--wait",
        type=int,
        default=CHECK_INTERVAL,
        metavar="SECONDS",
        help="Seconds to wait at EOF before polling again (default: 60).",
    )
    args = parser.parse_args()

    start_file: Path = args.start_file.expanduser().resolve()
    if not start_file.is_file():
        sys.exit(f"Start file '{start_file}' does not exist or is not a regular file.")

    sender = Sender(INFLUXDB_URL)


    try:
        process_files(start_file, sender, wait_time=args.wait)
    
    except KeyboardInterrupt:
        log("[INFO] Interrupted by user.")
        print("\nInterrupted by user.")
        sys.exit(0)
    except Exception as e:
        log(f"[FATAL] {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
