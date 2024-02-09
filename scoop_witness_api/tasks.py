import os
import time
import datetime
import subprocess
import random
import traceback
import json

import requests

from celery import shared_task

from flask import current_app, jsonify
from pathlib import Path

from scoop_witness_api.utils import capture_to_dict


@shared_task
def start_capture_process(proxy_port=9000) -> None:
    """
    Takes a capture from the queue and processes it.

    If interrupted during capture: puts capture back into queue.
    """
    from scoop_witness_api.models import Capture

    if not proxy_port or proxy_port > 65535:
        print("--proxy-port must be a valid, free TCP port.")
        return

    try:
        captures = None
        """ List of pending captures. """

        capture = None
        """ Capture currently being processed. """

        proxy_port_is_available = True
        """ Determines if the targeted proxy port is currently available.
            Capture cycle will be skipped if not. """

        storage_path = None
        """ Path to temporary folder used by the API to store artifacts.
            Will be created on the fly if necessary. """

        attachments_path = None
        """ Path to the temporary folder used by the API to store attachments.
            Will be created on the fly if necessary. """

        archive_path = None
        """ Path to the resulting WARC/WACZ file. Should be a filename under storage_path. """

        archive_format = "wacz"
        """ File extension of the archive. "wacz" unless by DOWNGRADE_TO_WARC flag is on. """

        json_summary_path = None
        """ Path to the JSON summary file. Should be a filename under storage_path. """

        scoop_options = current_app.config["SCOOP_CLI_OPTIONS"]
        """ Shortcut to app-level Scoop CLI options. """

        scoop_stdout = None
        """ STDOUT from Scoop run. """

        scoop_stderr = None
        """ STDERR from Scoop run. """

        scoop_exit_code = None
        """ Exit code from Scoop run. """

        #
        # Check for presence of deployment sentinel
        #
        sentinel = Path(current_app.config["DEPLOYMENT_SENTINEL_PATH"])
        if sentinel.exists():
            print("Deployment sentinel present, exiting.")
            return

        #
        # Pull 1 pending capture from the queue
        #
        captures = (
            Capture.select()
            .where(Capture.status == "pending")
            .order_by("created_timestamp")
            .paginate(1, 1)
        )

        #
        # If there is a capture to process:
        # Check that --proxy-port is available
        #
        if len(captures):
            try:
                requests.head(f"http://localhost:{proxy_port}", timeout=1)
                proxy_port_is_available = False
            except requests.exceptions.ReadTimeout:
                proxy_port_is_available = False
            except Exception:
                proxy_port_is_available = True

            if not proxy_port_is_available:
                print(f"{log_prefix()} Port {proxy_port} already in use - retrying")
                start_capture_process.delay(proxy_port=proxy_port + 1)
                return

        #
        # Return if there are no pending captures -- or run next capture?
        #
        if not len(captures):
            return

        capture = captures[0]

        #
        # Define paths
        #

        # Temporary storage folder
        temporary_storage_path = current_app.config["TEMPORARY_STORAGE_PATH"]
        storage_path = f"{temporary_storage_path}{os.sep}{capture.id_capture}"
        json_summary_path = f"{storage_path}{os.sep}archive.json"
        attachments_path = f"{storage_path}{os.sep}attachments"

        # Archive path requires archive format (default is WACZ unless specified otherwise)
        archive_format = "wacz"
        archive_path = f"{storage_path}{os.sep}archive."

        if current_app.config["DOWNGRADE_TO_WARC"]:
            archive_format = "warc-gzipped"

        if archive_format == "wacz":
            archive_path += "wacz"
        else:
            archive_path += "warc.gz"

        #
        # Mark capture as "started"
        #
        Capture.update(
            status="started"
        ).where(
            Capture.id_capture == capture.id_capture,
            Capture.status == "pending"
        ).execute()
        # https://github.com/harvard-lil/perma/blob/develop/perma_web/perma/models.py#L2115C1-L2118C18

        # Get a fresh copy of the capture job
        capture = Capture.get(Capture.id_capture == capture.id_capture)
        capture.started_timestamp = datetime.datetime.utcnow()
        capture.save()

        print(f"{log_prefix(capture)} Marked as started")

        #
        # Create capture-specific folders
        #
        os.makedirs(storage_path)
        print(f"{log_prefix(capture)} Temporary storage folder: {storage_path}")
        os.makedirs(attachments_path)

        #
        # Run capture
        #
        scoop_args = [
            "npx",
            "scoop",
            capture.url,
            "--output",
            archive_path,
            "--format",
            archive_format,
            "--json-summary-output",
            json_summary_path,
            "--export-attachments-output",
            attachments_path,
            "--proxy-port",
            str(proxy_port),
        ]

        for key, value in scoop_options.items():
            scoop_args.append(key)
            scoop_args.append(str(value))

        process = subprocess.Popen(
            scoop_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        scoop_stdout, scoop_stderr = process.communicate(
            # Enforce hard timeout after SCOOP_TIMEOUT_FUSE seconds past capture timeout
            timeout=scoop_options["--capture-timeout"] / 1000
            + int(current_app.config["SCOOP_TIMEOUT_FUSE"])
        )

        scoop_exit_code = process.poll()

        if scoop_exit_code is None:  # Kill rogue process after 5 second wait
            print(f"{log_prefix(capture)} Process still running - terminating in 5s")
            time.sleep(5)
            process.kill()

        capture.stdout_logs = scoop_stdout.decode("utf-8")
        capture.stderr_logs = scoop_stderr.decode("utf-8")

        #
        # Check capture results
        #

        # Assume capture failed until proven otherwise
        capture.status = "failed"
        capture.ended_timestamp = datetime.datetime.utcnow()
        success = False
        failed_reason = ""

        if scoop_exit_code != 0:
            failed_reason = f"exit code {scoop_exit_code}"

        # Confirm capture success
        if scoop_exit_code == 0:
            success = True

            # Archive file must exist
            if not os.path.exists(archive_path):
                failed_reason = f"{archive_path} not found"
                success = False

            # JSON summary must exist
            if not os.path.exists(json_summary_path) and not failed_reason:
                failed_reason = f"{json_summary_path} not found"
                success = False

            # Analyze JSON summary and:
            # - Check that expected extracted attachments are indeed on disk
            # - Store a copy of the summary in the database
            if success:
                with open(json_summary_path) as file:
                    json_summary = json.load(file)
                    filenames_to_check = []

                    capture.summary = json_summary  # Store copy of JSON summary

                    for filename in json_summary["attachments"].values():
                        if isinstance(filename, list):  # Example: "certificates" is a list
                            filenames_to_check = filenames_to_check + filename
                        else:
                            filenames_to_check.append(filename)

                    for filename in filenames_to_check:
                        filepath = f"{attachments_path}{os.sep}{filename}"
                        if not os.path.exists(filepath):
                            print(f"{log_prefix(capture)} Failed ({filepath} not found)")
                            success = False

        # Report on status and update database record
        if success:
            print(f"{log_prefix(capture)} Success")
            capture.status = "success"
        else:
            print(f"{log_prefix(capture)} Failed ({failed_reason})")
            capture.status = "failed"

        capture.save()

    #
    # Edge case: Scoop keeps running more than SCOOP_TIMEOUT_FUSE seconds after capture:
    # Process timeout has been reached
    #
    except subprocess.TimeoutExpired:
        if capture:
            capture.status = "failed"
            capture.ended_timestamp = datetime.datetime.utcnow()
            capture.save()
            print(f"{log_prefix(capture)} Failed (timeout violation)")

        if process:
            process.kill()
    #
    # Catch-all
    #
    except Exception:
        if capture:
            capture.status = "failed"
            capture.ended_timestamp = datetime.datetime.utcnow()
            capture.save()
            print(f"{log_prefix(capture)} Failed (other, see logs)")

        print(traceback.format_exc())  # Full trace should be in the logs
    #
    # Voluntary interruption
    #
    except:  # noqa: we can't intercept interrupt signals if we specify an exception type
        print("Operation aborted")

        if capture:
            if capture.status != "failed":
                capture.status = "failed"

            capture.ended_timestamp = datetime.datetime.utcnow()
            capture.save()  # Update capture state

    #
    # In any case: call webhook, if any
    #
    finally:
        if capture and capture.callback_url:
            try:
                print(f"{log_prefix(capture)} Callback to {capture.callback_url}")

                # Workaround to be able to use Flask's jsonify, for consistency across the app
                json_data = json.loads(jsonify(capture_to_dict(capture)).data.decode("utf-8"))

                requests.post(capture.callback_url, json=json_data, timeout=10)
            except Exception:
                print(f"{log_prefix(capture)} Callback to {capture.callback_url} failed")
                print(traceback.format_exc())  # Full trace should be in the logs
        start_capture_process.delay(proxy_port=proxy_port)
        return


def log_prefix(capture=None) -> str:
    """Returns a log prefix to be added at the beginning of each line."""
    timestamp = datetime.datetime.utcnow().isoformat(sep="T", timespec="auto")

    if capture:
        return f"[{timestamp}] Capture #{capture.id_capture} |"
    else:
        return f"[{timestamp}] (Pre-capture) |"
