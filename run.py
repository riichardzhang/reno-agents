"""
Single entry point for all Railway services.
Reads JOB and JOB_FLAGS environment variables to determine what to run.

Set per-service in Railway dashboard (Variables tab):
  daily_run service:
    JOB=daily_run
    JOB_FLAGS=--skip-fetch --skip-insights

  suburb_analysis service:
    JOB=suburb_analysis
    JOB_FLAGS=--skip-backfill --skip-vision
"""

import os
import sys
import runpy

job   = os.getenv("JOB", "daily_run")
flags = os.getenv("JOB_FLAGS", "").split()

sys.argv = [job] + flags

if job == "daily_run":
    runpy.run_module("jobs.daily_run", run_name="__main__")
elif job == "suburb_analysis":
    runpy.run_module("jobs.suburb_analysis", run_name="__main__")
else:
    print(f"✗ Unknown JOB: '{job}'. Set JOB=daily_run or JOB=suburb_analysis")
    sys.exit(1)
