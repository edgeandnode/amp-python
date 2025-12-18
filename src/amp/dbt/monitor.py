"""Job monitoring for Amp DBT."""

from datetime import datetime
from typing import Dict, List, Optional

from amp.admin.jobs import JobsClient
from amp.admin.models import JobInfo
from amp.dbt.state import JobHistory, ModelState, StateDatabase
from amp.dbt.tracker import ModelTracker


class JobMonitor:
    """Monitors job status and updates model state."""

    def __init__(self, jobs_client: JobsClient, tracker: ModelTracker):
        """Initialize job monitor.

        Args:
            jobs_client: JobsClient instance for querying job status
            tracker: ModelTracker instance for updating state
        """
        self.jobs_client = jobs_client
        self.tracker = tracker

    def monitor_job(self, job_id: int, model_name: str) -> JobInfo:
        """Monitor a job and update model state.

        Args:
            job_id: Job ID to monitor
            model_name: Name of the model associated with this job

        Returns:
            JobInfo with current job status
        """
        job = self.jobs_client.get(job_id)

        # Update model state based on job status
        if job.status == 'Running':
            # Job is running - mark as in progress
            self.tracker.update_progress(model_name, job_id=job_id, status='running')
        elif job.status == 'Completed':
            # Job completed - update state
            # Note: In a real implementation, we'd extract latest_block from job metadata
            self.tracker.update_progress(model_name, job_id=job_id, status='fresh')
            self._record_job_completion(job_id, model_name, job.status)
        elif job.status == 'Failed':
            # Job failed - mark as error
            self.tracker.update_progress(model_name, job_id=job_id, status='error')
            self._record_job_completion(job_id, model_name, job.status)
        elif job.status == 'Stopped':
            # Job stopped - mark appropriately
            self.tracker.update_progress(model_name, job_id=job_id, status='stale')
            self._record_job_completion(job_id, model_name, job.status)

        return job

    def _record_job_completion(self, job_id: int, model_name: str, status: str):
        """Record job completion in history.

        Args:
            job_id: Job ID
            model_name: Model name
            status: Final job status
        """
        job = self.jobs_client.get(job_id)

        # Extract job metadata (this would need to be enhanced based on actual JobInfo structure)
        job_history = JobHistory(
            job_id=job_id,
            model_name=model_name,
            status=status,
            completed_at=datetime.now(),
            # Note: In real implementation, extract started_at, final_block, rows_processed
            # from job.descriptor or job metadata
        )

        self.tracker.state_db.add_job_history(job_history)

    def monitor_all(self, model_job_map: Dict[str, int]) -> Dict[str, JobInfo]:
        """Monitor all jobs for given models.

        Args:
            model_job_map: Dictionary mapping model names to job IDs

        Returns:
            Dictionary mapping model names to JobInfo
        """
        results = {}

        for model_name, job_id in model_job_map.items():
            try:
                job_info = self.monitor_job(job_id, model_name)
                results[model_name] = job_info
            except Exception:
                # Skip models with errors
                pass

        return results

    def get_active_jobs(self, model_job_map: Dict[str, int]) -> Dict[str, JobInfo]:
        """Get all active (running/pending) jobs.

        Args:
            model_job_map: Dictionary mapping model names to job IDs

        Returns:
            Dictionary mapping model names to JobInfo for active jobs
        """
        active_statuses = {'Running', 'Pending', 'Scheduled'}
        results = {}

        for model_name, job_id in model_job_map.items():
            try:
                job = self.jobs_client.get(job_id)
                if job.status in active_statuses:
                    results[model_name] = job
            except Exception:
                pass

        return results

