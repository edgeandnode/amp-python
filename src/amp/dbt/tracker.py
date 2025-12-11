"""Model tracking and freshness monitoring for Amp DBT."""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

from amp.dbt.config import load_project_config
from amp.dbt.exceptions import ConfigError
from amp.dbt.state import FreshnessResult, ModelState, StateDatabase


class ModelTracker:
    """Tracks data progress for models."""

    def __init__(self, project_root: Path):
        """Initialize model tracker.

        Args:
            project_root: Root directory of the DBT project
        """
        self.project_root = project_root
        self.state_dir = project_root / '.amp-dbt'
        self.state_db = StateDatabase(self.state_dir / 'state.db')
        self.config = load_project_config(project_root)

    def get_latest_block(self, model_name: str) -> Optional[int]:
        """Get latest block number for a model.

        Args:
            model_name: Name of the model

        Returns:
            Latest block number, or None if not tracked
        """
        state = self.state_db.get_model_state(model_name)
        return state.latest_block if state else None

    def get_latest_timestamp(self, model_name: str) -> Optional[datetime]:
        """Get latest timestamp for a model.

        Args:
            model_name: Name of the model

        Returns:
            Latest timestamp, or None if not tracked
        """
        state = self.state_db.get_model_state(model_name)
        return state.latest_timestamp if state else None

    def update_progress(
        self,
        model_name: str,
        latest_block: Optional[int] = None,
        latest_timestamp: Optional[datetime] = None,
        job_id: Optional[int] = None,
        status: Optional[str] = None,
    ):
        """Update progress for a model.

        Args:
            model_name: Name of the model
            latest_block: Latest block number
            latest_timestamp: Latest timestamp
            job_id: Current job ID
            status: Status ('fresh', 'stale', 'error')
        """
        self.state_db.update_model_state(
            model_name=model_name,
            latest_block=latest_block,
            latest_timestamp=latest_timestamp or datetime.now(),
            job_id=job_id,
            status=status,
        )

    def get_model_state(self, model_name: str) -> Optional[ModelState]:
        """Get full state for a model.

        Args:
            model_name: Name of the model

        Returns:
            ModelState if found, None otherwise
        """
        return self.state_db.get_model_state(model_name)

    def get_all_states(self) -> Dict[str, ModelState]:
        """Get all model states.

        Returns:
            Dictionary mapping model names to ModelState
        """
        states = self.state_db.get_all_model_states()
        return {state.model_name: state for state in states}


class FreshnessMonitor:
    """Monitors data freshness for models."""

    def __init__(self, tracker: ModelTracker):
        """Initialize freshness monitor.

        Args:
            tracker: ModelTracker instance
        """
        self.tracker = tracker
        self.config = tracker.config

    def get_alert_threshold(self, model_name: str) -> timedelta:
        """Get alert threshold for a model.

        Args:
            model_name: Name of the model

        Returns:
            Alert threshold as timedelta
        """
        # Check model-specific config
        if 'models' in self.config:
            # Try to find model-specific threshold
            # For now, use default
            pass

        # Get default from monitoring config
        default_minutes = 30
        if 'monitoring' in self.config:
            default_minutes = self.config['monitoring'].get('alert_threshold_minutes', 30)

        return timedelta(minutes=default_minutes)

    def check_freshness(self, model_name: str) -> FreshnessResult:
        """Check freshness of a model.

        Args:
            model_name: Name of the model

        Returns:
            FreshnessResult with freshness information
        """
        state = self.tracker.get_model_state(model_name)

        if not state or not state.latest_timestamp:
            return FreshnessResult(
                stale=True, reason='No data tracked', latest_block=state.latest_block if state else None
            )

        now = datetime.now()
        age = now - state.latest_timestamp
        threshold = self.get_alert_threshold(model_name)

        is_stale = age > threshold

        # Update status in database
        new_status = 'stale' if is_stale else 'fresh'
        if state.status != new_status:
            self.tracker.state_db.update_model_state(model_name, status=new_status)

        return FreshnessResult(
            stale=is_stale,
            age=age,
            latest_block=state.latest_block,
            latest_timestamp=state.latest_timestamp,
            reason='Data is stale' if is_stale else None,
        )

    def check_all_freshness(self) -> Dict[str, FreshnessResult]:
        """Check freshness of all models.

        Returns:
            Dictionary mapping model names to FreshnessResult
        """
        states = self.tracker.get_all_states()
        results = {}

        for model_name in states.keys():
            results[model_name] = self.check_freshness(model_name)

        return results

