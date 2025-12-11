"""State management for Amp DBT monitoring and tracking."""

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from amp.dbt.exceptions import ConfigError


@dataclass
class ModelState:
    """State information for a model."""

    model_name: str
    connection_name: Optional[str] = None
    latest_block: Optional[int] = None
    latest_timestamp: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    job_id: Optional[int] = None
    status: str = 'unknown'  # 'fresh', 'stale', 'error', 'unknown'


@dataclass
class JobHistory:
    """History record for a job."""

    job_id: int
    model_name: str
    status: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    final_block: Optional[int] = None
    rows_processed: Optional[int] = None


@dataclass
class FreshnessResult:
    """Result of a freshness check."""

    stale: bool
    age: Optional[timedelta] = None
    latest_block: Optional[int] = None
    latest_timestamp: Optional[datetime] = None
    reason: Optional[str] = None


class StateDatabase:
    """SQLite database for storing model state and job history."""

    def __init__(self, db_path: Path):
        """Initialize state database.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _init_schema(self):
        """Initialize database schema."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Create model_state table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS model_state (
                model_name TEXT PRIMARY KEY,
                connection_name TEXT,
                latest_block INTEGER,
                latest_timestamp TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                job_id INTEGER,
                status TEXT
            )
        ''')

        # Create job_history table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS job_history (
                job_id INTEGER PRIMARY KEY,
                model_name TEXT,
                status TEXT,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                final_block INTEGER,
                rows_processed INTEGER
            )
        ''')

        # Create indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_model_state_status ON model_state(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_job_history_model ON job_history(model_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_job_history_status ON job_history(status)')

        conn.commit()
        conn.close()

    def get_model_state(self, model_name: str) -> Optional[ModelState]:
        """Get state for a model.

        Args:
            model_name: Name of the model

        Returns:
            ModelState if found, None otherwise
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute(
            'SELECT * FROM model_state WHERE model_name = ?', (model_name,)
        )
        row = cursor.fetchone()
        conn.close()

        if not row:
            return None

        return ModelState(
            model_name=row['model_name'],
            connection_name=row['connection_name'],
            latest_block=row['latest_block'],
            latest_timestamp=datetime.fromisoformat(row['latest_timestamp'])
            if row['latest_timestamp']
            else None,
            last_updated=datetime.fromisoformat(row['last_updated'])
            if row['last_updated']
            else None,
            job_id=row['job_id'],
            status=row['status'] or 'unknown',
        )

    def update_model_state(
        self,
        model_name: str,
        latest_block: Optional[int] = None,
        latest_timestamp: Optional[datetime] = None,
        job_id: Optional[int] = None,
        status: Optional[str] = None,
        connection_name: Optional[str] = None,
    ):
        """Update state for a model.

        Args:
            model_name: Name of the model
            latest_block: Latest block number processed
            latest_timestamp: Latest timestamp processed
            job_id: Current job ID
            status: Status ('fresh', 'stale', 'error')
            connection_name: Connection name
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Check if record exists
        cursor.execute('SELECT model_name FROM model_state WHERE model_name = ?', (model_name,))
        exists = cursor.fetchone() is not None

        if exists:
            # Update existing record
            updates = []
            params = []

            if latest_block is not None:
                updates.append('latest_block = ?')
                params.append(latest_block)
            if latest_timestamp is not None:
                updates.append('latest_timestamp = ?')
                params.append(latest_timestamp.isoformat())
            if job_id is not None:
                updates.append('job_id = ?')
                params.append(job_id)
            if status is not None:
                updates.append('status = ?')
                params.append(status)
            if connection_name is not None:
                updates.append('connection_name = ?')
                params.append(connection_name)

            updates.append('last_updated = CURRENT_TIMESTAMP')
            params.append(model_name)

            cursor.execute(
                f'UPDATE model_state SET {", ".join(updates)} WHERE model_name = ?', params
            )
        else:
            # Insert new record
            cursor.execute(
                '''
                INSERT INTO model_state 
                (model_name, connection_name, latest_block, latest_timestamp, job_id, status)
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
                (
                    model_name,
                    connection_name,
                    latest_block,
                    latest_timestamp.isoformat() if latest_timestamp else None,
                    job_id,
                    status or 'unknown',
                ),
            )

        conn.commit()
        conn.close()

    def add_job_history(self, job_history: JobHistory):
        """Add a job history record.

        Args:
            job_history: JobHistory record to add
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            '''
            INSERT OR REPLACE INTO job_history
            (job_id, model_name, status, started_at, completed_at, final_block, rows_processed)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                job_history.job_id,
                job_history.model_name,
                job_history.status,
                job_history.started_at.isoformat() if job_history.started_at else None,
                job_history.completed_at.isoformat() if job_history.completed_at else None,
                job_history.final_block,
                job_history.rows_processed,
            ),
        )

        conn.commit()
        conn.close()

    def get_all_model_states(self) -> List[ModelState]:
        """Get all model states.

        Returns:
            List of ModelState objects
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM model_state ORDER BY model_name')
        rows = cursor.fetchall()
        conn.close()

        states = []
        for row in rows:
            states.append(
                ModelState(
                    model_name=row['model_name'],
                    connection_name=row['connection_name'],
                    latest_block=row['latest_block'],
                    latest_timestamp=datetime.fromisoformat(row['latest_timestamp'])
                    if row['latest_timestamp']
                    else None,
                    last_updated=datetime.fromisoformat(row['last_updated'])
                    if row['last_updated']
                    else None,
                    job_id=row['job_id'],
                    status=row['status'] or 'unknown',
                )
            )

        return states

    def get_job_history(self, model_name: Optional[str] = None, limit: int = 50) -> List[JobHistory]:
        """Get job history.

        Args:
            model_name: Optional filter by model name
            limit: Maximum number of records to return

        Returns:
            List of JobHistory objects
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        if model_name:
            cursor.execute(
                'SELECT * FROM job_history WHERE model_name = ? ORDER BY started_at DESC LIMIT ?',
                (model_name, limit),
            )
        else:
            cursor.execute('SELECT * FROM job_history ORDER BY started_at DESC LIMIT ?', (limit,))

        rows = cursor.fetchall()
        conn.close()

        history = []
        for row in rows:
            history.append(
                JobHistory(
                    job_id=row['job_id'],
                    model_name=row['model_name'],
                    status=row['status'],
                    started_at=datetime.fromisoformat(row['started_at'])
                    if row['started_at']
                    else None,
                    completed_at=datetime.fromisoformat(row['completed_at'])
                    if row['completed_at']
                    else None,
                    final_block=row['final_block'],
                    rows_processed=row['rows_processed'],
                )
            )

        return history

