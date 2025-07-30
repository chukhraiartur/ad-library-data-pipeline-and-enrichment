#!/usr/bin/env python3
"""
Script to automatically clean up stuck Airflow tasks and DAG runs.
This script can be run as a cron job or called manually.
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from typing import List

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.utils.session import create_session
from airflow.utils.state import State

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def cleanup_stuck_dag_runs(max_age_minutes: int = 30) -> int:
    """
    Clean up DAG runs that have been running for too long.
    
    Args:
        max_age_minutes: Maximum age in minutes before considering a DAG run stuck
        
    Returns:
        Number of DAG runs cleaned up
    """
    cutoff_time = datetime.utcnow() - timedelta(minutes=max_age_minutes)
    cleaned_count = 0
    
    with create_session() as session:
        # Find stuck DAG runs
        stuck_runs = session.query(DagRun).filter(
            DagRun.state == State.RUNNING,
            DagRun.start_date < cutoff_time
        ).all()
        
        for dag_run in stuck_runs:
            logger.info(f"Cleaning up stuck DAG run: {dag_run.dag_id} - {dag_run.run_id}")
            
            # Mark all tasks in this DAG run as failed
            task_instances = session.query(TaskInstance).filter(
                TaskInstance.dag_id == dag_run.dag_id,
                TaskInstance.run_id == dag_run.run_id
            ).all()
            
            for ti in task_instances:
                if ti.state in [State.RUNNING, State.QUEUED]:
                    ti.state = State.FAILED
                    ti.end_date = datetime.utcnow()
                    logger.info(f"  - Marked task {ti.task_id} as FAILED")
            
            # Mark DAG run as failed
            dag_run.state = State.FAILED
            dag_run.end_date = datetime.utcnow()
            cleaned_count += 1
        
        session.commit()
    
    logger.info(f"Cleaned up {cleaned_count} stuck DAG runs")
    return cleaned_count


def cleanup_stuck_task_instances(max_age_minutes: int = 15) -> int:
    """
    Clean up individual task instances that have been running for too long.
    
    Args:
        max_age_minutes: Maximum age in minutes before considering a task stuck
        
    Returns:
        Number of task instances cleaned up
    """
    cutoff_time = datetime.utcnow() - timedelta(minutes=max_age_minutes)
    cleaned_count = 0
    
    with create_session() as session:
        # Find stuck task instances
        stuck_tasks = session.query(TaskInstance).filter(
            TaskInstance.state == State.RUNNING,
            TaskInstance.start_date < cutoff_time
        ).all()
        
        for task in stuck_tasks:
            logger.info(f"Cleaning up stuck task: {task.dag_id}.{task.task_id} - {task.run_id}")
            task.state = State.FAILED
            task.end_date = datetime.utcnow()
            cleaned_count += 1
        
        session.commit()
    
    logger.info(f"Cleaned up {cleaned_count} stuck task instances")
    return cleaned_count


def cleanup_old_failed_runs(max_age_hours: int = 24) -> int:
    """
    Clean up old failed DAG runs to prevent database bloat.
    
    Args:
        max_age_hours: Maximum age in hours before cleaning up failed runs
        
    Returns:
        Number of old runs cleaned up
    """
    cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)
    cleaned_count = 0
    
    with create_session() as session:
        # Find old failed DAG runs
        old_runs = session.query(DagRun).filter(
            DagRun.state.in_([State.FAILED, State.SUCCESS]),
            DagRun.end_date < cutoff_time
        ).all()
        
        for dag_run in old_runs:
            logger.info(f"Cleaning up old DAG run: {dag_run.dag_id} - {dag_run.run_id}")
            
            # Delete associated task instances
            session.query(TaskInstance).filter(
                TaskInstance.dag_id == dag_run.dag_id,
                TaskInstance.run_id == dag_run.run_id
            ).delete()
            
            # Delete the DAG run
            session.delete(dag_run)
            cleaned_count += 1
        
        session.commit()
    
    logger.info(f"Cleaned up {cleaned_count} old DAG runs")
    return cleaned_count


def main():
    """Main cleanup function."""
    logger.info("Starting Airflow cleanup process...")
    
    try:
        # Clean up stuck DAG runs (running for more than 30 minutes)
        stuck_dags = cleanup_stuck_dag_runs(max_age_minutes=30)
        
        # Clean up stuck task instances (running for more than 15 minutes)
        stuck_tasks = cleanup_stuck_task_instances(max_age_minutes=15)
        
        # Clean up old failed runs (older than 24 hours)
        old_runs = cleanup_old_failed_runs(max_age_hours=24)
        
        total_cleaned = stuck_dags + stuck_tasks + old_runs
        logger.info(f"Cleanup completed. Total items cleaned: {total_cleaned}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 