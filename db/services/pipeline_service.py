from datetime import datetime
from db.session_db import get_session
from db.schemas.pipeline_run import PipelineRun
from db.schemas.task import Task


def create_pipeline_run(source_name):
    session = get_session()

    run = PipelineRun(
        source_name=source_name,
        status="RUNNING",
        start_time=datetime.utcnow()
    )

    session.add(run)
    session.commit()
    session.refresh(run)
    session.close()

    return run.run_id


def update_pipeline_run(run_id, status):
    session = get_session()

    run = session.query(PipelineRun).filter_by(run_id=run_id).first()
    run.status = status
    run.end_time = datetime.utcnow()

    session.commit()
    session.close()


def log_task(run_id, task_name, status, start_time, records_count=0, error_message=None):
    session = get_session()

    task = Task(
        run_id=run_id,
        task_name=task_name,
        status=status,
        start_time=start_time,
        end_time=datetime.utcnow(),
        records_count=records_count,
        error_message=error_message
    )

    session.add(task)
    session.commit()
    session.close()