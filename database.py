from sqlalchemy import create_engine,text
import os

db_connection_string=os.environ['DB_CONNECTION']
engine=create_engine(db_connection_string)


def load_jobs_from_db():
  with engine.connect() as conn:
    result=conn.execute(text("select * from jobs"))
    jobs=[]
    for row in result.all():
      jobs.append(row._asdict())
    return jobs

def load_job_from_db(id):
  with engine.connect() as conn:
    result=conn.execute(text("select * from jobs where id=:id"),{"id":id})
    rows=result.all()
    if len(rows)==0:
      return None
    else:
      return rows[0]._asdict()