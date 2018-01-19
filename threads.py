from celery import Celery
import time
import psycopg2

app = Celery('threads', backend='amqp',broker='pyamqp://guest@localhost//')

@app.task
def batch_insert(rows):
    conn = psycopg2.connect(database="postgres", user="postgres", password="password", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    try:
        for row in rows:
            log=row.split("~")
            cur.execute("insert into logs(logline,hash) values(%s,%s)",(log[0],log[1]))

        conn.commit()
        conn.close()
        rows=rows[-1:]
    except psycopg2.DatabaseError as e:
        print(e)
