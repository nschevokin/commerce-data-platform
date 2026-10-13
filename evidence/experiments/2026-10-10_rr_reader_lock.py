import threading
import time

import psycopg

DSN = "host=127.0.0.1 port=5433 user=postgres dbname=postgres"


def setup():
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute("DROP SCHEMA IF EXISTS exp CASCADE; CREATE SCHEMA exp")
        c.execute("CREATE TABLE exp.pointer(release text); INSERT INTO exp.pointer VALUES ('A')")
        c.execute("CREATE TABLE exp.a(v int); INSERT INTO exp.a VALUES (1)")
        c.execute("CREATE TABLE exp.b(v int); INSERT INTO exp.b VALUES (2)")
        c.execute("CREATE VIEW exp.release_info AS SELECT 'A'::text AS release_id")
        c.execute("CREATE VIEW exp.biz AS SELECT v FROM exp.a")
        c.execute("CREATE VIEW exp.health AS SELECT release FROM exp.pointer")


def run(lock_first: bool):
    setup()
    pub = psycopg.connect(DSN)
    pub.execute("UPDATE exp.pointer SET release='B'")
    pub.execute("CREATE OR REPLACE VIEW exp.release_info AS SELECT 'B'::text AS release_id")
    out = {}

    def reader():
        with psycopg.connect(DSN) as r:
            r.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            if lock_first:
                r.execute("LOCK TABLE exp.release_info IN ACCESS SHARE MODE")
            out["release_info"] = r.execute("SELECT release_id FROM exp.release_info").fetchone()[0]
            out["biz"] = r.execute("SELECT v FROM exp.biz").fetchone()[0]
            out["health"] = r.execute("SELECT release FROM exp.health").fetchone()[0]
            r.execute("COMMIT")

    t = threading.Thread(target=reader)
    t.start()
    time.sleep(1.0)
    pub.execute("CREATE OR REPLACE VIEW exp.biz AS SELECT v FROM exp.b")
    pub.commit()
    t.join()
    pub.close()
    return out


for lf in (False, True):
    print(f"lock_first={lf}:", run(lf))
