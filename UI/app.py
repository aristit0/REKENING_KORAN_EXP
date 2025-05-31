import os
import time
from flask import Flask, request, render_template
import phoenixdb
import phoenixdb.cursor
from datetime import datetime

BASE_DIR = "/home/cdsw/UI"

app = Flask(
    __name__,
    template_folder=os.path.join(BASE_DIR, "templates"),
    static_folder=os.path.join(BASE_DIR, "static")
)

DATABASE_URL = 'https://cdpm1.cloudeka.ai:8443/gateway/cdp-proxy-api/avatica'
PHOENIX_USER = 'cmluser'
PHOENIX_PASSWORD = 'hwj5GpM8rVgy'

def get_cursor():
    conn = phoenixdb.connect(
        DATABASE_URL,
        autocommit=True,
        authentication='BASIC',
        avatica_user=PHOENIX_USER,
        avatica_password=PHOENIX_PASSWORD
    )
    return conn.cursor()

@app.route("/", methods=["GET", "POST"])
def index():
    message = ""
    job_id = None

    if request.method == "POST":
        try:
            nip = request.form.get("nip")
            acctno = request.form.get("acctno")
            start_date = request.form.get("start_date")
            end_date = request.form.get("end_date")
            doc_type = request.form.get("type", "PDF").upper()

            job_id = int(time.time() * 1000)
            req_date = int(datetime.now().strftime("%Y%m%d"))

            cursor = get_cursor()
            cursor.execute("""
                UPSERT INTO REKENING_KORAN.CRUD 
                (JOB_ID, NIP, ACCTNO, START_DATE, END_DATE, JOB_STATUS, REQ_DATE, TYPE)
                VALUES (?, ?, ?, ?, ?, 'SUBMITTED', ?, ?)
            """, (
                job_id, int(nip), int(acctno),
                int(start_date), int(end_date),
                req_date, doc_type
            ))

            message = f"NIP {nip} successfully requested document ({doc_type}). Your job ID is {job_id}."
        except Exception as e:
            message = f"Error: {str(e)}"

    return render_template("index.html", message=message, job_id=job_id)

@app.route("/track", methods=["GET"])
def track():
    job_id = request.args.get("job_id")
    results = []

    if job_id:
        try:
            cursor = get_cursor()
            cursor.execute("""
                SELECT JOB_ID, NIP, ACCTNO, START_DATE, END_DATE, JOB_STATUS, LINK_DOWNLOAD, TYPE
                FROM REKENING_KORAN.CRUD 
                WHERE JOB_ID = ?
            """, (int(job_id),))
            results = cursor.fetchall()
        except Exception as e:
            results = [("Error", str(e), "", "", "", "", "", "")]

    return render_template("track.html", results=results)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ["CDSW_APP_PORT"]))