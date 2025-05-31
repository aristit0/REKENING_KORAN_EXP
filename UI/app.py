import os
import time
from flask import Flask, request, render_template
import phoenixdb
import phoenixdb.cursor
from datetime import datetime

# Set project base directory
BASE_DIR = "/home/cdsw/UI"

# Create Flask app with correct template/static folder references
app = Flask(
    __name__,
    template_folder=os.path.join(BASE_DIR, "templates"),
    static_folder=os.path.join(BASE_DIR, "static")
)

# Phoenix Query Server via Knox Gateway
database_url = 'https://cdpm1.cloudeka.ai:8443/gateway/cdp-proxy-api/avatica'

# Connect to PhoenixDB using BASIC auth
conn = phoenixdb.connect(
    database_url,
    autocommit=True,
    authentication='BASIC',
    avatica_user='cmluser',
    avatica_password='hwj5GpM8rVgy'
)
cursor = conn.cursor()

@app.route("/", methods=["GET", "POST"])
def index():
    message = ""
    if request.method == "POST":
        try:
            nip = request.form.get("nip")
            acctno = request.form.get("acctno")
            start_date = request.form.get("start_date")
            end_date = request.form.get("end_date")

            req_date = datetime.now().strftime("%Y%m%d")
            job_id = int(time.time() * 1000)  # Milliseconds since epoch

            cursor.execute("""
                UPSERT INTO REKENING_KORAN.CRUD 
                (JOB_ID, NIP, ACCTNO, START_DATE, END_DATE, JOB_STATUS, REQ_DATE)
                VALUES (?, ?, ?, ?, ?, 'SUBMITTED', ?)
            """, (job_id, int(nip), int(acctno), int(start_date), int(end_date), int(req_date)))

            message = f"NIP {nip} successfully requested document. Your job ID is {job_id}."
        except Exception as e:
            message = f"Error: {str(e)}"

    return render_template("index.html", message=message)

@app.route("/track", methods=["GET", "POST"])
def track():
    results = []
    error = ""
    if request.method == "POST":
        job_id_str = request.form.get("job_id", "").strip()
        if job_id_str:
            try:
                job_id = int(job_id_str)
                cursor.execute("""
                    SELECT JOB_ID, NIP, ACCTNO, START_DATE, END_DATE, JOB_STATUS 
                    FROM REKENING_KORAN.CRUD WHERE JOB_ID = ?
                """, (job_id,))
                results = cursor.fetchall()
                if not results:
                    error = f"No job found with that ID: {job_id}"
            except Exception as e:
                error = f"Query error: {str(e)}"
        else:
            error = "Please provide a valid Job ID."

    return render_template("track.html", results=results, error=error)
# Start the app in CML environment
if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ["CDSW_APP_PORT"]))