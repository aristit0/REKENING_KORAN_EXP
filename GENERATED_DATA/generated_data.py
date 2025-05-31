import random
import uuid
from datetime import datetime, timedelta
import phoenixdb
import phoenixdb.cursor

# Setup connection
database_url = 'https://cdpm1.cloudeka.ai:8443/gateway/cdp-proxy-api/avatica'
conn = phoenixdb.connect(
    database_url,
    autocommit=True,
    authentication='BASIC',
    avatica_user='cmluser',
    avatica_password='hwj5GpM8rVgy'
)
cursor = conn.cursor()

# Julian date converter
def get_julian_date(date):
    return int(date.strftime('%Y') + date.strftime('%j'))

# Generate one row
def generate_transaction(seq_numb, tracct):
    now = datetime.now()
    delta_days = random.randint(0, 29)
    random_date = now - timedelta(days=delta_days)
    trdate = get_julian_date(random_date)
    trtime = random.randint(0, 235959)

    glsign = random.choice(['Cr', 'Db'])
    amount = round(random.uniform(1000, 100000), 2)
    debit = amount if glsign == 'Db' else 0.0
    credit = amount if glsign == 'Cr' else 0.0

    return (
        tracct,                # TRACCT
        trdate,                # TRDATE
        trtime,                # TRTIME
        seq_numb,              # SEQ_NUMB
        random.randint(1, 10), # TRBR
        "",                    # TRDAT6
        "",                    # TRDORC
        random.randint(1, 9),  # TRANCD
        amount,                # AMT
        str(random.randint(1000, 9999)),   # AUXTRC
        str(uuid.uuid4())[:16],           # TRREMK
        "",                    # EFTACC
        trdate,                # TREFFD
        "",                    # TREFF6
        str(random.randint(1000000, 9999999)), # TRUSER
        "", "",                # TRATYP, TRCTYP
        random.randint(100000000, 999999999),  # SEQ
        random.randint(10000000, 99999999),    # SERIAL
        "desc1",               # TLBDS1
        "desc2",               # TLBDS2
        debit,                 # DEBIT
        credit,                # KREDIT
        glsign                 # GLSIGN
    )

# Insert loop
tracct = 123701007794509
total_rows = 5_000_000

insert_sql = """
UPSERT INTO REKENING_KORAN.TRANSACTION (
    TRACCT, TRDATE, TRTIME, SEQ_NUMB, TRBR, TRDAT6, TRDORC, TRANCD, AMT,
    AUXTRC, TRREMK, EFTACC, TREFFD, TREFF6, TRUSER, TRATYP, TRCTYP, SEQ,
    SERIAL, TLBDS1, TLBDS2, DEBIT, KREDIT, GLSIGN
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

for i in range(1, total_rows + 1):
    row = generate_transaction(i, tracct)
    cursor.execute(insert_sql, row)
    if i % 10000 == 0:
        print(f"{i} rows inserted...")

conn.commit()
conn.close()
print("✅ All 5,000,000 rows inserted successfully.")