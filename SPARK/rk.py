from pyspark.sql import SparkSession
from datetime import datetime
import sys
import io
import math
import pandas as pd
from PyPDF2 import PdfReader, PdfWriter
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Frame, KeepTogether
from reportlab.lib import colors
from reportlab.pdfgen import canvas
import subprocess

# --- Arguments ---
if len(sys.argv) != 6:
    print("Usage: generate_report.py <job_id> <nip> <account_number> <start_date_yyyymmdd> <end_date_yyyymmdd>")
    sys.exit(1)

job_id, nip, account_number, start_date, end_date = sys.argv[1:6]

def yyyymmdd_to_julian(date_str):
    return int(datetime.strptime(date_str, "%Y%m%d").strftime("%Y%j"))

start_julian = yyyymmdd_to_julian(start_date)
end_julian = yyyymmdd_to_julian(end_date)

# --- Spark Init ---
spark = SparkSession.builder.appName("PhoenixZKReport").getOrCreate()

# --- Read Phoenix Tables ---
def read_phoenix_table(table):
    return spark.read \
        .format("org.apache.phoenix.spark") \
        .option("table", table) \
        .option("zkUrl", "cdpm1.cloudeka.ai,cdpm2.cloudeka.ai,cdpm3.cloudeka.ai:2181:/hbase") \
        .load()

df_tx = read_phoenix_table("REKENING_KORAN.TRANSACTION")
df_dm = read_phoenix_table("REKENING_KORAN.DEMOGRAPHY")

# --- Format TRTIME as HH:mm:ss ---
from pyspark.sql.functions import col, format_string, floor

df_tx = df_tx.withColumn("TRTIME", format_string(
    "%02d:%02d:%02d",
    floor(col("TRTIME") / 10000),
    floor((col("TRTIME") % 10000) / 100),
    col("TRTIME") % 100
))

# --- Filter + Rename + Format ---
df_filtered = df_tx.filter(
    (df_tx.TRACCT == account_number) &
    (df_tx.TRDATE >= start_julian) &
    (df_tx.TRDATE <= end_julian)
)

df_selected = df_filtered.selectExpr(
    "TRACCT as `NOMOR REKENING`",
    "DATE_FORMAT(TO_DATE(CAST(TRDATE AS STRING), 'yyyyDDD'), 'yyyy-MM-dd') as `TANGGAL TRANSAKSI`",
    "TRTIME as `JAM TRANSAKSI`",
    "TRREMK as `DESKRIPSI TRANSAKSI`",
    "CASE WHEN GLSIGN = 'Db' THEN 'DEBIT' WHEN GLSIGN = 'Cr' THEN 'CREDIT' ELSE GLSIGN END as `JENIS TRANSAKSI`",
    "concat('Rp. ', format_number(AMT, 2)) as `NOMINAL`"
)

demography = df_dm.filter(df_dm.CIFNO == 'RIFF012').limit(1).toPandas()
tx_data = df_selected.toPandas()
tx_data.insert(0, "NO", range(1, len(tx_data) + 1))

if tx_data.empty:
    print("No data found.")
    sys.exit(0)

# --- Extract customer info ---
info = {
    "NAMA LENGKAP": demography.get("NAMA_LENGKAP", ["-"])[0],
    "TANGGAL LAHIR": demography.get("TANGGAL_LAHIR", ["-"])[0],
    "ALAMAT": " ".join([
        demography.get("ALAMAT_ID1", [""])[0],
        demography.get("ALAMAT_ID2", [""])[0],
        demography.get("ALAMAT_ID3", [""])[0],
    ]).strip() or "-",
    "JENIS PEKERJAAN": demography.get("JENIS_PEKERJAAN", ["-"])[0]
}

# --- PDF Generator ---
def create_pdf(path, watermark_text, tx_df, customer_info):
    watermark_buffer = io.BytesIO()
    c = canvas.Canvas(watermark_buffer, pagesize=letter)
    width, height = letter
    c.setFont("Helvetica-Bold", 18)
    c.setFillColor(colors.grey, alpha=0.08)
    angle_rad = math.radians(30)
    step = max(width, height) / 5
    for i in range(-3, 8):
        for j in range(-3, 8):
            x = i * step * math.cos(angle_rad)
            y = j * step * math.sin(angle_rad) + height / 3
            c.saveState()
            c.translate(x, y)
            c.rotate(-30)
            for idx, line in enumerate(watermark_text.split('\n')):
                if line.strip():
                    c.drawString(0, -idx * 35, line)
            c.restoreState()
    c.save()

    content_buffer = io.BytesIO()
    doc = SimpleDocTemplate(content_buffer, pagesize=letter)
    styles = getSampleStyleSheet()

    # --- Info Section Table ---
    info_table = Table([
        [Paragraph("<b>NAMA LENGKAP:</b> " + info["NAMA LENGKAP"], styles['Normal']),
         Paragraph("<b>ALAMAT:</b> " + info["ALAMAT"], styles['Normal'])],
        [Paragraph("<b>TANGGAL LAHIR:</b> " + str(info["TANGGAL LAHIR"]), styles['Normal']),
         Paragraph("<b>JENIS PEKERJAAN:</b> " + info["JENIS PEKERJAAN"], styles['Normal'])]
    ], colWidths=[3.5*inch, 3.5*inch])

    info_table.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6)
    ]))

    # --- Transaction Table ---
    table_data = [tx_df.columns.tolist()] + tx_df.values.tolist()
    col_widths = [0.35 * inch] + [1.3 * inch] * (len(tx_df.columns) - 1)

    transaction_table = Table(table_data, repeatRows=1, colWidths=col_widths, style=TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#3E5F8A")),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('FONTSIZE', (0, 1), (-1, -1), 7),
        ('GRID', (0, 0), (-1, -1), 0.3, colors.grey),
    ]))

    elements = [
        Paragraph("INFORMASI NASABAH", styles['Heading2']),
        Spacer(1, 0.2 * inch),
        info_table,
        Spacer(1, 0.3 * inch),
        Paragraph("MUTASI TRANSAKSI", styles['Heading2']),
        Spacer(1, 0.2 * inch),
        transaction_table,
        Spacer(1, 0.3 * inch),
        Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles["Normal"])
    ]

    doc.build(elements)

    background = PdfReader(watermark_buffer)
    foreground = PdfReader(content_buffer)
    writer = PdfWriter()
    for page in foreground.pages:
        page.merge_page(background.pages[0])
        writer.add_page(page)

    with open("/tmp/report.pdf", "wb") as f:
        writer.write(f)

    subprocess.run(["hdfs", "dfs", "-put", "-f", "/tmp/report.pdf", path], check=True)

# --- Final Output Path ---
pdf_hdfs_path = f"/data/rk/report_{job_id}_{nip}_{account_number}_{start_date}_{end_date}.pdf"
create_pdf(pdf_hdfs_path, watermark_text=nip, tx_df=tx_data, customer_info=info)
print(f"PDF saved to HDFS: {pdf_hdfs_path}")