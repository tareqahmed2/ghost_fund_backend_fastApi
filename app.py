import os
import re
from io import BytesIO
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Tuple

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from textwrap import wrap

# PDF generation
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

# config
OUTPUT_XLSX = "ghost_fund_savings.xlsx"
BD_TZ = ZoneInfo("Asia/Dhaka")

# WhatsApp date/time prefix regex
date_prefix = re.compile(
    r"^(\d{1,2}/\d{1,2}/\d{2}),\s+(\d{1,2}:\d{2}\s*[APMapm]{2}) - (.*)$"
)

# Match amounts with currency like "160 tk", "BDT 90", "৳500" etc.
currency_amount_pattern = re.compile(
    r"""
    (?:
        (tk|taka|bdt|৳)\.?\s*([0-9][0-9,]*)   # e.g. "BDT 90" / "Tk. 160"
    )
    |
    (?:
        ([0-9][0-9,]*)\s*(tk|taka|bdt|৳)\.?   # e.g. "160 Tk" / "90 BDT"
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)


def parse_messages_from_string(text: str):
    messages = []
    current = None

    for raw in text.splitlines():
        line = (
            raw.rstrip("\n")
            .replace("\u202f", " ")
            .replace("\u200e", "")
        )

        m = date_prefix.match(line)
        if m:
            if current:
                messages.append(current)

            date_str, time_str, rest = m.groups()
            sender = None
            msg_text = ""

            if ": " in rest:
                sender, msg_text = rest.split(": ", 1)
            else:
                msg_text = rest

            current = {
                "date": date_str.strip(),
                "time": time_str.strip().upper(),
                "sender": sender.strip() if sender else None,
                "text": msg_text.strip(),
            }
        else:
            if current:
                current["text"] += " " + line.strip()

    if current:
        messages.append(current)

    return messages


def is_saving_message(text: str) -> bool:
    """
    Decide if a message is actually a saving entry or not.
    We:
    - skip weekly total announcements (e.g. "My weekly ghost fund...")
    - treat messages with currency+amount as saving
    - allow plain numeric messages as saving ("200")
    """
    if not text:
        return False

    lower = text.lower().strip()

    # 1) Explicitly skip weekly total announcements
    #    e.g. "My weekly ghost fund by Thursday 9 pm : BDT 90"
    if "weekly ghost fund" in lower:
        return False

    # 2) If we see at least one "<amount> + currency" combo -> valid saving
    if list(currency_amount_pattern.finditer(text)):
        return True

    # 3) Fallback: message is just a plain number, e.g. "200"
    #    (for people who only send numeric amount)
    if re.fullmatch(r"\d+", lower):
        return True

    return False


def extract_amount(text: str):
    """
    Extract numeric amount from a message.
    - If there are multiple "<amount> + currency" patterns, sum them.
      e.g. "Saved 160 Tk and 80 Tk" => 240
    - Otherwise, if the whole text is just a number, return that.
    """
    if not text:
        return None

    # 1) Try to find all "<amount> + currency" occurrences
    matches = list(currency_amount_pattern.finditer(text))
    amounts: List[int] = []

    for m in matches:
        # Our regex has either group 2 (currency first) or group 3 (number first)
        num_str = m.group(2) or m.group(3)
        if num_str:
            # remove any non-digit chars inside numbers, e.g. "1,200"
            num_clean = re.sub(r"[^\d]", "", num_str)
            try:
                amounts.append(int(num_clean))
            except ValueError:
                continue

    if amounts:
        # Sum all amounts in the message
        return sum(amounts)

    # 2) Fallback: if whole text is just a number like "200"
    stripped = text.strip()
    if re.fullmatch(r"\d+", stripped):
        return int(stripped)

    return None


def normalize_phone(phone: str) -> str:
    if not phone:
        return ""
    return re.sub(r"[^\d+]", "", phone)


def load_contact_mapping_from_df(df: pd.DataFrame):
    """
    Build mapping:
    - name_map: key = lowercased final_name, value = {name, phone}
    - phone_map: key = normalized phone, value = {name, phone}
    """
    name_map: Dict[str, Dict[str, str]] = {}
    phone_map: Dict[str, Dict[str, str]] = {}

    for _, row in df.iterrows():
        phone_raw = row.get("Phone Number", "")
        phone = str(phone_raw).strip() if not pd.isna(phone_raw) else ""
        phone_norm = normalize_phone(phone)

        saved_raw = row.get("Saved Name", "")
        display_raw = row.get("Contact's Public Display Name", "")

        saved = str(saved_raw).strip() if not pd.isna(saved_raw) else ""
        display = str(display_raw).strip() if not pd.isna(display_raw) else ""

        if saved:
            final_name = saved
        elif display:
            final_name = display
        else:
            final_name = phone if phone else ""

        if not final_name and not phone:
            continue

        entry = {
            "name": final_name if final_name else phone,
            "phone": phone,
        }

        if final_name:
            name_map[final_name.lower()] = entry

        if phone_norm:
            phone_map[phone_norm] = entry

    return name_map, phone_map


def get_last_date_from_excel():
    """
    Read OUTPUT_XLSX and return the max Date from Data sheet.
    If file or sheet does not exist, return None.
    """
    if not os.path.exists(OUTPUT_XLSX):
        return None

    try:
        df = pd.read_excel(OUTPUT_XLSX, sheet_name="Data")
    except Exception:
        return None

    if df.empty or "Date" not in df.columns:
        return None

    df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%y", errors="coerce")
    last_date = df["Date"].max()
    if pd.isna(last_date):
        return None

    return last_date


def build_dataframes_from_messages(messages, contact_df: pd.DataFrame):
    name_map, phone_map = load_contact_mapping_from_df(contact_df)
    last_excel_date = get_last_date_from_excel()

    rows_new = []

    for msg in messages:
        if not msg["sender"]:
            continue

        msg_date = pd.to_datetime(msg["date"], format="%m/%d/%y", errors="coerce")

        # skip older or equal dates if Excel already has data
        if last_excel_date is not None and msg_date is not pd.NaT:
            if msg_date <= last_excel_date:
                continue

        text = msg["text"]
        if not is_saving_message(text):
            continue

        amount = extract_amount(text)
        if not amount:
            continue

        sender_original = msg["sender"].strip()
        sender_key = sender_original.lower()

        final_name = sender_original
        final_phone = ""

        if sender_key in name_map:
            final_name = name_map[sender_key]["name"]
            final_phone = name_map[sender_key]["phone"]
        else:
            if any(ch.isdigit() for ch in sender_original):
                phone_norm = normalize_phone(sender_original)
                if phone_norm in phone_map:
                    final_name = phone_map[phone_norm]["name"]
                    final_phone = phone_map[phone_norm]["phone"]

        rows_new.append(
            {
                "Date": msg["date"],
                "Time": msg["time"],
                "Name": final_name,
                "Phone": final_phone,
                "Amount": amount,
                "howSaved": text,
            }
        )

    df_data_new = pd.DataFrame(rows_new)

    # prepare old data (if any)
    if os.path.exists(OUTPUT_XLSX):
        try:
            df_old = pd.read_excel(OUTPUT_XLSX, sheet_name="Data")
        except Exception:
            df_old = pd.DataFrame(
                columns=["Date", "Time", "Name", "Phone", "Amount", "howSaved"]
            )
    else:
        df_old = pd.DataFrame(
            columns=["Date", "Time", "Name", "Phone", "Amount", "howSaved"]
        )

    if df_data_new.empty:
        # no new rows; just rebuild summary from old data
        df_data = df_old.copy()
    else:
        df_data_new["Date_sort"] = pd.to_datetime(
            df_data_new["Date"], format="%m/%d/%y", errors="coerce"
        )
        df_data_new["Time_sort"] = pd.to_datetime(
            df_data_new["Time"], format="%I:%M %p", errors="coerce"
        ).dt.time
        df_data_new = df_data_new.sort_values(
            ["Date_sort", "Time_sort"], ascending=[False, False]
        )
        df_data_new = df_data_new.drop(columns=["Date_sort", "Time_sort"])



        df_data = pd.concat([df_old, df_data_new], ignore_index=True)

    if df_data.empty:
        df_summary = pd.DataFrame(columns=["Name", "Phone", "Total_Amount"])
    else:
        df_summary = (
            df_data.groupby(["Name", "Phone"], as_index=False)["Amount"]
            .sum()
            .rename(columns={"Amount": "Total_Amount"})
        )

    new_rows_count = len(df_data_new)
    return df_data, df_summary, new_rows_count


def write_excel_from_upload(messages, contact_df: pd.DataFrame):
    df_data, df_summary, new_rows_count = build_dataframes_from_messages(
        messages, contact_df
    )

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        df_data.to_excel(writer, sheet_name="Data", index=False)
        df_summary.to_excel(writer, sheet_name="Summary", index=False)

    return df_data, df_summary, new_rows_count


# show data models

class SavingItem(BaseModel):
    Date: str
    Time: str
    Name: str
    Phone: str
    Amount: float
    howSaved: str


class SummaryItem(BaseModel):
    Name: str
    Phone: str
    Total_Amount: float


app = FastAPI(
    title="Ghost Fund TXT → Excel Viewer",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "https://ghostfundlatest.vercel.app"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def load_dataframes_from_excel():
    if not os.path.exists(OUTPUT_XLSX):
        df_data = pd.DataFrame(
            columns=["Date", "Time", "Name", "Phone", "Amount", "howSaved"]
        )
        df_summary = pd.DataFrame(columns=["Name", "Phone", "Total_Amount"])
        return df_data, df_summary

    df_data = pd.read_excel(OUTPUT_XLSX, sheet_name="Data")
    df_summary = pd.read_excel(OUTPUT_XLSX, sheet_name="Summary")
    return df_data, df_summary


# ---------- Helpers for BD time & weekly/monthly/yearly ----------

def get_bd_now() -> datetime:
    return datetime.now(BD_TZ)


def to_bd_datetime(date_str: str, time_str: str) -> datetime:
    """
    Convert 'MM/DD/YY' + 'h:mm AM/PM' into a timezone-aware BD datetime.
    """
    try:
        dt = datetime.strptime(f"{date_str} {time_str}", "%m/%d/%y %I:%M %p")
    except ValueError:
        # Fallback if format mismatch
        dt = pd.to_datetime(f"{date_str} {time_str}", errors="coerce")
        if pd.isna(dt):
            # default to now if completely invalid
            return get_bd_now()
        dt = dt.to_pydatetime()

    return dt.replace(tzinfo=BD_TZ)


def get_bd_week_range(now_bd: datetime) -> Tuple[datetime, datetime]:
    """
    Week definition: Friday → Thursday (BD time)
    We return (week_start, week_end)
    In Python weekday(): Monday=0,...,Thursday=3,Friday=4,Saturday=5,Sunday=6
    We want week_end = Thursday.
    """
    thursday = 3
    day = now_bd.weekday()
    delta_to_thursday = (thursday - day) % 7

    week_end = (now_bd + timedelta(days=delta_to_thursday)).replace(
        hour=23, minute=59, second=59, microsecond=999999
    )
    week_start = (week_end - timedelta(days=6)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return week_start, week_end


def build_user_details(identifier: str) -> Dict[str, Any]:
    """
    Build per-user details similar to your dashboard:
    - records (sorted desc)
    - monthly totals
    - yearly totals
    - weekly ranges (Fri→Thu) with totals + records
    identifier can be Name or Phone.
    """
    df_data, _ = load_dataframes_from_excel()
    if df_data.empty:
        raise HTTPException(status_code=404, detail="No data available")

    df = df_data.copy()

    df["Phone"] = df["Phone"].fillna("").astype(str)
    df["Name"] = df["Name"].fillna("").astype(str)
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0)

    mask = (df["Phone"] == identifier) | (df["Name"].str.lower() == identifier.lower())
    df_user = df[mask].copy()

    if df_user.empty:
        raise HTTPException(status_code=404, detail="User not found in Excel data")

    # Build BD datetime column
    df_user["bd_datetime"] = df_user.apply(
        lambda row: to_bd_datetime(str(row["Date"]), str(row["Time"])), axis=1
    )

    # Sort latest first
    df_user = df_user.sort_values("bd_datetime", ascending=False)

    # Records for frontend
    records = []
    for _, row in df_user.iterrows():
        records.append(
            {
                "datetime": row["bd_datetime"].isoformat(),
                "amount": float(row["Amount"]),
                "howSaved": row.get("howSaved") or "",
            }
        )

    # Monthly and yearly aggregates
    monthly: Dict[str, float] = {}
    yearly: Dict[str, float] = {}

    for _, row in df_user.iterrows():
        dt = row["bd_datetime"]
        month_key = dt.strftime("%B %Y")  # e.g. "January 2025"
        year_key = str(dt.year)
        amt = float(row["Amount"])

        monthly[month_key] = monthly.get(month_key, 0.0) + amt
        yearly[year_key] = yearly.get(year_key, 0.0) + amt

    # Weekly ranges
    weeks: List[Dict[str, Any]] = []
    if not df_user.empty:
        oldest_dt: datetime = df_user["bd_datetime"].min()
        earliest_week_start, _ = get_bd_week_range(oldest_dt)
        _, current_week_end = get_bd_week_range(get_bd_now())

        ranges: List[Dict[str, datetime]] = []
        cursor = earliest_week_start
        while cursor <= current_week_end:
            start = cursor
            end = (start + timedelta(days=6)).replace(
                hour=23, minute=59, second=59, microsecond=999999
            )
            ranges.append({ "start": start, "end": end })
            cursor = cursor + timedelta(days=7)

        # latest first
        ranges.reverse()

        # prepare weeks structure
        for r in ranges:
            weeks.append(
                {
                    "start": r["start"],
                    "end": r["end"],
                    "records": [],
                    "total": 0.0,
                }
            )

        # assign each record into a week
        for _, row in df_user.iterrows():
            dt = row["bd_datetime"]
            amt = float(row["Amount"])
            rec = {
                "datetime": dt.isoformat(),
                "amount": amt,
                "howSaved": row.get("howSaved") or "",
            }
            for w in weeks:
                if w["start"] <= dt <= w["end"]:
                    w["records"].append(rec)
                    w["total"] += amt
                    break

        # convert datetimes to iso strings for JSON
        for w in weeks:
            w["start"] = w["start"].isoformat()
            w["end"] = w["end"].isoformat()
            w["total"] = float(w["total"])

    first_row = df_user.iloc[0]
    name = str(first_row["Name"]) or "Unknown"
    phone = str(first_row["Phone"]) or ""

    return {
        "identifier": identifier,
        "name": name,
        "phone": phone,
        "email": identifier,  # to match your existing frontend
        "records": records,
        "monthly": monthly,
        "yearly": yearly,
        "weeks": weeks,
    }


@app.get("/", response_class=HTMLResponse)
def root():
    html = """
    <h2>Ghost Fund Savings Viewer</h2>
    <p>Available endpoints:</p>
    <ul>
      <li><a href="/table">/table</a> - HTML table view</li>
      <li><a href="/summary-table">/summary-table</a> - HTML summary table</li>
      <li><a href="/all-users">/all-users</a> - JSON list of savers for dashboard</li>
      <li>/user/{identifier} - JSON per-saver details</li>
      <li><a href="/how-saved.pdf">/how-saved.pdf</a> - PDF of Name + How Saved</li>
    </ul>
    <p>POST /upload with contact_file + txt_file (multipart/form-data) to process new data.</p>
    """
    return html


@app.post("/upload")
async def upload_files(
    contact_file: UploadFile = File(...),
    txt_file: UploadFile = File(...),
):
    # basic validation
    if not contact_file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=400, detail="Contact file must be an Excel (.xlsx / .xls)"
        )

    # read files into memory
    contact_bytes = await contact_file.read()
    txt_bytes = await txt_file.read()

    # load contact dataframe
    try:
        contact_df = pd.read_excel(BytesIO(contact_bytes))
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to read contact Excel: {str(e)}"
        )

    # decode txt content
    try:
        txt_content = txt_bytes.decode("utf-8")
    except UnicodeDecodeError:
        try:
            txt_content = txt_bytes.decode("utf-8-sig")
        except Exception:
            txt_content = txt_bytes.decode(errors="ignore")

    messages = parse_messages_from_string(txt_content)

    if not messages:
        raise HTTPException(status_code=400, detail="No messages parsed from txt file.")

    df_data, df_summary, new_rows_count = write_excel_from_upload(
        messages, contact_df
    )

    total_amount = float(df_summary["Total_Amount"].sum()) if not df_summary.empty else 0.0
    unique_savers = int(df_summary["Name"].nunique()) if not df_summary.empty else 0

    return {
        "status": "success",
        "new_rows_added": new_rows_count,
        "total_rows_in_data": int(len(df_data)),
        "unique_savers": unique_savers,
        "total_amount": total_amount,
    }


@app.get("/table", response_class=HTMLResponse)
def table_view():
    df_data, _ = load_dataframes_from_excel()
    html_table = df_data.to_html(index=False)
    html_page = f"""
    <html>
    <head>
        <title>Ghost Fund Data</title>
    </head>
    <body>
        <h2>Data (from Excel)</h2>
        {html_table}
        <p><a href="/">Back</a></p>
    </body>
    </html>
    """
    return html_page


@app.get("/summary-table", response_class=HTMLResponse)
def summary_table_view():
    _, df_summary = load_dataframes_from_excel()
    html_table = df_summary.to_html(index=False)
    html_page = f"""
    <html>
    <head>
        <title>Ghost Fund Summary</title>
    </head>
    <body>
        <h2>Summary (from Excel)</h2>
        {html_table}
        <p><a href="/">Back</a></p>
    </body>
    </html>
    """
    return html_page


# ----- NEW: PDF with Name + How Saved only -----

@app.get("/how-saved.pdf")
def how_saved_pdf():
    """
    ✅ Creates a physical PDF file in project folder
    ✅ Shows ONLY howSaved text
    ✅ Skips:
       - Only numbers: "100", "60", "315"
       - Only amount without reason: "BDT 20", "50 tk", "BDT 215"
       - Generic lines: "i saved 30tk", "I saved 20 tk today", "i saved total 80tk"
    ✅ Allows only: amount + clear reason
    ✅ Affects ONLY PDF generation (no effect on Excel / API / dashboard)
    """

    df_data, _ = load_dataframes_from_excel()
    if df_data.empty:
        raise HTTPException(status_code=404, detail="No data available")

    df = df_data.copy()
    df["howSaved"] = df["howSaved"].fillna("").astype(str)

    # ---------------- FILTER LOGIC (only for PDF) ----------------

    def is_valid_howsaved(text: str) -> bool:
        if not text:
            return False

        s = text.strip()
        if not s:
            return False

        # only digits like "100", "60", "315"
        if re.fullmatch(r"\d+", s):
            return False

        # "BDT 20", "bdt20", "50 tk" -> only amount, no reason
        if re.fullmatch(r"(?i)(bdt\s*)?\d+", s):
            return False
        if re.fullmatch(r"(?i)\d+\s*(tk|taka|bdt)", s):
            return False

        # must contain some alphabet (for reason)
        if not re.search(r"[a-zA-Z]", s):
            return False

        # must have a valid amount
        if extract_amount(s) is None:
            return False

        s_lower = s.lower()

        # generic useless patterns
        GENERIC_PATTERNS = [
            r"i saved \d+ ?tk",
            r"i saved \d+ ?taka",
            r"i saved \d+ ?bdt",
            r"i saved \d+ ?tk today",
            r"today i saved \d+ ?tk",
            r"i saved total \d+ ?tk",   # ✅ covers "i saved total 80tk"
        ]
        for pat in GENERIC_PATTERNS:
            if re.fullmatch(pat, s_lower):
                return False

        return True

    # apply filter
    df = df[df["howSaved"].apply(is_valid_howsaved)]

    if df.empty:
        raise HTTPException(status_code=404, detail="No valid reason-based savings found")

    # ---------------- PDF GENERATION ----------------

    file_path = "how_saved.pdf"

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    # Title
    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, height - 50, "Ghost Fund - How Savings Were Made")

    c.setFont("Helvetica", 10)
    c.drawString(50, height - 70, f"Total entries: {len(df)}")

    y = height - 100

    for _, row in df.iterrows():
        how_saved = row["howSaved"].strip()

        if y < 80:
            c.showPage()
            c.setFont("Helvetica", 10)
            y = height - 80

        wrapped_lines = wrap(how_saved, 90)

        for line in wrapped_lines:
            if y < 60:
                c.showPage()
                c.setFont("Helvetica", 10)
                y = height - 80
            c.drawString(50, y, f"- {line}")
            y -= 14

        y -= 10  # space between entries

    c.save()
    buffer.seek(0)

    # also save to disk (optional but you wanted this)
    with open(file_path, "wb") as f:
        f.write(buffer.getbuffer())

    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={
            "Content-Disposition": 'inline; filename="how_saved.pdf"'
        },
    )

# --------------- JSON API FOR DASHBOARD -----------------

@app.get("/all-users")
def all_users():
    """
    Return list of savers with count + total to power the dashboard table.
    """
    df_data, _ = load_dataframes_from_excel()
    if df_data.empty:
        return {"list": []}

    df = df_data.copy()
    df["Phone"] = df["Phone"].fillna("").astype(str)
    df["Name"] = df["Name"].fillna("").astype(str)
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0)

    grouped = df.groupby(["Name", "Phone"])
    out: List[Dict[str, Any]] = []

    for (name, phone), grp in grouped:
        count = int(len(grp))
        total = float(grp["Amount"].sum())
        identifier = phone if phone else name  # used as "email" in frontend

        out.append(
            {
                "name": name or phone or "Unknown",
                "email": identifier,
                "count": count,
                "total": total,
            }
        )

    # sort by total amount (desc)
    out.sort(key=lambda x: x["total"], reverse=True)
    return {"list": out}


@app.get("/user/{identifier}")
def user_details(identifier: str):
    """
    Per-saver details, to be used by /dashboard type page.
    identifier is matched with Phone or Name.
    """
    details = build_user_details(identifier)
    return details


if __name__ == "__main__":
    # In this version Excel is only created when /upload is called
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
