import re
from typing import List

import pandas as pd
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel


# config
TXT_PATH = "ghost_fund.txt"
OUTPUT_XLSX = "ghost_fund_savings.xlsx"



date_prefix = re.compile(
    r"^(\d{1,2}/\d{1,2}/\d{2}),\s+(\d{1,2}:\d{2}\s*[APMapm]{2}) - (.*)$"
)


def parse_messages(txt_path: str):
    messages = []
    current = None

    with open(txt_path, "r", encoding="utf-8") as f:
        for raw in f:
          
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
                text = ""

                if ": " in rest:
                    sender, text = rest.split(": ", 1)
                else:
                    text = rest

                current = {
                    "date": date_str.strip(),
                    "time": time_str.strip().upper(),
                    "sender": sender.strip() if sender else None,
                    "text": text.strip(),
                }
            else:
             
                if current:
                    current["text"] += " " + line.strip()

    if current:
        messages.append(current)

    return messages


def is_saving_message(text: str) -> bool:
    if not text:
        return False
    lower = text.lower()
  
    if re.search(r"\d+", text) and any(k in lower for k in ["bdt", "tk", "saved", "taka"]):
        return True
    
    if re.fullmatch(r"\d+", text.strip()):
        return True
    return False


def extract_amount(text: str):
    nums = [int(n) for n in re.findall(r"\d+", text)]
    if not nums:
        return None
    
    return nums[-1]


def build_dataframes_from_txt(txt_path: str):
    messages = parse_messages(txt_path)

    rows = []

    for msg in messages:
        if not msg["sender"]:
            continue

        text = msg["text"]
        if not is_saving_message(text):
            continue

        amount = extract_amount(text)
        if not amount:
            continue

        rows.append({
            "Date": msg["date"],
            "Time": msg["time"],
            "Name": msg["sender"],
            "Phone": "",   # Phone is not on txt
            "Amount": amount,
            "howSaved": text
        })

    df_data = pd.DataFrame(rows)

    if df_data.empty:
        
        df_data = pd.DataFrame(columns=["Date", "Time", "Name", "Phone", "Amount", "howSaved"])
        df_summary = pd.DataFrame(columns=["Name", "Phone", "Total_Amount"])
        return df_data, df_summary

    # Sorting
    df_data["Date_sort"] = pd.to_datetime(df_data["Date"], format="%m/%d/%y", errors="coerce")
    df_data["Time_sort"] = pd.to_datetime(df_data["Time"], format="%I:%M %p", errors="coerce").dt.time
    df_data = df_data.sort_values(["Date_sort", "Time_sort"], ascending=[False, False])
    df_data = df_data.drop(columns=["Date_sort", "Time_sort"])

    # Summary
    df_summary = (
        df_data.groupby(["Name", "Phone"], as_index=False)["Amount"]
        .sum()
        .rename(columns={"Amount": "Total_Amount"})
    )

    return df_data, df_summary


def write_excel_from_txt():
    df_data, df_summary = build_dataframes_from_txt(TXT_PATH)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        df_data.to_excel(writer, sheet_name="Data", index=False)
        df_summary.to_excel(writer, sheet_name="Summary", index=False)

    print(f"SUCCESS! Excel created: {OUTPUT_XLSX}")


#show data

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


def load_dataframes_from_excel():
    df_data = pd.read_excel(OUTPUT_XLSX, sheet_name="Data")
    df_summary = pd.read_excel(OUTPUT_XLSX, sheet_name="Summary")
    return df_data, df_summary


@app.on_event("startup")
def startup_event():
    # Build Excel from txt on startup
    write_excel_from_txt()


@app.get("/", response_class=HTMLResponse)
def root():
    html = """
    <h2>Ghost Fund Savings Viewer</h2>
    <p>Available endpoints:</p>
    <ul>
      
      <li><a href="/table">/table</a> - HTML table view</li>
      <li><a href="/summary-table">/summary-table</a> - HTML summary table</li>
    </ul>
    """
    return html




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


if __name__ == "__main__":
    write_excel_from_txt()
