from flask import Flask, json, render_template
import pandas as pd
import os
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
load_dotenv()

app = Flask(__name__)

SHEET_ID          = "1DMGk4L_WQ0kFrOXHPO-gbkPrFFG02-9I3ZV01AtogLo"
DOUBT_SHEET_ID    = "1NwG9FxQq0Yxugrs2z5RWcG2YMd17Nc2Js4ygJZAllhk"
LIVEEVAL_SHEET_ID = "1e4JrN1pXa-cfr1f1Rshyg0w9VCz5GEkHliIMCDq8eLE"
SESSIONS_SHEET_ID = "113k5JzTSIZknYzLZFnj0lc20Zdcw0svkK3S8R3J8FDI"
TRACKER_SHEET_ID  = "1igZAYA3wT7Yt_oFdkbA459KMAEqa4jJ8J3QC_KOplis"        


# ── helper: authenticate once, reuse ──
def get_client():
    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


# ── load function ──
def load_df():
    client = get_client()
    sheet = client.open_by_key(SHEET_ID)
    worksheet = sheet.sheet1
    data = worksheet.get_all_values()
    headers = data[0]
    rows = data[1:]
    df = pd.DataFrame(rows, columns=headers)
    df.columns = df.columns.str.strip()
    df = df.fillna("")
    df = df[['Date', 'Query No', 'Mentor', 'Status', 'Product',
             'Batch Code', 'Query Type', 'Mail Id', 'Time Taken']]
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

    def convert_time(text):
        if not text:
            return 0
        text = str(text)
        days    = re.search(r'(\d+)\s*day', text)
        hours   = re.search(r'(\d+)\s*hr', text)
        minutes = re.search(r'(\d+)\s*min', text)
        total = 0
        if days:    total += int(days.group(1)) * 1440
        if hours:   total += int(hours.group(1)) * 60
        if minutes: total += int(minutes.group(1))
        return total

    df['time_minutes'] = df['Time Taken'].apply(convert_time)
    return df


# ── NEW: load Project Doubt Session sheet (Image 1: Date, Mentor, Count) ──
# Change the worksheet name below to match your actual sheet tab name
def load_doubt_df():
    try:
        client = get_client()
        sheet = client.open_by_key(DOUBT_SHEET_ID)
        worksheet = sheet.sheet1  # first tab of the doubt session sheet
        data = worksheet.get_all_values()
        headers = data[0]
        rows = data[1:]
        df = pd.DataFrame(rows, columns=headers)
        df.columns = df.columns.str.strip()
        df = df.fillna("")
        # Keep Date, Mentor, Count columns
        df = df[['Date', 'Mentor', 'Count']]
        df['Date']  = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
        df['Count'] = pd.to_numeric(df['Count'], errors='coerce').fillna(0).astype(int)
        return df
    except Exception as e:
        print(f"Doubt sheet load error: {e}")
        return pd.DataFrame(columns=['Date', 'Mentor', 'Count'])


# ── NEW: load Project Live Evaluation sheet (Image 2: Date, Zen portal, Mentor) ──
# Change the worksheet name below to match your actual sheet tab name
def load_liveeval_df():
    try:
        client = get_client()
        sheet = client.open_by_key(LIVEEVAL_SHEET_ID)
        worksheet = sheet.sheet1  # first tab of the live eval sheet
        data = worksheet.get_all_values()
        headers = data[1]
        rows = data[2:]
        df = pd.DataFrame(rows, columns=headers)
        df.columns = df.columns.str.strip()
        df = df.fillna("")
        # Keep only Date, Zen portal, Mentor (as seen in Image 2)
        df = df[['Date', 'Zen portal', 'Mentor']]
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
        return df
    except Exception as e:
        print(f"Live eval sheet load error: {e}")
        return pd.DataFrame(columns=['Date', 'Zen portal', 'Mentor'])


# ── your original build_context, unchanged ──
def build_context(df):
    total_queries  = len(df)
    closed_queries = len(df[df['Status'].str.lower() == 'closed'])
    total_mentors  = df['Mentor'].nunique()
    avg_time = int(df['time_minutes'].mean()) if not df['time_minutes'].isnull().all() else 0

    mentor_counts = df['Mentor'].value_counts().to_dict()
    mentor_labels = list(mentor_counts.keys())
    mentor_values = [int(x) for x in mentor_counts.values()]

    daily_trend = df.groupby('Date').size()
    date_labels = list(daily_trend.index)
    date_values = [int(x) for x in daily_trend.values]

    status_counts = df['Status'].value_counts().to_dict()
    status_labels = list(status_counts.keys())
    status_values = [int(x) for x in status_counts.values()]

    unique_dates  = sorted(df['Date'].astype(str).unique())
    data_records  = df.to_dict(orient='records')
    mentors       = df['Mentor'].dropna().unique().tolist()

    mentor_stats = []
    for m in df['Mentor'].unique():
        if not m:
            continue
        mdf = df[df['Mentor'] == m]
        mentor_stats.append({
            'name':       m,
            'total':      len(mdf),
            'closed':     len(mdf[mdf['Status'].str.lower() == 'closed']),
            'avg_time':   int(mdf['time_minutes'].mean()) if mdf['time_minutes'].sum() > 0 else 0,
            'close_rate': round(len(mdf[mdf['Status'].str.lower() == 'closed']) / len(mdf) * 100, 1) if len(mdf) > 0 else 0,
        })
    mentor_stats.sort(key=lambda x: x['total'], reverse=True)

    return dict(
        records        = data_records,
        mentors        = mentors,
        mentor_stats   = mentor_stats,
        unique_dates   = unique_dates,
        total_queries  = total_queries,
        closed_queries = closed_queries,
        total_mentors  = total_mentors,
        avg_time       = avg_time,
        mentor_labels  = mentor_labels,
        mentor_values  = mentor_values,
        date_labels    = date_labels,
        date_values    = date_values,
        status_labels  = status_labels,
        status_values  = status_values,
    )


# ── load Sessions sheet ──
def load_sessions_df():
    """Sessions sheet — columns: Date, Session Name, Mentor Name, Hosted by."""
    try:
        client    = get_client()
        sheet     = client.open_by_key(SESSIONS_SHEET_ID)
        worksheet = sheet.worksheet("Sessions-2026")
        data      = worksheet.get_all_values()
        headers   = data[1]
        rows      = data[2:]
        df        = pd.DataFrame(rows, columns=headers)
        df.columns = df.columns.str.strip()
        df = df.fillna("")
        df = df[['Date', 'Session Name', 'Mentor Name', 'Hosted by']]
        df['Date']  = pd.to_datetime(df['Date'], errors='coerce')
        df['Month'] = df['Date'].dt.strftime('%B')
        df['Year']  = df['Date'].dt.year.astype(str)
        df['Date']  = df['Date'].dt.strftime('%Y-%m-%d')
        return df
    except Exception as e:
        print(f"Sessions sheet load error: {e}")
        return pd.DataFrame(columns=['Date', 'Session Name', 'Mentor Name', 'Hosted by', 'Month', 'Year'])


# ── ROUTES ──
@app.route("/")
def home():
    df  = load_df()
    ctx = build_context(df)
    return render_template("home.html", **ctx)


# ── DELETED mentors route ──
# The /mentors page has been removed. Mentor data is now shown
# directly in /queries and /projects pages.
# Keeping this comment as a placeholder so nothing breaks.
# mentors() route removed — data lives in /queries and /projects





# ── NEW: Queries page (query-focused analytics) ──
@app.route("/queries")
def queries():
    df  = load_df()
    ctx = build_context(df)
    return render_template("queries.html", **ctx)


# ── NEW: Projects page (doubt sessions + live eval) ──
@app.route("/projects")
def projects():
    df          = load_df()
    ctx         = build_context(df)
    doubt_df    = load_doubt_df()
    liveeval_df = load_liveeval_df()

    # Raw records for JS-side filtering
    ctx['doubt_records']   = doubt_df.to_dict(orient='records')
    ctx['liveeval_records'] = liveeval_df.to_dict(orient='records')

    # All unique dates for the date filter dropdown (union of both sheets)
    all_doubt_dates = sorted(doubt_df['Date'].dropna().unique().tolist())
    ctx['all_doubt_dates'] = all_doubt_dates

    # Summary KPIs
    ctx['doubt_total_sessions'] = len(doubt_df)
    ctx['doubt_total_learners'] = int(doubt_df['Count'].sum()) if not doubt_df.empty else 0
    ctx['liveeval_total']       = len(liveeval_df)

    # Keep existing per-mentor dicts for backward compat
    doubt_by_mentor = {}
    liveeval_by_mentor = {}
    for mentor in ctx['mentors']:
        try:
            mdf = doubt_df[doubt_df['Mentor'] == mentor]
            doubt_by_mentor[mentor] = {
                'dates':  mdf['Date'].tolist(),
                'counts': mdf['Count'].tolist(),
            }
        except Exception:
            doubt_by_mentor[mentor] = {'dates': [], 'counts': []}
        try:
            mdf    = liveeval_df[liveeval_df['Mentor'] == mentor]
            status = mdf['Zen portal'].value_counts().to_dict()
            liveeval_by_mentor[mentor] = {
                'labels': list(status.keys()),
                'values': [int(v) for v in status.values()],
            }
        except Exception:
            liveeval_by_mentor[mentor] = {'labels': [], 'values': []}

    ctx['doubt_by_mentor']    = doubt_by_mentor
    ctx['liveeval_by_mentor'] = liveeval_by_mentor

    return render_template("projects.html", **ctx)



# ── NEW: Sessions page ──
@app.route("/sessions")
def sessions():
    df  = load_df()
    ctx = build_context(df)
    sessions_df = load_sessions_df()

    ctx['sessions_records']  = sessions_df.to_dict(orient='records')
    ctx['session_names']     = sorted(sessions_df['Session Name'].dropna().unique().tolist())
    ctx['session_mentors']   = sorted(sessions_df['Mentor Name'].dropna().unique().tolist())
    ctx['session_hosts']     = sorted(sessions_df['Hosted by'].dropna().unique().tolist())
    ctx['session_months']    = sorted(sessions_df['Month'].dropna().unique().tolist())
    ctx['session_years']     = sorted(sessions_df['Year'].dropna().unique().tolist())
    ctx['sessions_total']    = len(sessions_df)
    ctx['sessions_mentors_count'] = sessions_df['Mentor Name'].nunique()
    ctx['sessions_types_count']   = sessions_df['Session Name'].nunique()
    ctx['sessions_hosts_count']   = sessions_df['Hosted by'].nunique()

    return render_template("sessions.html", **ctx)

if __name__ == "__main__":
    app.run(debug=True)

# ════════════════════════════════════════════════════════════
#  TRACKER CONFIG — fill in your real values
# ════════════════════════════════════════════════════════════
#TRACKER_SHEET_ID = "YOUR_TRACKER_SHEET_ID_HERE"

MANAGER_EMAIL = "manager@example.com"

MENTOR_EMAILS = {
    "Shadiya":   "shadiya@example.com",
    "Nehlath":   "nehlath@example.com",
    "Gomathi":   "gomathi@example.com",
    "Aravinth":  "aravinth@example.com",
    "Asvin":     "asvin@example.com",
    # add remaining mentors here
}

SMTP_HOST     = "smtp.gmail.com"
SMTP_PORT     = 587
SMTP_USER     = "your@gmail.com"
SMTP_PASSWORD = "your-app-password"
SMTP_FROM     = "your@gmail.com"


# ── email helper ──
def send_email(to_list, subject, body):
    """Send a plain-text email. to_list is a list of address strings."""
    import smtplib, ssl
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    try:
        msg = MIMEMultipart()
        msg["From"]    = SMTP_FROM
        msg["To"]      = ", ".join(to_list)
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))
        ctx = ssl.create_default_context()
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls(context=ctx)
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_FROM, to_list, msg.as_string())
        print(f"[EMAIL SENT] To: {to_list} | Subject: {subject}")
        return True
    except Exception as e:
        print(f"[EMAIL ERROR] {e}")
        return False


# ── load tracker sheet (both tabs combined) ──
def load_tracker_df():
    try:
        client = get_client()
        sheet  = client.open_by_key(TRACKER_SHEET_ID)
        frames = []
        for tab_name in ["Data Science", "AI ML"]:
            try:
                ws   = sheet.worksheet(tab_name)
                data = ws.get_all_values()
                if len(data) < 2:
                    continue
                headers = [h.strip() for h in data[0]]
                rows    = data[1:]
                df_tab  = pd.DataFrame(rows, columns=headers)
                df_tab["Sheet"] = tab_name
                frames.append(df_tab)
            except Exception as e:
                print(f"Tracker tab '{tab_name}' error: {e}")
        if not frames:
            return pd.DataFrame()
        df = pd.concat(frames, ignore_index=True)
        df.columns = df.columns.str.strip()
        df = df.fillna("")
        # keep only needed columns (robust to missing)
        needed = ["S.No","Batch","Mode","Language","Project Number",
                  "Project Title","Assigned Date","Deadline Date",
                  "Assigned by","Mentor","Sheet"]
        df = df[[c for c in needed if c in df.columns]]
        df["Assigned Date"] = pd.to_datetime(df["Assigned Date"], dayfirst=True, errors="coerce")
        df["Deadline Date"] = pd.to_datetime(df["Deadline Date"], dayfirst=True, errors="coerce")
        # normalise Mode → Weekend / Weekday
        def norm_mode(m):
            m2 = str(m).strip().upper()
            if m2.startswith("WE") or m2 == "WEEKEND":
                return "Weekend"
            return "Weekday"
        df["Mode_Simple"] = df["Mode"].apply(norm_mode)
        return df
    except Exception as e:
        print(f"Tracker load error: {e}")
        return pd.DataFrame()


# ── alert logic ──
_alert_log = []   # in-memory log: [{type, batch, mentor, detail, sent_at}]

def run_tracker_alerts():
    """Called on startup and every 24 h by scheduler."""
    global _alert_log
    df = load_tracker_df()
    if df.empty:
        return
    today = pd.Timestamp(datetime.now().date())

    # ── 1. Deadline alerts ──
    deadline_rows = df[df["Deadline Date"].dt.normalize() == today]
    for _, row in deadline_rows.iterrows():
        mentor = row.get("Mentor","").strip()
        batch  = row.get("Batch","")
        proj   = row.get("Project Title","")
        email  = MENTOR_EMAILS.get(mentor)
        subject = f"[MentorHub] Deadline Today: {batch} – {proj}"
        body = (
            f"Hi {mentor},\n\n"
            f"This is an automated reminder from MentorHub.\n\n"
            f"The deadline for the following project is TODAY:\n"
            f"  Batch        : {batch}\n"
            f"  Project Title: {proj}\n"
            f"  Deadline Date: {row['Deadline Date'].strftime('%d/%m/%Y')}\n\n"
            f"Please ensure everything is in order.\n\nRegards,\nMentorHub"
        )
        sent = False
        if email:
            sent = send_email([email], subject, body)
        _alert_log.append({
            "type":    "Deadline",
            "sheet":   row.get("Sheet",""),
            "batch":   batch,
            "mentor":  mentor,
            "detail":  f"{proj} — deadline today",
            "email_to": email or "NOT CONFIGURED",
            "sent":    sent,
            "sent_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        })

    # ── 2. Final-project out-of-order alerts ──
    # Group by Sheet + Batch; check if Final assigned before any numbered project
    for (sheet_name, batch), grp in df.groupby(["Sheet","Batch"]):
        grp = grp.dropna(subset=["Assigned Date"])
        final_rows = grp[grp["Project Title"].str.strip().str.lower() == "final"]
        other_rows = grp[grp["Project Title"].str.strip().str.lower() != "final"]
        if final_rows.empty or other_rows.empty:
            continue
        final_date = final_rows["Assigned Date"].min()
        # alert if Final was assigned before the latest numbered project
        if final_date < other_rows["Assigned Date"].max():
            mentor = grp["Mentor"].iloc[0].strip()
            m_email  = MENTOR_EMAILS.get(mentor)
            to_list  = [e for e in [m_email, MANAGER_EMAIL] if e]
            subject  = f"[MentorHub] ⚠️ Final Project Assigned Out of Order: {batch}"
            body = (
                f"Hi,\n\n"
                f"This is an automated alert from MentorHub.\n\n"
                f"The FINAL project for batch '{batch}' (Sheet: {sheet_name}) "
                f"appears to have been assigned BEFORE some preceding projects.\n\n"
                f"  Batch        : {batch}\n"
                f"  Mentor       : {mentor}\n"
                f"  Final Assigned: {final_date.strftime('%d/%m/%Y')}\n\n"
                f"Please review and correct the assignment order.\n\nRegards,\nMentorHub"
            )
            sent = False
            if to_list:
                sent = send_email(to_list, subject, body)
            _alert_log.append({
                "type":    "Out-of-Order Final",
                "sheet":   sheet_name,
                "batch":   batch,
                "mentor":  mentor,
                "detail":  f"Final assigned {final_date.strftime('%d/%m/%Y')} before other projects",
                "email_to": ", ".join(to_list) if to_list else "NOT CONFIGURED",
                "sent":    sent,
                "sent_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            })


# ── APScheduler (24-hour repeat) ──
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    _scheduler = BackgroundScheduler()
    _scheduler.add_job(run_tracker_alerts, "interval", hours=24, id="tracker_alerts")
    _scheduler.start()
    print("[Scheduler] Tracker alert job started (every 24 h).")
except ImportError:
    print("[Scheduler] APScheduler not installed — alerts will run on route visit only.")


# ── build tracker context ──
def build_tracker_ctx(df):
    if df.empty:
        return {"tracker_empty": True, "tracker_records": []}

    records = []
    for _, r in df.iterrows():
        rec = {}
        for col in df.columns:
            val = r[col]
            if pd.isna(val):
                rec[col] = ""
            elif isinstance(val, pd.Timestamp):
                rec[col] = val.strftime("%Y-%m-%d")
            else:
                rec[col] = str(val)
        records.append(rec)

    mentors   = sorted(df["Mentor"].dropna().unique().tolist())
    languages = sorted(df["Language"].dropna().unique().tolist())
    sheets    = sorted(df["Sheet"].dropna().unique().tolist())

    # chart 1 — project title vs no of batches
    title_batch = (
        df[df["Project Title"].str.strip().str.lower() != "final"]
        .groupby("Project Title")["Batch"].nunique()
        .sort_values(ascending=False)
        .head(20)
    )
    # chart 2 — unique project titles per mentor
    mentor_proj = (
        df[df["Project Title"].str.strip().str.lower() != "final"]
        .groupby("Mentor")["Project Title"].nunique()
        .sort_values(ascending=False)
    )
    # chart 3 — batches per mentor × language (stacked)
    lang_pivot = (
        df.groupby(["Mentor","Language"])["Batch"]
        .nunique().reset_index(name="count")
    )
    # chart 4 — weekend vs weekday
    mode_counts = df["Mode_Simple"].value_counts().to_dict()
    # chart 5 — assigned by
    assigned_counts = df["Assigned by"].value_counts().to_dict()
    # chart 6 — batches per mentor × mode (stacked)
    mode_pivot = (
        df.groupby(["Mentor","Mode_Simple"])["Batch"]
        .nunique().reset_index(name="count")
    )
    # chart 7 — batch completion
    total_per_batch   = df.groupby(["Sheet","Batch"])["Project Number"].count()
    assigned_per_batch = df.groupby(["Sheet","Batch"])["Assigned Date"].apply(
        lambda s: s.notna().sum()
    )
    completion = pd.DataFrame({"total": total_per_batch, "assigned": assigned_per_batch}).reset_index()
    completion["label"] = completion["Sheet"].str[:2] + "·" + completion["Batch"]

    # deadline alerts preview
    today = pd.Timestamp(datetime.now().date())
    deadline_today = df[df["Deadline Date"].dt.normalize() == today]

    # out-of-order final preview
    ooo_batches = []
    for (sn, batch), grp in df.groupby(["Sheet","Batch"]):
        grp2 = grp.dropna(subset=["Assigned Date"])
        finals = grp2[grp2["Project Title"].str.strip().str.lower() == "final"]
        others = grp2[grp2["Project Title"].str.strip().str.lower() != "final"]
        if finals.empty or others.empty:
            continue
        if finals["Assigned Date"].min() < others["Assigned Date"].max():
            ooo_batches.append({
                "sheet": sn, "batch": batch,
                "mentor": grp2["Mentor"].iloc[0],
                "final_date": finals["Assigned Date"].min().strftime("%d/%m/%Y"),
            })

    return dict(
        tracker_empty        = False,
        tracker_records      = records,
        mentors              = mentors,
        languages            = languages,
        sheets               = sheets,
        total_batches        = int(df["Batch"].nunique()),
        total_projects       = len(df),
        total_mentors        = int(df["Mentor"].nunique()),
        total_langs          = int(df["Language"].nunique()),
        # chart 1
        title_labels         = title_batch.index.tolist(),
        title_values         = [int(x) for x in title_batch.values],
        # chart 2
        mentor_proj_labels   = mentor_proj.index.tolist(),
        mentor_proj_values   = [int(x) for x in mentor_proj.values],
        # chart 3 — lang stacked
        lang_pivot           = lang_pivot.to_dict(orient="records"),
        lang_list            = sorted(df["Language"].dropna().unique().tolist()),
        # chart 4
        mode_labels          = list(mode_counts.keys()),
        mode_values          = [int(v) for v in mode_counts.values()],
        # chart 5
        assigned_labels      = list(assigned_counts.keys()),
        assigned_values      = [int(v) for v in assigned_counts.values()],
        # chart 6 — mode stacked
        mode_pivot           = mode_pivot.to_dict(orient="records"),
        mode_list            = ["Weekend","Weekday"],
        # chart 7
        completion_labels    = completion["label"].tolist(),
        completion_total     = [int(x) for x in completion["total"].tolist()],
        completion_assigned  = [int(x) for x in completion["assigned"].tolist()],
        # alerts
        deadline_today       = deadline_today[["Sheet","Batch","Project Title","Mentor","Deadline Date"]].to_dict(orient="records") if not deadline_today.empty else [],
        ooo_batches          = ooo_batches,
    )


# ── ROUTES ──
@app.route("/tracker")
def tracker():
    df  = load_tracker_df()
    ctx = build_tracker_ctx(df)
    run_tracker_alerts()   # also fire alerts on page load
    return render_template("tracker.html", **ctx)


@app.route("/tracker-alerts")
def tracker_alerts():
    df  = load_tracker_df()
    ctx = build_tracker_ctx(df)
    ctx["alert_log"] = list(reversed(_alert_log))   # newest first
    return render_template("tracker_alerts.html", **ctx)

