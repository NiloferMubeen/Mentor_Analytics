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
        headers = data[0]
        rows = data[1:]
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


# ── ROUTES ──
@app.route("/")
def home():
    df  = load_df()
    ctx = build_context(df)
    return render_template("home.html", **ctx)


# ── updated mentors route: now also loads 2 extra sheets ──
@app.route("/mentors")
def mentors():
    df       = load_df()
    ctx      = build_context(df)

    # Load extra sheets
    doubt_df    = load_doubt_df()
    liveeval_df = load_liveeval_df()

    # Per-mentor doubt session data → safe even if sheet failed to load
    doubt_by_mentor = {}
    for mentor in ctx['mentors']:
        try:
            mdf = doubt_df[doubt_df['Mentor'] == mentor]
            doubt_by_mentor[mentor] = {
                'dates':  mdf['Date'].tolist(),
                'counts': mdf['Count'].tolist(),
            }
        except Exception:
            doubt_by_mentor[mentor] = {'dates': [], 'counts': []}

    # Per-mentor live eval data → safe even if sheet failed to load
    liveeval_by_mentor = {}
    for mentor in ctx['mentors']:
        try:
            mdf    = liveeval_df[liveeval_df['Mentor'] == mentor]
            status = mdf['Zen portal'].value_counts().to_dict()
            liveeval_by_mentor[mentor] = {
                'labels': list(status.keys()),
                'values': [int(v) for v in status.values()],
            }
        except Exception:
            liveeval_by_mentor[mentor] = {'labels': [], 'values': []}

    # Total projects evaluated per mentor (for the bar chart)
    try:
        eval_counts = liveeval_df[liveeval_df['Mentor'] != '']                         .groupby('Mentor').size().sort_values(ascending=False)
        liveeval_mentor_labels = eval_counts.index.tolist()
        liveeval_mentor_values = [int(x) for x in eval_counts.values]
    except Exception:
        liveeval_mentor_labels = []
        liveeval_mentor_values = []

    # Total learners handled per mentor in doubt sessions (sum of Count column)
    try:
        doubt_learners = doubt_df[doubt_df['Mentor'] != '']                             .groupby('Mentor')['Count'].sum().sort_values(ascending=False)
        doubt_learner_labels = doubt_learners.index.tolist()
        doubt_learner_values = [int(x) for x in doubt_learners.values]
    except Exception:
        doubt_learner_labels = []
        doubt_learner_values = []

    ctx['doubt_by_mentor']        = doubt_by_mentor
    ctx['liveeval_by_mentor']     = liveeval_by_mentor
    ctx['liveeval_mentor_labels'] = liveeval_mentor_labels
    ctx['liveeval_mentor_values'] = liveeval_mentor_values
    ctx['doubt_learner_labels']   = doubt_learner_labels
    ctx['doubt_learner_values']   = doubt_learner_values

    return render_template("mentors.html", **ctx)


@app.route("/weekly")
def weekly():
    df  = load_df()
    cutoff = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    df  = df[df['Date'] >= cutoff]
    ctx = build_context(df)
    ctx['period']       = 'Weekly'
    ctx['period_range'] = 'Last 7 Days'
    return render_template("dashboard.html", **ctx)


@app.route("/monthly")
def monthly():
    df  = load_df()
    cutoff = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    df  = df[df['Date'] >= cutoff]
    ctx = build_context(df)
    ctx['period']       = 'Monthly'
    ctx['period_range'] = 'Last 30 Days'
    return render_template("dashboard.html", **ctx)


@app.route("/yearly")
def yearly():
    df  = load_df()
    cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    df  = df[df['Date'] >= cutoff]
    ctx = build_context(df)
    ctx['period']       = 'Yearly'
    ctx['period_range'] = 'Last 12 Months'
    return render_template("dashboard.html", **ctx)


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


if __name__ == "__main__":
    app.run(debug=True)