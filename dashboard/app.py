import streamlit as st
import plotly.express as px
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


def _secret(section: str, key: str, default=None):
    """Read st.secrets[section][key]; fall back to `default` when there is no
    secrets file (local dev) or the key is missing. Never raises."""
    try:
        return st.secrets[section][key]
    except Exception:
        return default


# ── CONFIG ───────────────────────────────────────────────────────────────────
# Deployed (Streamlit Community Cloud): values come from the app's Secrets.
# Local dev: no secrets file needed; these defaults + your own gcloud login
# (Application Default Credentials) are used, exactly as before.
PROJECT_ID = _secret("bigquery", "project_id", "dbt-tutorial-488323")
DATASET    = _secret("bigquery", "dataset", "dbt_sergio")

# Data only changes when a PR is merged (dbt build on main), so a long cache is
# safe. The cache is shared by ALL visitors: at most one BigQuery refresh per
# TTL window, however many people open the page.
CACHE_TTL_SECONDS = 6 * 60 * 60

# Hard per-query ceiling: BigQuery refuses (and bills nothing for) any query
# from this app that would scan more than this. The real queries scan a few MB.
MAX_BYTES_BILLED = 1 * 1024**3  # 1 GiB
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Solar Analytics",
    page_icon="☀️",
    layout="wide",
)


# ── BIGQUERY CONNECTION ───────────────────────────────────────────────────────

@st.cache_resource
def get_client():
    # Deployed: read-only service-account key stored ONLY in Streamlit secrets.
    try:
        sa_info = dict(st.secrets["gcp_service_account"])
    except Exception:
        sa_info = None
    if sa_info:
        creds = service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        return bigquery.Client(project=PROJECT_ID, credentials=creds)
    # Local dev: Application Default Credentials (gcloud auth / env var).
    return bigquery.Client(project=PROJECT_ID)


def run_query(sql: str) -> pd.DataFrame:
    job_config = bigquery.QueryJobConfig(maximum_bytes_billed=MAX_BYTES_BILLED)
    job = get_client().query(sql, job_config=job_config)
    # Small result sets: plain REST download, so the service account does not
    # need the extra BigQuery Storage "read session" permission.
    return job.to_dataframe(create_bqstorage_client=False)


# ── DATA LOADERS ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_daily() -> pd.DataFrame:
    sql = f"""
        SELECT
            d.system_id,
            d.date,
            m.inclination_deg,
            d.pr_pct,
            d.pr_dc_sp_theta_pct,
            d.h_i_kwh_m2,
            d.e_ac_kwh,
            d.data_availability_pct
        FROM `{PROJECT_ID}.{DATASET}.mart_daily_performance` d
        LEFT JOIN `{PROJECT_ID}.{DATASET}.seed_system_metadata` m
            ON d.system_id = m.system_id
        WHERE d.pr_pct IS NOT NULL
          AND d.pr_pct > 5
          AND d.h_i_kwh_m2 >= 0.5
        ORDER BY d.system_id, d.date
    """
    df = run_query(sql)
    df["date"] = pd.to_datetime(df["date"])
    df["system_id"] = df["system_id"].astype(str)
    return df


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_monthly() -> pd.DataFrame:
    sql = f"""
        SELECT
            d.system_id,
            d.month,
            m.inclination_deg,
            d.pr_pct,
            d.e_ac_kwh,
            d.h_i_kwh_m2,
            d.data_availability_pct,
            d.days_with_data
        FROM `{PROJECT_ID}.{DATASET}.mart_monthly_performance` d
        LEFT JOIN `{PROJECT_ID}.{DATASET}.seed_system_metadata` m
            ON d.system_id = m.system_id
        WHERE d.pr_pct IS NOT NULL
          AND d.pr_pct > 5
        ORDER BY d.system_id, d.month
    """
    df = run_query(sql)
    df["month_date"] = pd.to_datetime(df["month"])
    df["system_id"] = df["system_id"].astype(str)
    return df


# ── SIDEBAR ───────────────────────────────────────────────────────────────────

def render_sidebar(daily: pd.DataFrame):
    st.sidebar.title("☀️ Filters")

    all_systems = sorted(daily["system_id"].unique(), key=int)
    systems = st.sidebar.multiselect("Systems", all_systems, default=all_systems)

    all_incl = sorted(daily["inclination_deg"].unique())
    incl = st.sidebar.multiselect("Inclination (°)", all_incl, default=all_incl)

    min_d, max_d = daily["date"].min().date(), daily["date"].max().date()
    date_range = st.sidebar.date_input(
        "Date range", value=(min_d, max_d), min_value=min_d, max_value=max_d
    )

    return sorted(systems, key=int), sorted(incl), date_range


# ── CHART 1: Daily PR Trends ──────────────────────────────────────────────────

def chart_daily_pr(daily: pd.DataFrame, systems, incl, date_range):
    st.subheader("Daily Performance Ratio")

    df = daily[
        daily["system_id"].isin(systems)
        & daily["inclination_deg"].isin(incl)
        & (daily["date"] >= pd.Timestamp(date_range[0]))
        & (daily["date"] <= pd.Timestamp(date_range[1]))
    ].copy()

    df = df.sort_values(["system_id", "date"])
    df["pr_30d"] = df.groupby("system_id")["pr_pct"].transform(
        lambda s: s.rolling(30, min_periods=5).mean()
    )

    col1, col2 = st.columns([3, 1])
    with col2:
        smooth = st.radio("View", ["30-day average", "Raw daily"], index=0)
    y_col = "pr_30d" if smooth == "30-day average" else "pr_pct"

    fig = px.line(
        df, x="date", y=y_col, color="system_id",
        category_orders={"system_id": [str(i) for i in range(1, 14)]},
        labels={"date": "Date", y_col: "PR (%)", "system_id": "System"},
        template="plotly_white",
    )
    fig.add_hline(y=100, line_dash="dot", line_color="lightgray")
    fig.update_layout(height=460, legend_title="System")
    st.plotly_chart(fig, width="stretch")

    # Key stats row
    stats = df.groupby("system_id")["pr_pct"].agg(["mean", "min", "max"]).round(1)
    stats.columns = ["Mean PR (%)", "Min PR (%)", "Max PR (%)"]
    with st.expander("Summary statistics"):
        st.dataframe(stats, width="stretch")


# ── CHART 2: Monthly Comparison ───────────────────────────────────────────────

def chart_monthly_pr(monthly: pd.DataFrame, systems, incl, date_range):
    st.subheader("Monthly Performance Ratio — System Comparison")

    df = monthly[
        monthly["system_id"].isin(systems)
        & monthly["inclination_deg"].isin(incl)
        & (monthly["month_date"] >= pd.Timestamp(date_range[0]))
        & (monthly["month_date"] <= pd.Timestamp(date_range[1]))
    ].sort_values(["system_id", "month_date"])

    fig = px.line(
        df, x="month_date", y="pr_pct", color="system_id",
        category_orders={"system_id": [str(i) for i in range(1, 14)]},
        labels={"month_date": "Month", "pr_pct": "PR (%)", "system_id": "System"},
        template="plotly_white",
    )
    fig.add_hline(y=100, line_dash="dot", line_color="lightgray")
    fig.update_layout(height=460, legend_title="System")
    st.plotly_chart(fig, width="stretch")


# ── CHART 3: PR vs Irradiance Scatter ────────────────────────────────────────

def chart_pr_vs_irr(daily: pd.DataFrame, systems, incl, date_range):
    st.subheader("PR vs Daily Irradiation")

    df = daily[
        daily["system_id"].isin(systems)
        & daily["inclination_deg"].isin(incl)
        & (daily["date"] >= pd.Timestamp(date_range[0]))
        & (daily["date"] <= pd.Timestamp(date_range[1]))
    ]

    fig = px.scatter(
        df, x="h_i_kwh_m2", y="pr_pct", color="system_id",
        opacity=0.35, trendline="lowess",
        labels={
            "h_i_kwh_m2": "Daily Irradiation (kWh/m²)",
            "pr_pct": "PR (%)",
            "system_id": "System",
        },
        template="plotly_white",
    )
    fig.update_layout(height=460, legend_title="System")
    st.plotly_chart(fig, width="stretch")


# ── CHART 4: Data Quality Heatmap ────────────────────────────────────────────

def chart_data_quality(monthly: pd.DataFrame, systems, incl, date_range):
    st.subheader("Data Availability — System × Month")

    df = monthly[
        monthly["system_id"].isin(systems)
        & monthly["inclination_deg"].isin(incl)
        & (monthly["month_date"] >= pd.Timestamp(date_range[0]))
        & (monthly["month_date"] <= pd.Timestamp(date_range[1]))
    ]

    pivot = df.pivot_table(
        index="system_id",
        columns="month_date",
        values="data_availability_pct",
        aggfunc="mean",
    )
    pivot.index = pivot.index.astype(int)
    pivot = pivot.sort_index()
    pivot.index = pivot.index.astype(str)

    fig = px.imshow(
        pivot,
        color_continuous_scale="RdYlGn",
        zmin=0, zmax=100,
        labels={"x": "Month", "y": "System", "color": "Availability (%)"},
        aspect="auto",
        template="plotly_white",
    )
    fig.update_layout(height=420)
    st.plotly_chart(fig, width="stretch")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    st.title("☀️ Solar Analytics Dashboard")
    st.caption("13 PV systems · 11 years (2013–2023) · IEC 61724 Performance Ratio · Validation accuracy: 0.003%")

    with st.spinner("Loading from BigQuery…"):
        daily   = load_daily()
        monthly = load_monthly()

    systems, incl, date_range = render_sidebar(daily)

    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 Daily PR Trends",
        "📊 Monthly Comparison",
        "🔵 PR vs Irradiance",
        "🟩 Data Quality",
    ])

    with tab1:
        chart_daily_pr(daily, systems, incl, date_range)
    with tab2:
        chart_monthly_pr(monthly, systems, incl, date_range)
    with tab3:
        chart_pr_vs_irr(daily, systems, incl, date_range)
    with tab4:
        chart_data_quality(monthly, systems, incl, date_range)


if __name__ == "__main__":
    main()