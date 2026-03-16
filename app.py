"""
DataPulse -- Dash Analytics Dashboard
======================================
Run  : python app.py
Open : http://127.0.0.1:8050

Author: Shubham Naralkar
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
import dash
from dash import dcc, html, Input, Output, dash_table
import plotly.graph_objects as go
import plotly.express as px

# ── ENV ───────────────────────────────────────────────────
load_dotenv()

DB_CONFIG = {
    "host"    : os.getenv("DB_HOST",     "localhost"),
    "port"    : int(os.getenv("DB_PORT", "3306")),
    "user"    : os.getenv("DB_USER",     "root"),
    "password": os.getenv("DB_PASSWORD", "your_password"),
    "database": os.getenv("DB_NAME",     "datapulse"),
}

# ── DATA LOADER (MySQL) ───────────────────────────────────
def load_data() -> pd.DataFrame:
    url = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        f"?charset=utf8mb4"
    )
    engine = create_engine(url, echo=False, pool_pre_ping=True,
                           connect_args={"connect_timeout": 10})
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM sales", conn)
    engine.dispose()
    df["date"] = pd.to_datetime(df["date"])
    return df

try:
    df = load_data()
except Exception as e:
    print(f"DB connection failed: {e}")
    print("Loading fallback CSV data...")
    df = pd.read_csv("sales_data.csv", parse_dates=["date"])
    df["year"]             = df["date"].dt.year
    df["month"]            = df["date"].dt.month
    df["quarter"]          = df["date"].dt.quarter
    df["month_name"]       = df["date"].dt.strftime("%b")
    df["profit_margin_pct"]= (df["profit"] / df["sales"] * 100).round(2)
    df["year_month"]       = df["date"].dt.to_period("M").astype(str)
    mu, sigma = df["sales"].mean(), df["sales"].std()
    df["is_outlier"] = (df["sales"] > mu + 3 * sigma).astype(int)
    df["profit_tier"]      = "Medium"
    df["customer_segment"] = "Returning"

ALL_YEARS    = sorted(df["year"].unique())
ALL_CATS     = sorted(df["category"].unique())
ALL_REGIONS  = sorted(df["region"].unique())
ALL_PRODUCTS = sorted(df["product"].unique())
ALL_QUARTERS = sorted(df["quarter"].unique())
DATE_MIN     = df["date"].min().date()
DATE_MAX     = df["date"].max().date()

# ── COLOURS ───────────────────────────────────────────────
C = {
    "bg"     : "#0D1117", "card"   : "#161B22", "border" : "#30363D",
    "a1"     : "#58A6FF", "a2"     : "#3FB950", "a3"     : "#F78166",
    "a4"     : "#D2A8FF", "a5"     : "#FFA657", "text"   : "#E6EDF3",
    "sub"    : "#8B949E",
}
CAT_CLR  = [C["a1"], C["a2"], C["a3"], C["a4"], C["a5"]]
TIER_CLR = {"High": C["a2"], "Medium": C["a1"], "Low": C["a5"], "Loss": C["a3"]}
SEG_CLR  = {"Loyal": C["a2"], "Returning": C["a1"], "One-time": C["a5"]}

BASE_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=C["text"], family="'DM Sans',sans-serif"),
    xaxis=dict(gridcolor=C["border"], zerolinecolor=C["border"]),
    yaxis=dict(gridcolor=C["border"], zerolinecolor=C["border"]),
    margin=dict(l=40, r=20, t=40, b=40),
)

F_STYLE  = {"background": C["card"], "color": C["text"], "border": f"1px solid {C['border']}"}
LBL_STYLE = {"color": C["sub"], "fontSize": "11px", "marginBottom": "4px",
              "display": "block", "textTransform": "uppercase", "letterSpacing": "0.8px"}

# ── HELPERS ───────────────────────────────────────────────
def fmt(val):
    if val >= 1_00_00_000: return f"Rs {val/1_00_00_000:.1f}Cr"
    if val >= 1_00_000:    return f"Rs {val/1_00_000:.1f}L"
    return f"Rs {val:,.0f}"

def kpi(title, value, sub="", color=C["a1"]):
    return html.Div([
        html.P(title, style={"margin": "0", "fontSize": "11px", "color": C["sub"],
                              "letterSpacing": "1px", "textTransform": "uppercase"}),
        html.H2(value, style={"margin": "4px 0 2px", "fontSize": "24px",
                               "color": color, "fontWeight": "700"}),
        html.Span(sub, style={"fontSize": "11px", "color": C["sub"]}),
    ], style={"background": C["card"], "border": f"1px solid {C['border']}",
              "borderTop": f"3px solid {color}", "borderRadius": "8px",
              "padding": "16px 18px", "flex": "1", "minWidth": "150px"})

def card(title, children, flex="1"):
    return html.Div([
        html.H3(title, style={"color": C["sub"], "fontSize": "11px", "textTransform": "uppercase",
                               "letterSpacing": "1px", "margin": "0 0 12px"}),
        children,
    ], style={"flex": flex, "background": C["card"], "border": f"1px solid {C['border']}",
              "borderRadius": "8px", "padding": "18px"})

def ef():
    return go.Figure(layout=BASE_LAYOUT)

# ── APP ───────────────────────────────────────────────────
app  = dash.Dash(__name__, title="DataPulse")
server = app.server   # Required for gunicorn / Render

app.layout = html.Div([
    html.Link(rel="stylesheet",
              href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&family=Space+Mono&display=swap"),

    # HEADER
    html.Div([
        html.Div([
            html.Span("DataPulse", style={"fontSize": "20px", "fontWeight": "700", "color": C["a1"]}),
            html.Span(" / Sales Intelligence", style={"color": C["sub"], "fontSize": "13px", "marginLeft": "6px"}),
        ]),
        html.Span(f"MySQL  |  {datetime.now().strftime('%d %b %Y, %H:%M')}",
                  style={"color": C["sub"], "fontSize": "11px", "fontFamily": "Space Mono"}),
    ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
              "padding": "14px 28px", "borderBottom": f"1px solid {C['border']}", "background": C["card"]}),

    # FILTERS
    html.Div([
        html.Div([html.Label("Year", style=LBL_STYLE),
                  dcc.Dropdown(id="f-year",
                               options=[{"label": "All Years", "value": "all"}] +
                                       [{"label": str(y), "value": y} for y in ALL_YEARS],
                               value="all", clearable=False, style=F_STYLE)],
                 style={"width": "130px"}),

        html.Div([html.Label("Quarter", style=LBL_STYLE),
                  dcc.Dropdown(id="f-quarter",
                               options=[{"label": "All Quarters", "value": "all"}] +
                                       [{"label": f"Q{q}", "value": q} for q in ALL_QUARTERS],
                               value="all", clearable=False, style=F_STYLE)],
                 style={"width": "140px"}),

        html.Div([html.Label("Category", style=LBL_STYLE),
                  dcc.Dropdown(id="f-cat",
                               options=[{"label": "All Categories", "value": "all"}] +
                                       [{"label": c, "value": c} for c in ALL_CATS],
                               value="all", clearable=False, style=F_STYLE)],
                 style={"width": "175px"}),

        html.Div([html.Label("Product", style=LBL_STYLE),
                  dcc.Dropdown(id="f-product",
                               options=[{"label": "All Products", "value": "all"}] +
                                       [{"label": p, "value": p} for p in ALL_PRODUCTS],
                               value="all", clearable=False, style=F_STYLE)],
                 style={"width": "195px"}),

        html.Div([html.Label("Region", style=LBL_STYLE),
                  dcc.Dropdown(id="f-region",
                               options=[{"label": "All Regions", "value": "all"}] +
                                       [{"label": r, "value": r} for r in ALL_REGIONS],
                               value="all", clearable=False, style=F_STYLE)],
                 style={"width": "145px"}),

        html.Div([html.Label("Date Range", style=LBL_STYLE),
                  dcc.DatePickerRange(id="f-date", min_date_allowed=DATE_MIN,
                                      max_date_allowed=DATE_MAX,
                                      start_date=DATE_MIN, end_date=DATE_MAX,
                                      display_format="DD MMM YYYY",
                                      style={"fontSize": "12px"})]),
    ], style={"display": "flex", "gap": "14px", "padding": "14px 28px",
              "background": C["bg"], "alignItems": "flex-end", "flexWrap": "wrap",
              "borderBottom": f"1px solid {C['border']}"}),

    # MAIN
    html.Div([
        # KPIs
        html.Div(id="kpis", style={"display": "flex", "gap": "12px",
                                    "marginBottom": "14px", "flexWrap": "wrap"}),
        # Row 1
        html.Div([
            card("Monthly Sales & Profit Trend",
                 dcc.Graph(id="ch-monthly", config={"displayModeBar": False}, style={"height": "260px"}),
                 flex="2"),
            card("Revenue by Category",
                 dcc.Graph(id="ch-pie", config={"displayModeBar": False}, style={"height": "260px"}),
                 flex="1"),
        ], style={"display": "flex", "gap": "12px", "marginBottom": "12px"}),
        # Row 2
        html.Div([
            card("3-Month Rolling Average",
                 dcc.Graph(id="ch-rolling", config={"displayModeBar": False}, style={"height": "240px"})),
            card("MoM Growth %",
                 dcc.Graph(id="ch-growth", config={"displayModeBar": False}, style={"height": "240px"})),
        ], style={"display": "flex", "gap": "12px", "marginBottom": "12px"}),
        # Row 3
        html.Div([
            card("Region Performance",
                 dcc.Graph(id="ch-region", config={"displayModeBar": False}, style={"height": "250px"})),
            card("Top 10 Products by Revenue",
                 dcc.Graph(id="ch-products", config={"displayModeBar": False}, style={"height": "250px"}),
                 flex="1.4"),
        ], style={"display": "flex", "gap": "12px", "marginBottom": "12px"}),
        # Row 4
        html.Div([
            card("Profit Tier Breakdown",
                 dcc.Graph(id="ch-tier", config={"displayModeBar": False}, style={"height": "240px"})),
            card("Customer Segmentation",
                 dcc.Graph(id="ch-seg", config={"displayModeBar": False}, style={"height": "240px"})),
            card("Discount Impact on Margin",
                 dcc.Graph(id="ch-disc", config={"displayModeBar": False}, style={"height": "240px"})),
        ], style={"display": "flex", "gap": "12px", "marginBottom": "12px"}),
        # Row 5
        card("Recent Transactions", html.Div(id="tbl")),
    ], style={"padding": "20px 28px", "background": C["bg"], "minHeight": "100vh"}),

], style={"fontFamily": "'DM Sans',sans-serif", "background": C["bg"], "color": C["text"], "margin": "0"})


# ── FILTER HELPER ─────────────────────────────────────────
def filt(year, quarter, cat, product, region, sd, ed):
    d = df.copy()
    if year    != "all": d = d[d["year"]     == int(year)]
    if quarter != "all": d = d[d["quarter"]  == int(quarter)]
    if cat     != "all": d = d[d["category"] == cat]
    if product != "all": d = d[d["product"]  == product]
    if region  != "all": d = d[d["region"]   == region]
    if sd and ed:
        d = d[(d["date"] >= pd.to_datetime(sd)) & (d["date"] <= pd.to_datetime(ed))]
    return d


# ── CALLBACK ──────────────────────────────────────────────
@app.callback(
    Output("kpis",       "children"),
    Output("ch-monthly", "figure"),
    Output("ch-pie",     "figure"),
    Output("ch-rolling", "figure"),
    Output("ch-growth",  "figure"),
    Output("ch-region",  "figure"),
    Output("ch-products","figure"),
    Output("ch-tier",    "figure"),
    Output("ch-seg",     "figure"),
    Output("ch-disc",    "figure"),
    Output("tbl",        "children"),
    Input("f-year",    "value"),
    Input("f-quarter", "value"),
    Input("f-cat",     "value"),
    Input("f-product", "value"),
    Input("f-region",  "value"),
    Input("f-date",    "start_date"),
    Input("f-date",    "end_date"),
)
def update(year, quarter, cat, product, region, sd, ed):
    d = filt(year, quarter, cat, product, region, sd, ed)
    if d.empty:
        empty = ef()
        return [], empty, empty, empty, empty, empty, empty, empty, empty, empty, []

    tot_sales  = d["sales"].sum()
    tot_profit = d["profit"].sum()
    avg_margin = d["profit_margin_pct"].mean()
    tot_orders = len(d)
    aov        = tot_sales / tot_orders if tot_orders else 0

    # KPIs
    kpis = [
        kpi("Total Revenue",    fmt(tot_sales),       f"{tot_orders:,} orders",      C["a1"]),
        kpi("Total Profit",     fmt(tot_profit),      f"{avg_margin:.1f}% margin",   C["a2"]),
        kpi("Avg Order Value",  fmt(aov),             "per transaction",             C["a4"]),
        kpi("Unique Customers", f"{d['customer_id'].nunique():,}", "active buyers",  C["a5"]),
        kpi("Outlier Orders",   f"{d['is_outlier'].sum():,}",      "high-val spikes",C["a3"]),
    ]

    # Monthly Trend
    mon = d.groupby("year_month", as_index=False).agg(
        sales=("sales","sum"), profit=("profit","sum")).sort_values("year_month")
    fig_mon = go.Figure([
        go.Bar(x=mon["year_month"], y=mon["sales"], name="Sales",
               marker_color=C["a1"], opacity=0.8),
        go.Scatter(x=mon["year_month"], y=mon["profit"], name="Profit",
                   mode="lines+markers", line=dict(color=C["a2"], width=2), marker=dict(size=4)),
    ])
    fig_mon.update_layout(**BASE_LAYOUT, bargap=0.3,
                           legend=dict(orientation="h", y=1.15, font=dict(size=10)))

    # Category Donut
    cat_df = d.groupby("category", as_index=False)["sales"].sum()
    fig_pie = go.Figure(go.Pie(labels=cat_df["category"], values=cat_df["sales"],
                                hole=0.52, marker_colors=CAT_CLR,
                                textinfo="percent+label", textfont_size=10))
    fig_pie.update_layout(**BASE_LAYOUT, showlegend=False)

    # Rolling 3M
    roll = d.groupby("year_month", as_index=False)["sales"].sum().sort_values("year_month")
    roll["r3m"] = roll["sales"].rolling(3, min_periods=1).mean()
    fig_roll = go.Figure([
        go.Bar(x=roll["year_month"], y=roll["sales"], name="Monthly",
               marker_color=C["a1"], opacity=0.45),
        go.Scatter(x=roll["year_month"], y=roll["r3m"], name="3M Avg",
                   mode="lines", line=dict(color=C["a5"], width=2.5, dash="dot")),
    ])
    fig_roll.update_layout(**BASE_LAYOUT, bargap=0.3,
                            legend=dict(orientation="h", y=1.15, font=dict(size=10)))

    # MoM Growth
    grw = d.groupby("year_month", as_index=False)["sales"].sum().sort_values("year_month")
    grw["mom"] = grw["sales"].pct_change() * 100
    grw = grw.dropna()
    fig_grw = go.Figure(go.Bar(
        x=grw["year_month"], y=grw["mom"].round(1),
        marker_color=[C["a2"] if v >= 0 else C["a3"] for v in grw["mom"]],
        text=grw["mom"].round(1).astype(str) + "%", textposition="outside",
        textfont=dict(size=9),
    ))
    fig_grw.add_hline(y=0, line_color=C["border"], line_width=1)
    fig_grw.update_layout(**BASE_LAYOUT, showlegend=False, yaxis_title="MoM %", bargap=0.3)

    # Region
    reg = d.groupby("region", as_index=False).agg(
        sales=("sales","sum"), profit=("profit","sum")).sort_values("sales", ascending=True)
    fig_reg = go.Figure([
        go.Bar(y=reg["region"], x=reg["sales"], name="Sales", orientation="h",
               marker_color=C["a1"], opacity=0.85),
        go.Bar(y=reg["region"], x=reg["profit"], name="Profit", orientation="h",
               marker_color=C["a2"], opacity=0.85),
    ])
    fig_reg.update_layout(**BASE_LAYOUT, barmode="group",
                           legend=dict(orientation="h", y=1.1, font=dict(size=10)))

    # Top Products
    top = d.groupby("product", as_index=False)["sales"].sum().nlargest(10, "sales").sort_values("sales")
    fig_top = go.Figure(go.Bar(
        y=top["product"], x=top["sales"], orientation="h",
        marker=dict(color=top["sales"], colorscale=[[0, C["a1"]], [1, C["a4"]]]),
        text=top["sales"].apply(fmt), textposition="outside",
        textfont=dict(size=9, color=C["text"]),
    ))
    fig_top.update_layout(**BASE_LAYOUT, showlegend=False)

    # Profit Tier
    tier = d.groupby("profit_tier", as_index=False).agg(orders=("order_id","count"))
    tier["profit_tier"] = pd.Categorical(tier["profit_tier"],
                                          categories=["Loss","Low","Medium","High"], ordered=True)
    tier = tier.sort_values("profit_tier")
    fig_tier = go.Figure([
        go.Bar(x=[str(r["profit_tier"])], y=[r["orders"]],
               name=str(r["profit_tier"]),
               marker_color=TIER_CLR.get(str(r["profit_tier"]), C["a1"]),
               text=[f"{r['orders']} orders"], textposition="outside",
               textfont=dict(size=10))
        for _, r in tier.iterrows()
    ])
    fig_tier.update_layout(**BASE_LAYOUT, showlegend=False, yaxis_title="Orders", bargap=0.35)

    # Customer Segmentation (dual axis — custom layout, no BASE_LAYOUT spread)
    seg = d.groupby("customer_segment", as_index=False).agg(
        customers=("customer_id","nunique"), revenue=("sales","sum"))
    seg["customer_segment"] = pd.Categorical(
        seg["customer_segment"], categories=["One-time","Returning","Loyal"], ordered=True)
    seg = seg.sort_values("customer_segment")

    fig_seg = go.Figure([
        go.Bar(x=seg["customer_segment"], y=seg["customers"],
               marker_color=[SEG_CLR.get(s, C["a1"]) for s in seg["customer_segment"]],
               text=seg["customers"], textposition="outside", textfont=dict(size=10),
               name="Customers", yaxis="y"),
        go.Scatter(x=seg["customer_segment"], y=seg["revenue"],
                   mode="lines+markers", line=dict(color=C["a4"], width=2),
                   marker=dict(size=8), name="Revenue", yaxis="y2"),
    ])
    fig_seg.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=C["text"], family="'DM Sans',sans-serif"),
        margin=dict(l=40, r=50, t=40, b=40),
        xaxis=dict(gridcolor=C["border"], zerolinecolor=C["border"]),
        yaxis=dict(title="Customers", gridcolor=C["border"], zerolinecolor=C["border"]),
        yaxis2=dict(title="Revenue", overlaying="y", side="right",
                    gridcolor="rgba(0,0,0,0)", zerolinecolor="rgba(0,0,0,0)"),
        legend=dict(orientation="h", y=1.15, font=dict(size=10)),
        bargap=0.35,
    )

    # Discount Impact
    dc = d.copy()
    dc["disc_band"] = pd.cut(dc["discount_pct"], bins=[-1,0,10,20,100],
                              labels=["No Discount","1-10%","11-20%","20%+"])
    disc = dc.groupby("disc_band", as_index=False, observed=True).agg(
        avg_margin=("profit_margin_pct","mean"), orders=("order_id","count"))
    fig_disc = go.Figure(go.Bar(
        x=disc["disc_band"], y=disc["avg_margin"].round(1),
        marker_color=[C["a2"], C["a1"], C["a5"], C["a3"]],
        text=disc["avg_margin"].round(1).astype(str) + "%",
        textposition="outside", textfont=dict(size=10),
    ))
    fig_disc.update_layout(**BASE_LAYOUT, showlegend=False, yaxis_title="Avg Margin %")

    # Table
    recent = (d.sort_values("date", ascending=False).head(20)
               [["order_id","customer_id","product","category","sales","profit",
                 "profit_margin_pct","region","date","profit_tier","customer_segment"]]
               .copy())
    recent["sales"]             = recent["sales"].apply(fmt)
    recent["profit"]            = recent["profit"].apply(fmt)
    recent["profit_margin_pct"] = recent["profit_margin_pct"].apply(lambda x: f"{x:.1f}%")
    recent["date"]              = pd.to_datetime(recent["date"]).dt.strftime("%d %b %Y")

    tbl = dash_table.DataTable(
        data=recent.to_dict("records"),
        columns=[{"name": c.replace("_"," ").title(), "id": c} for c in recent.columns],
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": C["bg"], "color": C["sub"], "fontSize": "10px",
                       "fontWeight": "600", "textTransform": "uppercase", "letterSpacing": "0.5px",
                       "border": f"1px solid {C['border']}"},
        style_cell={"backgroundColor": C["card"], "color": C["text"], "fontSize": "12px",
                     "padding": "9px 12px", "border": f"1px solid {C['border']}",
                     "fontFamily": "'DM Sans',sans-serif"},
        style_data_conditional=[{"if": {"row_index": "odd"}, "backgroundColor": "#1A2030"}],
        page_size=10,
    )

    return kpis, fig_mon, fig_pie, fig_roll, fig_grw, fig_reg, fig_top, fig_tier, fig_seg, fig_disc, tbl


# ── ENTRY POINT ───────────────────────────────────────────
if __name__ == "__main__":
    print("\nDataPulse starting -> http://127.0.0.1:8050\n")
    app.run(debug=True, host="127.0.0.1", port=8050)
