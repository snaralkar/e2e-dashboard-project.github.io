"""
DataPulse -- Dash Analytics Dashboard
======================================
Run  : python app.py
Open : http://127.0.0.1:8050

Requirements:
    pip install dash plotly pandas numpy sqlalchemy pymysql

Author: Shubham Naralkar
"""

import sys
import pandas as pd
import numpy as np
from datetime import datetime, date

from sqlalchemy import create_engine, text
import dash
from dash import dcc, html, Input, Output, dash_table
import plotly.graph_objects as go
import plotly.express as px

from config import DB_CONFIG


# ─────────────────────────────────────────────
# DATA LOADER  (MySQL)
# ─────────────────────────────────────────────
def load_data() -> pd.DataFrame:
    """Load full sales table from MySQL."""
    url = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        f"?charset=utf8mb4"
    )
    engine = create_engine(url, echo=False)
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM sales", conn)
    engine.dispose()
    df["date"] = pd.to_datetime(df["date"])
    return df


df = load_data()

# Pre-compute dropdown options from data
ALL_YEARS    = sorted(df["year"].unique())
ALL_CATS     = sorted(df["category"].unique())
ALL_REGIONS  = sorted(df["region"].unique())
ALL_PRODUCTS = sorted(df["product"].unique())
ALL_QUARTERS = sorted(df["quarter"].unique())
DATE_MIN     = df["date"].min().date()
DATE_MAX     = df["date"].max().date()


# ─────────────────────────────────────────────
# COLOUR PALETTE
# ─────────────────────────────────────────────
COLORS = {
    "bg"      : "#0D1117",
    "card"    : "#161B22",
    "border"  : "#30363D",
    "accent1" : "#58A6FF",
    "accent2" : "#3FB950",
    "accent3" : "#F78166",
    "accent4" : "#D2A8FF",
    "accent5" : "#FFA657",
    "text"    : "#E6EDF3",
    "subtext" : "#8B949E",
}

CAT_COLORS = [
    COLORS["accent1"], COLORS["accent2"], COLORS["accent3"],
    COLORS["accent4"], COLORS["accent5"]
]

TIER_COLORS = {
    "High"  : COLORS["accent2"],
    "Medium": COLORS["accent1"],
    "Low"   : COLORS["accent5"],
    "Loss"  : COLORS["accent3"],
}

SEG_COLORS = {
    "Loyal"    : COLORS["accent2"],
    "Returning": COLORS["accent1"],
    "One-time" : COLORS["accent5"],
}

CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor ="rgba(0,0,0,0)",
    font=dict(color=COLORS["text"], family="'DM Sans', sans-serif"),
    xaxis=dict(gridcolor=COLORS["border"], zerolinecolor=COLORS["border"]),
    yaxis=dict(gridcolor=COLORS["border"], zerolinecolor=COLORS["border"]),
    margin=dict(l=40, r=20, t=40, b=40),
)

FILTER_STYLE = {
    "background": COLORS["card"],
    "color"     : COLORS["text"],
    "border"    : f"1px solid {COLORS['border']}",
}

LABEL_STYLE = {
    "color"        : COLORS["subtext"],
    "fontSize"     : "11px",
    "marginBottom" : "4px",
    "display"      : "block",
    "textTransform": "uppercase",
    "letterSpacing": "0.8px",
}


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def fmt_inr(val):
    if val >= 1_00_00_000:
        return f"Rs {val/1_00_00_000:.1f}Cr"
    elif val >= 1_00_000:
        return f"Rs {val/1_00_000:.1f}L"
    else:
        return f"Rs {val:,.0f}"


def kpi_card(title, value, sub=None, color=COLORS["accent1"]):
    return html.Div([
        html.P(title, style={"margin": "0", "fontSize": "11px",
                              "color": COLORS["subtext"], "letterSpacing": "1px",
                              "textTransform": "uppercase"}),
        html.H2(value, style={"margin": "4px 0 2px", "fontSize": "24px",
                               "color": color, "fontWeight": "700"}),
        html.Span(sub or "", style={"fontSize": "11px", "color": COLORS["subtext"]}),
    ], style={
        "background"  : COLORS["card"],
        "border"      : f"1px solid {COLORS['border']}",
        "borderTop"   : f"3px solid {color}",
        "borderRadius": "8px",
        "padding"     : "16px 18px",
        "flex"        : "1",
        "minWidth"    : "150px",
    })


def card_wrap(title, children, flex="1"):
    return html.Div([
        html.H3(title, style={"color": COLORS["subtext"], "fontSize": "11px",
                               "textTransform": "uppercase", "letterSpacing": "1px",
                               "margin": "0 0 12px"}),
        children,
    ], style={
        "flex"        : flex,
        "background"  : COLORS["card"],
        "border"      : f"1px solid {COLORS['border']}",
        "borderRadius": "8px",
        "padding"     : "18px",
    })


def empty_fig():
    return go.Figure(layout=CHART_LAYOUT)


# ─────────────────────────────────────────────
# APP LAYOUT
# ─────────────────────────────────────────────
app = dash.Dash(__name__, title="DataPulse")

app.layout = html.Div([

    html.Link(rel="stylesheet",
              href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&family=Space+Mono&display=swap"),

    # ── HEADER ────────────────────────────────────────
    html.Div([
        html.Div([
            html.Span("DataPulse", style={"fontSize": "20px", "fontWeight": "700",
                                           "color": COLORS["accent1"]}),
            html.Span(" / Sales Intelligence",
                      style={"color": COLORS["subtext"], "fontSize": "13px", "marginLeft": "6px"}),
        ]),
        html.Span(
            f"MySQL  |  Last synced: {datetime.now().strftime('%d %b %Y, %H:%M')}",
            style={"color": COLORS["subtext"], "fontSize": "11px", "fontFamily": "Space Mono"}
        ),
    ], style={
        "display": "flex", "justifyContent": "space-between", "alignItems": "center",
        "padding": "14px 28px", "borderBottom": f"1px solid {COLORS['border']}",
        "background": COLORS["card"],
    }),

    # ── FILTERS ───────────────────────────────────────
    html.Div([
        # Year
        html.Div([
            html.Label("Year", style=LABEL_STYLE),
            dcc.Dropdown(
                id="filter-year",
                options=[{"label": "All Years", "value": "all"}] +
                        [{"label": str(y), "value": y} for y in ALL_YEARS],
                value="all", clearable=False, style=FILTER_STYLE,
            ),
        ], style={"width": "130px"}),

        # Quarter
        html.Div([
            html.Label("Quarter", style=LABEL_STYLE),
            dcc.Dropdown(
                id="filter-quarter",
                options=[{"label": "All Quarters", "value": "all"}] +
                        [{"label": f"Q{q}", "value": q} for q in ALL_QUARTERS],
                value="all", clearable=False, style=FILTER_STYLE,
            ),
        ], style={"width": "140px"}),

        # Category
        html.Div([
            html.Label("Category", style=LABEL_STYLE),
            dcc.Dropdown(
                id="filter-category",
                options=[{"label": "All Categories", "value": "all"}] +
                        [{"label": c, "value": c} for c in ALL_CATS],
                value="all", clearable=False, style=FILTER_STYLE,
            ),
        ], style={"width": "180px"}),

        # Product
        html.Div([
            html.Label("Product", style=LABEL_STYLE),
            dcc.Dropdown(
                id="filter-product",
                options=[{"label": "All Products", "value": "all"}] +
                        [{"label": p, "value": p} for p in ALL_PRODUCTS],
                value="all", clearable=False, style=FILTER_STYLE,
            ),
        ], style={"width": "200px"}),

        # Region
        html.Div([
            html.Label("Region", style=LABEL_STYLE),
            dcc.Dropdown(
                id="filter-region",
                options=[{"label": "All Regions", "value": "all"}] +
                        [{"label": r, "value": r} for r in ALL_REGIONS],
                value="all", clearable=False, style=FILTER_STYLE,
            ),
        ], style={"width": "150px"}),

        # Date Range
        html.Div([
            html.Label("Date Range", style=LABEL_STYLE),
            dcc.DatePickerRange(
                id="filter-date",
                min_date_allowed=DATE_MIN,
                max_date_allowed=DATE_MAX,
                start_date=DATE_MIN,
                end_date=DATE_MAX,
                display_format="DD MMM YYYY",
                style={"fontSize": "12px"},
            ),
        ]),

    ], style={
        "display": "flex", "gap": "14px", "padding": "14px 28px",
        "background": COLORS["bg"], "alignItems": "flex-end", "flexWrap": "wrap",
        "borderBottom": f"1px solid {COLORS['border']}",
    }),

    # ── MAIN CONTENT ──────────────────────────────────
    html.Div([

        # KPI Row
        html.Div(id="kpi-row",
                 style={"display": "flex", "gap": "12px", "marginBottom": "14px", "flexWrap": "wrap"}),

        # ROW 1: Monthly Trend + Category Donut
        html.Div([
            card_wrap("Monthly Sales & Profit Trend",
                      dcc.Graph(id="chart-monthly", config={"displayModeBar": False},
                                style={"height": "260px"}), flex="2"),
            card_wrap("Revenue by Category",
                      dcc.Graph(id="chart-category-pie", config={"displayModeBar": False},
                                style={"height": "260px"}), flex="1"),
        ], style={"display": "flex", "gap": "12px", "marginBottom": "12px"}),

        # ROW 2: Rolling Avg + MoM Growth
        html.Div([
            card_wrap("3-Month Rolling Average Sales",
                      dcc.Graph(id="chart-rolling", config={"displayModeBar": False},
                                style={"height": "240px"}), flex="1"),
            card_wrap("MoM / YoY Growth %",
                      dcc.Graph(id="chart-growth", config={"displayModeBar": False},
                                style={"height": "240px"}), flex="1"),
        ], style={"display": "flex", "gap": "12px", "marginBottom": "12px"}),

        # ROW 3: Region + Top Products
        html.Div([
            card_wrap("Region Performance",
                      dcc.Graph(id="chart-region", config={"displayModeBar": False},
                                style={"height": "250px"}), flex="1"),
            card_wrap("Top 10 Products by Revenue",
                      dcc.Graph(id="chart-top-products", config={"displayModeBar": False},
                                style={"height": "250px"}), flex="1.4"),
        ], style={"display": "flex", "gap": "12px", "marginBottom": "12px"}),

        # ROW 4: Profit Tier + Customer Segmentation
        html.Div([
            card_wrap("Profit Tier Breakdown",
                      dcc.Graph(id="chart-profit-tier", config={"displayModeBar": False},
                                style={"height": "240px"}), flex="1"),
            card_wrap("Customer Segmentation",
                      dcc.Graph(id="chart-customer-seg", config={"displayModeBar": False},
                                style={"height": "240px"}), flex="1"),
            card_wrap("Discount Impact on Margin",
                      dcc.Graph(id="chart-discount", config={"displayModeBar": False},
                                style={"height": "240px"}), flex="1"),
        ], style={"display": "flex", "gap": "12px", "marginBottom": "12px"}),

        # ROW 5: Data Table
        card_wrap("Recent Transactions", html.Div(id="data-table")),

    ], style={"padding": "20px 28px", "background": COLORS["bg"], "minHeight": "100vh"}),

], style={"fontFamily": "'DM Sans', sans-serif", "background": COLORS["bg"],
          "color": COLORS["text"], "margin": "0"})


# ─────────────────────────────────────────────
# FILTER HELPER
# ─────────────────────────────────────────────
def apply_filters(year, quarter, category, product, region, start_date, end_date):
    d = df.copy()
    if year != "all":
        d = d[d["year"] == int(year)]
    if quarter != "all":
        d = d[d["quarter"] == int(quarter)]
    if category != "all":
        d = d[d["category"] == category]
    if product != "all":
        d = d[d["product"] == product]
    if region != "all":
        d = d[d["region"] == region]
    if start_date and end_date:
        d = d[(d["date"] >= pd.to_datetime(start_date)) &
              (d["date"] <= pd.to_datetime(end_date))]
    return d


# ─────────────────────────────────────────────
# MAIN CALLBACK
# ─────────────────────────────────────────────
@app.callback(
    Output("kpi-row",           "children"),
    Output("chart-monthly",     "figure"),
    Output("chart-category-pie","figure"),
    Output("chart-rolling",     "figure"),
    Output("chart-growth",      "figure"),
    Output("chart-region",      "figure"),
    Output("chart-top-products","figure"),
    Output("chart-profit-tier", "figure"),
    Output("chart-customer-seg","figure"),
    Output("chart-discount",    "figure"),
    Output("data-table",        "children"),
    Input("filter-year",     "value"),
    Input("filter-quarter",  "value"),
    Input("filter-category", "value"),
    Input("filter-product",  "value"),
    Input("filter-region",   "value"),
    Input("filter-date",     "start_date"),
    Input("filter-date",     "end_date"),
)
def update_all(year, quarter, category, product, region, start_date, end_date):
    d = apply_filters(year, quarter, category, product, region, start_date, end_date)
    ef = empty_fig()

    if d.empty:
        return [], ef, ef, ef, ef, ef, ef, ef, ef, ef, []

    # ── KPIs ──────────────────────────────────────────────────
    total_sales   = d["sales"].sum()
    total_profit  = d["profit"].sum()
    avg_margin    = d["profit_margin_pct"].mean()
    total_orders  = len(d)
    unique_custs  = d["customer_id"].nunique()
    avg_order_val = total_sales / total_orders if total_orders else 0

    kpis = [
        kpi_card("Total Revenue",   fmt_inr(total_sales),        sub=f"{total_orders:,} orders",  color=COLORS["accent1"]),
        kpi_card("Total Profit",    fmt_inr(total_profit),       sub=f"{avg_margin:.1f}% margin",  color=COLORS["accent2"]),
        kpi_card("Avg Order Value", fmt_inr(avg_order_val),      sub="per transaction",            color=COLORS["accent4"]),
        kpi_card("Unique Customers",f"{unique_custs:,}",         sub="active buyers",              color=COLORS["accent5"]),
        kpi_card("Outlier Orders",  f"{d['is_outlier'].sum():,}",sub="high-value spikes",          color=COLORS["accent3"]),
    ]

    # ── Monthly Trend ──────────────────────────────────────────
    monthly = (
        d.groupby("year_month", as_index=False)
         .agg(sales=("sales","sum"), profit=("profit","sum"))
         .sort_values("year_month")
    )
    fig_monthly = go.Figure()
    fig_monthly.add_trace(go.Bar(
        x=monthly["year_month"], y=monthly["sales"],
        name="Sales", marker_color=COLORS["accent1"], opacity=0.8,
    ))
    fig_monthly.add_trace(go.Scatter(
        x=monthly["year_month"], y=monthly["profit"],
        name="Profit", mode="lines+markers",
        line=dict(color=COLORS["accent2"], width=2), marker=dict(size=4),
    ))
    fig_monthly.update_layout(**CHART_LAYOUT,
                               legend=dict(orientation="h", y=1.15, font=dict(size=10)),
                               bargap=0.3)

    # ── Category Donut ─────────────────────────────────────────
    cat = d.groupby("category", as_index=False)["sales"].sum()
    fig_pie = go.Figure(go.Pie(
        labels=cat["category"], values=cat["sales"],
        hole=0.52, marker_colors=CAT_COLORS,
        textinfo="percent+label", textfont_size=10,
    ))
    fig_pie.update_layout(**CHART_LAYOUT, showlegend=False)

    # ── Rolling 3-Month Avg ────────────────────────────────────
    roll = (
        d.groupby("year_month", as_index=False)["sales"].sum()
         .sort_values("year_month")
    )
    roll["rolling_3m"] = roll["sales"].rolling(window=3, min_periods=1).mean()

    fig_rolling = go.Figure()
    fig_rolling.add_trace(go.Bar(
        x=roll["year_month"], y=roll["sales"],
        name="Monthly Sales", marker_color=COLORS["accent1"], opacity=0.45,
    ))
    fig_rolling.add_trace(go.Scatter(
        x=roll["year_month"], y=roll["rolling_3m"],
        name="3M Avg", mode="lines",
        line=dict(color=COLORS["accent5"], width=2.5, dash="dot"),
    ))
    fig_rolling.update_layout(**CHART_LAYOUT,
                               legend=dict(orientation="h", y=1.15, font=dict(size=10)),
                               bargap=0.3)

    # ── MoM / YoY Growth % ────────────────────────────────────
    growth = (
        d.groupby("year_month", as_index=False)["sales"].sum()
         .sort_values("year_month")
    )
    growth["mom_pct"] = growth["sales"].pct_change() * 100
    growth = growth.dropna()

    colors_growth = [COLORS["accent2"] if v >= 0 else COLORS["accent3"]
                     for v in growth["mom_pct"]]
    fig_growth = go.Figure(go.Bar(
        x=growth["year_month"], y=growth["mom_pct"].round(1),
        marker_color=colors_growth,
        text=growth["mom_pct"].round(1).astype(str) + "%",
        textposition="outside", textfont=dict(size=9),
    ))
    fig_growth.add_hline(y=0, line_color=COLORS["border"], line_width=1)
    fig_growth.update_layout(**CHART_LAYOUT, showlegend=False,
                              yaxis_title="MoM Growth %", bargap=0.3)

    # ── Region Performance ─────────────────────────────────────
    reg = (
        d.groupby("region", as_index=False)
         .agg(sales=("sales","sum"), profit=("profit","sum"))
         .sort_values("sales", ascending=True)
    )
    fig_region = go.Figure()
    fig_region.add_trace(go.Bar(
        y=reg["region"], x=reg["sales"], name="Sales",
        orientation="h", marker_color=COLORS["accent1"], opacity=0.85,
    ))
    fig_region.add_trace(go.Bar(
        y=reg["region"], x=reg["profit"], name="Profit",
        orientation="h", marker_color=COLORS["accent2"], opacity=0.85,
    ))
    fig_region.update_layout(**CHART_LAYOUT, barmode="group",
                              legend=dict(orientation="h", y=1.1, font=dict(size=10)))

    # ── Top Products ───────────────────────────────────────────
    top = (
        d.groupby("product", as_index=False)["sales"].sum()
         .nlargest(10, "sales").sort_values("sales")
    )
    fig_products = go.Figure(go.Bar(
        y=top["product"], x=top["sales"], orientation="h",
        marker=dict(color=top["sales"],
                    colorscale=[[0, COLORS["accent1"]], [1, COLORS["accent4"]]]),
        text=top["sales"].apply(fmt_inr),
        textposition="outside",
        textfont=dict(size=9, color=COLORS["text"]),
    ))
    fig_products.update_layout(**CHART_LAYOUT, showlegend=False)

    # ── Profit Tier Breakdown ──────────────────────────────────
    tier = (
        d.groupby("profit_tier", as_index=False)
         .agg(orders=("order_id","count"), revenue=("sales","sum"))
    )
    tier_order = ["Loss", "Low", "Medium", "High"]
    tier["profit_tier"] = pd.Categorical(tier["profit_tier"], categories=tier_order, ordered=True)
    tier = tier.sort_values("profit_tier")

    fig_tier = go.Figure()
    for _, row in tier.iterrows():
        t = str(row["profit_tier"])
        fig_tier.add_trace(go.Bar(
            x=[t], y=[row["orders"]],
            name=t, marker_color=TIER_COLORS.get(t, COLORS["accent1"]),
            text=[f"{row['orders']} orders"], textposition="outside",
            textfont=dict(size=10),
        ))
    fig_tier.update_layout(**CHART_LAYOUT, showlegend=False,
                            yaxis_title="Orders", bargap=0.35)

    # ── Customer Segmentation ──────────────────────────────────
    seg = (
        d.groupby("customer_segment", as_index=False)
         .agg(customers=("customer_id","nunique"), revenue=("sales","sum"))
    )
    seg_order = ["One-time", "Returning", "Loyal"]
    seg["customer_segment"] = pd.Categorical(
        seg["customer_segment"], categories=seg_order, ordered=True
    )
    seg = seg.sort_values("customer_segment")

    fig_seg = go.Figure()
    fig_seg.add_trace(go.Bar(
        x=seg["customer_segment"], y=seg["customers"],
        marker_color=[SEG_COLORS.get(s, COLORS["accent1"]) for s in seg["customer_segment"]],
        text=seg["customers"], textposition="outside", textfont=dict(size=10),
        yaxis="y", name="Customers",
    ))
    fig_seg.add_trace(go.Scatter(
        x=seg["customer_segment"], y=seg["revenue"],
        mode="lines+markers",
        line=dict(color=COLORS["accent4"], width=2),
        marker=dict(size=8), name="Revenue",
        yaxis="y2",
    ))
    seg_layout = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor ="rgba(0,0,0,0)",
        font=dict(color=COLORS["text"], family="'DM Sans', sans-serif"),
        margin=dict(l=40, r=50, t=40, b=40),
        xaxis=dict(gridcolor=COLORS["border"], zerolinecolor=COLORS["border"]),
        yaxis=dict(title="Customers", gridcolor=COLORS["border"], zerolinecolor=COLORS["border"]),
        yaxis2=dict(title="Revenue", overlaying="y", side="right",
                    gridcolor="rgba(0,0,0,0)", zerolinecolor="rgba(0,0,0,0)"),
        legend=dict(orientation="h", y=1.15, font=dict(size=10)),
        bargap=0.35,
    )
    fig_seg.update_layout(**seg_layout)

    # ── Discount Impact ────────────────────────────────────────
    dc = d.copy()
    dc["disc_band"] = pd.cut(
        dc["discount_pct"],
        bins=[-1, 0, 10, 20, 100],
        labels=["No Discount", "1-10%", "11-20%", "20%+"]
    )
    disc = (
        dc.groupby("disc_band", as_index=False, observed=True)
          .agg(avg_margin=("profit_margin_pct","mean"), orders=("order_id","count"))
    )
    fig_disc = go.Figure(go.Bar(
        x=disc["disc_band"], y=disc["avg_margin"].round(1),
        marker_color=[COLORS["accent2"], COLORS["accent1"],
                      COLORS["accent5"], COLORS["accent3"]],
        text=disc["avg_margin"].round(1).astype(str) + "%",
        textposition="outside", textfont=dict(size=10),
    ))
    fig_disc.update_layout(**CHART_LAYOUT, showlegend=False, yaxis_title="Avg Margin %")

    # ── Data Table ─────────────────────────────────────────────
    recent = (
        d.sort_values("date", ascending=False)
         .head(20)[["order_id","customer_id","product","category",
                    "sales","profit","profit_margin_pct","region","date",
                    "profit_tier","customer_segment"]]
         .copy()
    )
    recent["sales"]            = recent["sales"].apply(fmt_inr)
    recent["profit"]           = recent["profit"].apply(fmt_inr)
    recent["profit_margin_pct"]= recent["profit_margin_pct"].apply(lambda x: f"{x:.1f}%")
    recent["date"]             = pd.to_datetime(recent["date"]).dt.strftime("%d %b %Y")

    table = dash_table.DataTable(
        data=recent.to_dict("records"),
        columns=[{"name": c.replace("_"," ").title(), "id": c} for c in recent.columns],
        style_table={"overflowX": "auto"},
        style_header={
            "backgroundColor": COLORS["bg"], "color": COLORS["subtext"],
            "fontSize": "10px", "fontWeight": "600", "textTransform": "uppercase",
            "letterSpacing": "0.5px", "border": f"1px solid {COLORS['border']}",
        },
        style_cell={
            "backgroundColor": COLORS["card"], "color": COLORS["text"],
            "fontSize": "12px", "padding": "9px 12px",
            "border": f"1px solid {COLORS['border']}",
            "fontFamily": "'DM Sans', sans-serif",
        },
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": "#1A2030"},
        ],
        page_size=10,
    )

    return (kpis, fig_monthly, fig_pie, fig_rolling, fig_growth,
            fig_region, fig_products, fig_tier, fig_seg, fig_disc, table)


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("\nDataPulse Dashboard starting...")
    print("Open: http://127.0.0.1:8050\n")
    app.run(debug=True, host="127.0.0.1", port=8050)
