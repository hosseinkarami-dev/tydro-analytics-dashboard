import snowflake.connector
import streamlit as st
import altair as alt
import pandas as pd
from pathlib import Path
import plotly.graph_objects as go

st.set_page_config(layout="wide", page_title="Trdro on Ink Dashbaord")

def read_sql(file_path: str) -> str:
    path = Path(file_path)
    if not path.exists():
        st.error(f"SQL file not found: {file_path}")
        return ""
    return path.read_text()


def load_query_data(conn, file_path: str, condition: str, period: str):
    sql_query = read_sql(file_path)
    if not sql_query:
        return []
    sql_query = sql_query.replace("{condition}", condition)
    sql_query = sql_query.replace("{period}", period)
    with st.spinner("Loading data..."):
        cursor = conn.cursor()
        try:
            cursor.execute(sql_query)
            return cursor.fetchall()
        except Exception as e:
            st.error(f"Query execution failed: {e}")
            return []
        finally:
            try:
                cursor.close()
            except Exception:
                pass

def plot_cex_to_ink_inflow_volume_by_chain(conn, condition, period):
    results = load_query_data(conn, "queries/cex-to-ink-inflow-volume-by-chain.sql", condition, period)
    if not results:
        st.info("No CEX -> Ink inflow data returned by the query.")
        return

    # DataFrame
    df = pd.DataFrame(results, columns=['LABEL', 'VOLUME_USD'])
    df.columns = [c.lower() for c in df.columns]   # label, volume_usd
    df['label'] = df['label'].astype(str).str.strip()
    df = to_num(df, ['volume_usd'])

    # Remove zero rows (pie charts cannot handle zero angles)
    df = df[df['volume_usd'] > 0]

    # Normalize
    df = df.sort_values("volume_usd", ascending=False)

    st.subheader("CEX → Ink Inflow Volume by Exchange (USD)")

    # ==== PIE BASE ====
    pie = (
        alt.Chart(df)
        .mark_arc(innerRadius=70)
        .encode(
            theta=alt.Theta("volume_usd:Q", title="Volume (USD)"),
            color=alt.Color("label:N", title="Exchange"),
            tooltip=[
                alt.Tooltip("label:N", title="Exchange"),
                alt.Tooltip("volume_usd:Q", title="Volume (USD)", format=",.2f")
            ]
        )
        .properties(width=420, height=420)
    )

    st.altair_chart(pie, use_container_width=True)

def plot_bridge_inflows_outflows_by_chain(conn, condition, period):
    results = load_query_data(conn, "queries/bridge-inflows-outflows-by-chain.sql", condition, period)
    if not results:
        st.info("No bridge inflows/outflows data returned by the query.")
        return

    # Create DataFrame and normalize column names to lowercase
    df = pd.DataFrame(results, columns=[
        'DIRECTION', 'CHAIN', 'TRANSACTIONS', 'VOLUME_USD', 'AVERAGE_AMOUNT_USD'
    ])
    df.columns = [c.lower() for c in df.columns]   # now: direction, chain, transactions, volume_usd, average_amount_usd

    # Normalize text values (direction casing) and whitespace
    df['direction'] = df['direction'].astype(str).str.strip().str.title()  # e.g. "inflow" -> "Inflow"

    # Ensure numeric types
    df = to_num(df, ['transactions', 'volume_usd', 'average_amount_usd'])

    # Determine desired direction order (prefer Inflow then Outflow if present)
    preferred_dirs = ['Inflow', 'Outflow']
    present_dirs = [d for d in preferred_dirs if d in df['direction'].unique()]
    other_dirs = [d for d in df['direction'].unique() if d not in present_dirs]
    dir_order = present_dirs + sorted(other_dirs)  # deterministic order

    # Make direction categorical for plotting order
    df['direction'] = pd.Categorical(df['direction'], categories=dir_order, ordered=True)

    # Order chains by total volume (largest first) for consistent colors/ordering
    chain_order = (
        df.groupby('chain')['volume_usd']
        .sum()
        .sort_values(ascending=False)
        .index.tolist()
    )

    # Subheader
    st.subheader("Bridge inflows / outflows by chain (volume USD)")

    # If there are a lot of chains, informing user helps
    if len(chain_order) > 12:
        st.caption(f"{len(chain_order)} chains detected — chart may appear crowded.")

    # Build grouped (side-by-side) bar chart using xOffset
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X('direction:N', title='Direction', sort=dir_order),
            y=alt.Y('volume_usd:Q', title='Volume (USD)', axis=alt.Axis(format=",.0f")),
            color=alt.Color('chain:N', title='Chain', sort=chain_order),
            tooltip=[
                alt.Tooltip('direction:N', title='Direction'),
                alt.Tooltip('chain:N', title='Chain'),
                alt.Tooltip('transactions:Q', title='Transactions'),
                alt.Tooltip('volume_usd:Q', title='Volume (USD)', format=",.0f"),
                alt.Tooltip('average_amount_usd:Q', title='Avg amount (USD)', format=",.0f"),
            ],
            xOffset='chain:N'   # places bars side-by-side for each chain within each direction
        )
        .properties(height=420)
        .interactive()
    )

    st.altair_chart(chart, use_container_width=True)

def plot_user_flow_sankey(
        conn,
        condition,
        period,
        preserve='before',
        query_path="queries/user-behavior-before-and-after-tydro-interaction.sql"
):

    results = load_query_data(conn, query_path, condition, period)
    if not results:
        st.info("No data returned from user behavior query.")
        return

    # 2) Build DataFrame
    df = pd.DataFrame(results, columns=["action_type", "event_name", "users"])
    df["action_type"] = df["action_type"].astype(str).str.strip().str.title()
    df["event_name"] = df["event_name"].astype(str).str.strip()
    df["users"] = pd.to_numeric(df["users"], errors="coerce").fillna(0)

    # Separate Before / After
    before = df[df["action_type"] == "Before"].set_index("event_name")["users"].to_dict()
    after = df[df["action_type"] == "After"].set_index("event_name")["users"].to_dict()

    # If one side is missing, no Sankey possible
    if len(before) == 0 or len(after) == 0:
        st.warning("Either Before or After data is missing — showing fallback bar chart.")
        fallback = df.pivot(index="event_name", columns="action_type", values="users").fillna(0)
        fallback = fallback.reset_index().melt(id_vars="event_name", var_name="phase", value_name="users")

        chart = (
            alt.Chart(fallback)
            .mark_bar()
            .encode(
                x=alt.X("phase:N", title="Phase"),
                y=alt.Y("users:Q", title="Users"),
                color="event_name:N",
                column=alt.Column("event_name:N", header=alt.Header(labelAngle=270))
            )
            .properties(height=300)
        )
        st.altair_chart(chart, use_container_width=True)
        return

    # 3) Node labels
    before_events = list(before.keys())
    after_events = list(after.keys())

    nodes = [f"Before: {e}" for e in before_events] + [f"After: {e}" for e in after_events]

    index_before = {e: i for i, e in enumerate(before_events)}
    index_after = {e: i + len(before_events) for i, e in enumerate(after_events)}

    # 4) Compute approximate flows
    before_total = sum(before.values())
    after_total = sum(after.values())

    sources = []
    targets = []
    values = []
    hover_labels = []

    if preserve not in ("before", "after"):
        preserve = "before"

    if preserve == "before":
        # Distribute each Before bucket across After buckets proportionally
        if after_total == 0:
            st.error("Cannot build Sankey: After total = 0")
            return
        for b in before_events:
            b_val = before[b]
            for a in after_events:
                a_val = after[a]
                flow = b_val * (a_val / after_total)
                if flow > 0:
                    sources.append(index_before[b])
                    targets.append(index_after[a])
                    values.append(float(flow))
                    hover_labels.append(f"{b} → {a}<br>Users (approx.): {flow:,.0f}")

    else:  # preserve == "after"
        if before_total == 0:
            st.error("Cannot build Sankey: Before total = 0")
            return
        for b in before_events:
            b_val = before[b]
            for a in after_events:
                a_val = after[a]
                flow = a_val * (b_val / before_total)
                if flow > 0:
                    sources.append(index_before[b])
                    targets.append(index_after[a])
                    values.append(float(flow))
                    hover_labels.append(f"{b} → {a}<br>Users (approx.): {flow:,.0f}")

    if not values:
        st.warning("All computed flows are zero — cannot render Sankey.")
        return

    # 5) Build Sankey diagram
    node_colors = (
        ["#8DD3C7"] * len(before_events) +  # Before nodes
        ["#FB8072"] * len(after_events)     # After nodes
    )

    sankey = go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=nodes,
            color=node_colors
        ),
        link=dict(
            source=sources,
            target=targets,
            value=values,
            hovertemplate=hover_labels
        )
    )

    fig = go.Figure(data=[sankey])
    fig.update_layout(
        title="User Behavior Flow — Before → After",
        font_size=12,
        height=600
    )

    st.plotly_chart(fig, use_container_width=True)

def plot_bridge_inflows_outflows_by_token(conn, condition, period):
    results = load_query_data(conn, "queries/bridge-inflows-outflows-by-token.sql", condition, period)
    if not results:
        st.info("No bridge inflows/outflows data returned by the query.")
        return

    # Create DataFrame and normalize column names to lowercase
    df = pd.DataFrame(results, columns=[
        'DIRECTION', 'SYMBOL', 'TRANSACTIONS', 'VOLUME_USD', 'AVERAGE_AMOUNT_USD'
    ])
    df.columns = [c.lower() for c in df.columns]   # now: direction, token, transactions, volume_usd, average_amount_usd

    # Normalize text values (direction casing) and whitespace
    df['direction'] = df['direction'].astype(str).str.strip().str.title()  # e.g. "inflow" -> "Inflow"

    # Ensure numeric types
    df = to_num(df, ['transactions', 'volume_usd', 'average_amount_usd'])

    # Determine desired direction order (prefer Inflow then Outflow if present)
    preferred_dirs = ['Inflow', 'Outflow']
    present_dirs = [d for d in preferred_dirs if d in df['direction'].unique()]
    other_dirs = [d for d in df['direction'].unique() if d not in present_dirs]
    dir_order = present_dirs + sorted(other_dirs)  # deterministic order

    # Make direction categorical for plotting order
    df['direction'] = pd.Categorical(df['direction'], categories=dir_order, ordered=True)

    # Order tokens by total volume (largest first) for consistent colors/ordering
    token_order = (
        df.groupby('symbol')['volume_usd']
        .sum()
        .sort_values(ascending=False)
        .index.tolist()
    )

    # Subheader
    st.subheader("Bridge inflows / outflows by Token (volume USD)")

    # If there are a lot of tokens, informing user helps
    if len(token_order) > 12:
        st.caption(f"{len(token_order)} symbols detected — chart may appear crowded.")

    # Build grouped (side-by-side) bar chart using xOffset
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X('direction:N', title='Direction', sort=dir_order),
            y=alt.Y('volume_usd:Q', title='Volume (USD)', axis=alt.Axis(format=",.0f")),
            color=alt.Color('symbol:N', title='Token', sort=token_order),
            tooltip=[
                alt.Tooltip('direction:N', title='Direction'),
                alt.Tooltip('symbol:N', title='Token'),
                alt.Tooltip('transactions:Q', title='Transactions'),
                alt.Tooltip('volume_usd:Q', title='Volume (USD)', format=",.0f"),
                alt.Tooltip('average_amount_usd:Q', title='Avg amount (USD)', format=",.0f"),
            ],
            xOffset='symbol:N'   # places bars side-by-side for each token within each direction
        )
        .properties(height=420)
        .interactive()
    )

    st.altair_chart(chart, use_container_width=True)


def plot_tydro_inflows_outflows_by_token(conn, condition, period):
    results = load_query_data(conn, "queries/inflows-outflows-by-token.sql", condition, period)
    if not results:
        st.info("No by-token data returned by the query.")
        return

    # Build DataFrame and normalize column names to lowercase
    df = pd.DataFrame(results, columns=[
        'EVENT_NAME', 'SYMBOL', 'VOLUME', 'VOLUME_USD', 'AVERAGE_AMOUNT', 'AVERAGE_AMOUNT_USD'
    ])
    df.columns = [c.lower() for c in df.columns]  # now: event_name, symbol, volume, volume_usd, average_amount, average_amount_usd

    # Normalize text values and casing
    df['event_name'] = df['event_name'].astype(str).str.strip().str.title()   # e.g. "supply" -> "Supply"
    df['symbol'] = df['symbol'].astype(str).str.strip()

    # Ensure numeric types
    df = to_num(df, ['volume','volume_usd','average_amount','average_amount_usd'])

    # Desired order for event_name (prefer Supply then Withdraw if present)
    preferred_events = ['Supply', 'Withdraw']
    present_events = [e for e in preferred_events if e in df['event_name'].unique()]
    other_events = [e for e in df['event_name'].unique() if e not in present_events]
    event_order = present_events + sorted(other_events)
    df['event_name'] = pd.Categorical(df['event_name'], categories=event_order, ordered=True)

    # Order symbols by total volume_usd (descending) for consistent coloring/order
    symbol_order = (
        df.groupby('symbol')['volume_usd']
        .sum()
        .sort_values(ascending=False)
        .index.tolist()
    )

    st.subheader("Tydro Inflow/Outflow Volume by token")

    if len(symbol_order) > 12:
        st.caption(f"{len(symbol_order)} symbols detected — chart may be crowded.")

    # Build grouped bar chart (xOffset by symbol for side-by-side bars)
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X('event_name:N', title='Event', sort=event_order),
            y=alt.Y('volume_usd:Q', title='Volume (USD)', axis=alt.Axis(format=",.0f")),
            color=alt.Color('symbol:N', title='Symbol', sort=symbol_order),
            tooltip=[
                alt.Tooltip('event_name:N', title='Event'),
                alt.Tooltip('symbol:N', title='Symbol'),
                alt.Tooltip('volume:Q', title='Volume (native)'),
                alt.Tooltip('volume_usd:Q', title='Volume (USD)', format=",.0f"),
                alt.Tooltip('average_amount_usd:Q', title='Avg amount (USD)', format=",.0f"),
            ],
            xOffset='symbol:N'
        )
        .properties(height=420)
        .interactive()
    )

    st.altair_chart(chart, use_container_width=True)



def tydro_general(conn, condition, period):
    borrow_stats = load_query_data(conn, "queries/total-borrow.sql", condition, period)
    supply_stats = load_query_data(conn, "queries/total-supply.sql", condition, period)

    prefix = "Borrow"
    total_transactions, total_users, total_volume_usd, avg_amount_usd, median_amount_usd, max_amount_usd = borrow_stats[0]
    c1, c2, c3 = st.columns(3)
    c1.metric(f"{prefix} Transactions", f"{int(total_transactions):,}")
    c2.metric(f"{prefix} Users", f"{int(total_users):,}")
    c3.metric(f"{prefix} Volume (USD)", f"${float(total_volume_usd):,.2f}")

    prefix = "Supply"
    total_transactions, total_users, total_volume_usd, avg_amount_usd, median_amount_usd, max_amount_usd = supply_stats[0]
    c1, c2, c3 = st.columns(3)
    c1.metric(f"{prefix} Transactions", f"{int(total_transactions):,}")
    c2.metric(f"{prefix} Users", f"{int(total_users):,}")
    c3.metric(f"{prefix} Volume (USD)", f"${float(total_volume_usd):,.2f}")

def tydro_historical_data(conn, condition, period, period_choice):
    with st.spinner(f"Loading historical data for {range_choice} ({period_choice})..."):
        overtime_results = load_query_data(conn, "queries/overtime.sql", condition, period)

    if overtime_results:
        df = pd.DataFrame(overtime_results, columns=[
            'date','event_name','transactions','users','volume_usd',
            'average_amount_usd','median_amount_usd','max_amount_usd'
        ])

        # convert and sanitize
        df['date'] = pd.to_datetime(df['date'])
        df = to_num(df, ['transactions','users','volume_usd','average_amount_usd','median_amount_usd','max_amount_usd'])

        # AGGREGATE to ensure one row per date+event (safety)
        df = df.groupby(['date','event_name'], as_index=False).agg({
            'transactions':'sum',
            'users':'sum',
            'volume_usd':'sum',
            'average_amount_usd':'mean',
            'median_amount_usd':'median',
            'max_amount_usd':'max'
        }).sort_values('date')

        # separate event dataframes
        supply_data = df[df['event_name']=='Supply']
        borrow_data = df[df['event_name']=='Borrow']
        withdraw_data = df[df['event_name']=='Withdraw']
        repay_data = df[df['event_name']=='Repay']


        # ---------------------------
        # 3-Column Layout
        # ---------------------------
        col1, col2, col3 = st.columns(3)

        # ---------------------------
        # Transactions per Event (stacked/grouped)
        # ---------------------------
        with col1:
            st.subheader(f"{period_choice} Transactions per Event")
            chart_tx = (
                alt.Chart(df)
                .mark_bar()
                .encode(
                    x=alt.X("date:T", title="Date"),
                    y=alt.Y("transactions:Q", title="Transactions"),
                    color=alt.Color("event_name:N", title="Event"),
                    tooltip=[
                        alt.Tooltip("date:T", title="Date"),
                        alt.Tooltip("event_name:N", title="Event"),
                        alt.Tooltip("transactions:Q", title="Transactions"),
                        alt.Tooltip("users:Q", title="Active Users"),
                        alt.Tooltip("volume_usd:Q", title="Volume USD", format=","),
                    ],
                )
                .properties(height=350)
                .interactive()
            )
            st.altair_chart(chart_tx, use_container_width=True)

        # ---------------------------
        # Active users per event
        # ---------------------------
        with col2:
            st.subheader("Active Users per Event")
            chart_users = (
                alt.Chart(df)
                .mark_bar()
                .encode(
                    x="date:T",
                    y="users:Q",
                    color="event_name:N",
                    tooltip=["date:T", "event_name:N", "users:Q"],
                )
                .properties(height=350)
                .interactive()
            )
            st.altair_chart(chart_users, use_container_width=True)

        # ---------------------------
        # Volume (USD) per event (line)
        # ---------------------------
        with col3:
            st.subheader("Volume (USD) per Event")
            chart_volume = (
                alt.Chart(df)
                .mark_line(point=True)
                .encode(
                    x="date:T",
                    y=alt.Y("volume_usd:Q", title="Volume (USD)"),
                    color="event_name:N",
                    tooltip=[
                        "date:T",
                        "event_name:N",
                        alt.Tooltip("volume_usd:Q", format=","),
                    ],
                )
                .properties(height=350)
                .interactive()
            )
            st.altair_chart(chart_volume, use_container_width=True)


    st.subheader(f"📊 {period_choice} Supply & Borrow Metrics")

    # 1st row of charts
    row1_col1, row1_col2, row1_col3 = st.columns(3)

    # ---------------------------
    # Weekly Supply Transactions (bar)
    # ---------------------------
    with row1_col1:
        st.markdown(f"### {period_choice} Supply Transactions")
        chart_supply_tx = (
            alt.Chart(supply_data)
            .mark_bar()
            .encode(
                x="date:T",
                y="transactions:Q",
                tooltip=["date:T", "transactions:Q"],
            )
            .properties(height=300)
            .interactive()
        )
        st.altair_chart(chart_supply_tx, use_container_width=True)

    # ---------------------------
    # Weekly USD Supply Volume (line)
    # ---------------------------
    with row1_col2:
        st.markdown(f"### {period_choice} Supply Volume (USD)")
        chart_supply_volume = (
            alt.Chart(supply_data)
            .mark_line(point=True)
            .encode(
                x="date:T",
                y=alt.Y("volume_usd:Q", title="Volume (USD)"),
                tooltip=["date:T", alt.Tooltip("volume_usd:Q", format=",")],
            )
            .properties(height=300)
            .interactive()
        )
        st.altair_chart(chart_supply_volume, use_container_width=True)

    # ---------------------------
    # Weekly Active Suppliers (bar)
    # ---------------------------
    with row1_col3:
        st.markdown(f"### {period_choice} Active Suppliers")
        chart_supply_users = (
            alt.Chart(supply_data)
            .mark_bar()
            .encode(
                x="date:T",
                y="users:Q",
                tooltip=["date:T", "users:Q"],
            )
            .properties(height=300)
            .interactive()
        )
        st.altair_chart(chart_supply_users, use_container_width=True)

    # 2nd row of charts
    row2_col1, row2_col2, row2_col3 = st.columns(3)

    # ---------------------------
    # Weekly Borrow Transactions (bar)
    # ---------------------------
    with row2_col1:
        st.markdown(f"### {period_choice} Borrow Transactions")
        chart_borrow_tx = (
            alt.Chart(borrow_data)
            .mark_bar()
            .encode(
                x="date:T",
                y="transactions:Q",
                tooltip=["date:T", "transactions:Q"],
            )
            .properties(height=300)
            .interactive()
        )
        st.altair_chart(chart_borrow_tx, use_container_width=True)

    # ---------------------------
    # Weekly Borrow Volume (USD) (line)
    # ---------------------------
    with row2_col2:
        st.markdown(f"### {period_choice} Borrow Volume (USD)")
        chart_borrow_volume = (
            alt.Chart(borrow_data)
            .mark_line(point=True)
            .encode(
                x="date:T",
                y=alt.Y("volume_usd:Q", title="Volume (USD)"),
                tooltip=["date:T", alt.Tooltip("volume_usd:Q", format=",")],
            )
            .properties(height=300)
            .interactive()
        )
        st.altair_chart(chart_borrow_volume, use_container_width=True)

    # ---------------------------
    # Weekly Active Borrowers (bar)
    # ---------------------------
    with row2_col3:
        st.markdown(f"### {period_choice} Active Borrowers")
        chart_borrow_users = (
            alt.Chart(borrow_data)
            .mark_bar()
            .encode(
                x="date:T",
                y="users:Q",
                tooltip=["date:T", "users:Q"],
            )
            .properties(height=300)
            .interactive()
        )
        st.altair_chart(chart_borrow_users, use_container_width=True)


def to_num(df, cols):
    for c in cols:
        df[c] = pd.to_numeric(df[c].astype(str).str.replace(',',''), errors='coerce').fillna(0)
    return df


def display_bridge_big_numbers(conn, condition, period):
    bridge_stats = load_query_data(conn, "queries/total-bridge.sql", condition, period)

    total_borrowed_volume_of_tydro, total_bridged_out_volume, borrowed_vs_bridged_out = bridge_stats[0]
    c1, c2, c3 = st.columns(3)
    c1.metric(f"Total Borrowed Volume of Tydro", f"{int(total_borrowed_volume_of_tydro):,}")
    c2.metric(f"Total Bridged out Volume (USD)", f"{int(total_bridged_out_volume):,}")
    c3.metric(f"Borrowed vs. Bridged-Out Ratio", f"%{float(borrowed_vs_bridged_out):,.2f}")


def plot_deposit_size_distribution(conn, condition, period):
    title="Deposit Size Distribution — Volume per Bucket"
    deposit_results = load_query_data(conn, "queries/deposit-size-distribution.sql", condition, period)

    if not deposit_results:
        st.warning("No deposit size data available.")
        return

    # -----------------------------------
    # Load into Frame
    # -----------------------------------
    df = pd.DataFrame(deposit_results, columns=[
        'DEPOSIT_SIZE_RANGE','DEPOSIT_COUNT','TOTAL_DEPOSIT_USD','MIN_AMOUNT_USD','MAX_AMOUNT_USD'
    ])

    # Fix mojibake or dash issues
    df['DEPOSIT_SIZE_RANGE'] = (
        df['DEPOSIT_SIZE_RANGE'].astype(str)
        .str.replace('â€“', '–')
        .str.replace('-', '–')
        .str.strip()
    )

    # Numeric columns
    for col in ['DEPOSIT_COUNT','TOTAL_DEPOSIT_USD','MIN_AMOUNT_USD','MAX_AMOUNT_USD']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # -----------------------------------
    # AUTO-SORT BUCKETS from MIN_AMOUNT_USD
    # -----------------------------------
    df = df.sort_values("MIN_AMOUNT_USD")
    ordered_buckets = df["DEPOSIT_SIZE_RANGE"].tolist()

    # -----------------------------------
    # Categorical order for plotting
    # -----------------------------------
    df["DEPOSIT_SIZE_RANGE"] = pd.Categorical(
        df["DEPOSIT_SIZE_RANGE"],
        categories=ordered_buckets,
        ordered=True
    )

    # -----------------------------------
    # Percent of total
    # -----------------------------------
    total_count = df["DEPOSIT_COUNT"].sum()
    df["PCT_OF_TOTAL"] = (
        (df["DEPOSIT_COUNT"] / total_count * 100).round(2)
        if total_count > 0 else 0
    )

    # -----------------------------------
    # Chart
    # -----------------------------------
    st.subheader(title)

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("DEPOSIT_SIZE_RANGE:N", title="Deposit Size Range", sort=ordered_buckets),
            y=alt.Y("TOTAL_DEPOSIT_USD:Q", title="Total Deposit Volume (USD)", axis=alt.Axis(format=",.0f")),
            tooltip=[
                alt.Tooltip("DEPOSIT_SIZE_RANGE:N", title="Bucket"),
                alt.Tooltip("DEPOSIT_COUNT:Q", title="Count"),
                alt.Tooltip("TOTAL_DEPOSIT_USD:Q", title="Total USD", format=",.0f"),
                alt.Tooltip("PCT_OF_TOTAL:Q", title="% of Total")
            ]
        )
        .properties(height=420)
    )

    # Add labels on bars
    text = (
        chart.mark_text(align="center", dy=-6, size=11)
        .encode(text=alt.Text("TOTAL_DEPOSIT_USD:Q", format=",.0f"))
    )

    st.altair_chart(chart + text, use_container_width=True)

try:
    conn = snowflake.connector.connect(
        user='afonsodiaz',
        password='cnuppNkP8qk7TNK',
        account='gob41769.us-east-1',
        warehouse='INK_ENGINE',
        database='INK',
        schema='CORE'
    )

    # Settings
    with st.expander("⚙️ Configuration", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            range_choice = st.radio("Select Time Range:", ["All time", "Past year", "Past month", "Past week"], horizontal=True)
        with col2:
            period_choice = st.radio("Select Aggregation Period:", ["Daily", "Weekly", "Monthly"], horizontal=True)

        if range_choice == "All time":
            condition = "1 = 1"
        elif range_choice == "Past year":
            condition = "block_timestamp::date >= current_date - interval '1 year'"
        elif range_choice == "Past month":
            condition = "block_timestamp::date >= current_date - interval '1 month'"
        else:
            condition = "block_timestamp::date >= current_date - interval '7 day'"

        if period_choice == "Daily":
            period = "day"
        elif period_choice == "Weekly":
            period = "week"
        else:
            period = "month"

    tydro_general(conn, condition, period)

    tydro_historical_data(conn, condition, period, period_choice)

    plot_deposit_size_distribution(conn, condition, period)

    plot_tydro_inflows_outflows_by_token(conn, condition, period)

    display_bridge_big_numbers(conn, condition, period)

    plot_bridge_inflows_outflows_by_chain(conn, condition, period)

    col_left, col_right = st.columns(2)

    with col_left:
        plot_bridge_inflows_outflows_by_token(conn, condition, period)

    with col_right:
        plot_cex_to_ink_inflow_volume_by_chain(conn, condition, period)

    plot_user_flow_sankey(conn, condition, period, preserve='before')

except Exception as e:
    st.error(f"Connection failed: {e}")
finally:
    try:
        conn.close()
    except Exception:
        pass
