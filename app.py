import sqlite3
from contextlib import closing
from datetime import datetime, date
import calendar
import pandas as pd
import streamlit as st

DB_PATH = "entrenos.db"

# ----- Meses en español -----
MESES_ES = [
    "Enero","Febrero","Marzo","Abril","Mayo","Junio",
    "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"
]
MES_A_NUM = {name: i+1 for i, name in enumerate(MESES_ES)}

# ---------- Helpers ----------
def fmt_money(x: float) -> str:
    try:
        return f"${x:,.0f}".replace(",", ".")
    except Exception:
        return str(x)

def normalize_client(raw: str) -> str:
    """
    Normaliza el nombre de cliente:
    - quita espacios dobles
    - recorta extremos
    - Title Case (Juana Pérez)
    """
    if not raw:
        return ""
    s = " ".join(raw.split())
    return s.title()

# ---------- DB: creación y helpers ----------
def init_db():
    with closing(sqlite3.connect(DB_PATH)) as conn, conn, closing(conn.cursor()) as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client TEXT NOT NULL,
                ts TEXT NOT NULL,          -- 'YYYY-MM-DD HH:MM:SS'
                amount REAL NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS monthly_payments (
                client TEXT NOT NULL,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                paid INTEGER NOT NULL DEFAULT 0,  -- 0 pendiente, 1 pagado
                paid_on TEXT,                     -- ISO date
                PRIMARY KEY (client, year, month)
            )
        """)
        conn.commit()

    # Unifica clientes ya existentes (migración suave)
    migrate_normalize_clients()

def migrate_normalize_clients():
    with closing(sqlite3.connect(DB_PATH)) as conn, conn, closing(conn.cursor()) as cur:
        # Distintos en sessions
        cur.execute("SELECT DISTINCT client FROM sessions")
        rows = cur.fetchall()
        mapping = {}
        for (c,) in rows:
            if c is None:
                continue
            canon = normalize_client(c)
            if canon != c:
                mapping[c] = canon

        # Aplica mapping a sessions y monthly_payments
        for old, new in mapping.items():
            cur.execute("UPDATE sessions SET client=? WHERE client=?", (new, old))
            cur.execute("UPDATE monthly_payments SET client=? WHERE client=?", (new, old))
        conn.commit()

def add_session(client: str, ts: datetime, amount: float):
    client = normalize_client(client)
    with closing(sqlite3.connect(DB_PATH)) as conn, conn, closing(conn.cursor()) as cur:
        cur.execute(
            "INSERT INTO sessions (client, ts, amount) VALUES (?, ?, ?)",
            (client, ts.strftime("%Y-%m-%d %H:%M:%S"), float(amount))
        )
        conn.commit()

def delete_session(row_id: int):
    with closing(sqlite3.connect(DB_PATH)) as conn, conn, closing(conn.cursor()) as cur:
        cur.execute("DELETE FROM sessions WHERE id = ?", (row_id,))
        conn.commit()

def fetch_sessions_between(start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        df = pd.read_sql_query(
            """
            SELECT id, client, ts, amount
            FROM sessions
            WHERE ts >= ? AND ts < ?
            ORDER BY ts ASC
            """,
            conn,
            params=(start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    end_dt.strftime("%Y-%m-%d %H:%M:%S"))
        )
    if not df.empty:
        df["client"] = df["client"].apply(normalize_client)
        df["ts"] = pd.to_datetime(df["ts"])
        df["fecha"] = df["ts"].dt.date
        df["hora"] = df["ts"].dt.strftime("%H:%M")
        df = df[["id", "client", "fecha", "hora", "amount", "ts"]]
    return df

def month_range(year: int, month: int):
    start_dt = datetime(year, month, 1, 0, 0, 0)
    end_dt = datetime(year + (1 if month == 12 else 0), (1 if month == 12 else month + 1), 1, 0, 0, 0)
    return start_dt, end_dt

def fetch_distinct_clients() -> list:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        df = pd.read_sql_query("SELECT DISTINCT client FROM sessions", conn)
    if df.empty:
        return []
    df["client"] = df["client"].apply(normalize_client)
    return sorted(df["client"].unique().tolist())

def get_monthly_payment(client: str, year: int, month: int) -> dict:
    client = normalize_client(client)
    with closing(sqlite3.connect(DB_PATH)) as conn:
        df = pd.read_sql_query(
            "SELECT paid, paid_on FROM monthly_payments WHERE client=? AND year=? AND month=?",
            conn, params=(client, year, month)
        )
    if df.empty:
        return {"paid": 0, "paid_on": None}
    row = df.iloc[0]
    return {"paid": int(row["paid"]), "paid_on": row["paid_on"]}

def set_monthly_payment(client: str, year: int, month: int, paid: bool, paid_on: date | None):
    client = normalize_client(client)
    paid_on_str = paid_on.strftime("%Y-%m-%d") if (paid and paid_on) else (None if not paid else date.today().strftime("%Y-%m-%d"))
    with closing(sqlite3.connect(DB_PATH)) as conn, conn, closing(conn.cursor()) as cur:
        cur.execute("""
            INSERT INTO monthly_payments (client, year, month, paid, paid_on)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(client, year, month) DO UPDATE SET
                paid=excluded.paid,
                paid_on=excluded.paid_on
        """, (client, year, month, 1 if paid else 0, paid_on_str))
        conn.commit()

def month_label_es(y: int, m: int) -> str:
    return f"{MESES_ES[m-1]} {y}"

def sessions_agg_by_client_month(client: str | None = None) -> pd.DataFrame:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        df = pd.read_sql_query("SELECT client, ts, amount FROM sessions", conn)
    if df.empty:
        return pd.DataFrame(columns=["Cliente", "Año", "Mes", "Clases", "Monto"])

    df["client"] = df["client"].apply(normalize_client)
    df["ts"] = pd.to_datetime(df["ts"])
    df["Año"] = df["ts"].dt.year
    df["Mes"] = df["ts"].dt.month
    if client:
        df = df[df["client"] == normalize_client(client)]

    agg = (
        df.groupby(["client", "Año", "Mes"])
        .agg(Clases=("ts", "count"), Monto=("amount", "sum"))
        .reset_index()
        .rename(columns={"client": "Cliente"})
    )
    return agg.sort_values(["Cliente", "Año", "Mes"])

def join_with_payments(agg_df: pd.DataFrame) -> pd.DataFrame:
    if agg_df.empty:
        return agg_df
    with closing(sqlite3.connect(DB_PATH)) as conn:
        pays = pd.read_sql_query("""
            SELECT client as Cliente, year as Año, month as Mes, paid, paid_on
            FROM monthly_payments
        """, conn)
    if not pays.empty:
        pays["Cliente"] = pays["Cliente"].apply(normalize_client)

    if pays.empty:
        agg_df["Estado mes"] = "Pendiente"
        agg_df["Fecha pago mes"] = "—"
        return agg_df

    merged = pd.merge(agg_df, pays, on=["Cliente", "Año", "Mes"], how="left")
    merged["paid"] = merged["paid"].fillna(0).astype(int)
    merged["Estado mes"] = merged["paid"].map({0: "Pendiente", 1: "Pagado"})
    merged["Fecha pago mes"] = merged["paid_on"].fillna("—")
    merged = merged.drop(columns=["paid", "paid_on"])
    return merged

# ---------- UI ----------
st.set_page_config(page_title="Entrenos - Registro y Resumen", page_icon="💪", layout="wide")
init_db()

st.title("💪 Registro de Entrenos para Cobro")
st.caption("Registra clases y lleva el pago por **mes** y por **persona**. Nombres unificados (sin duplicados por mayúsculas/minúsculas).")

# Sidebar: filtros para VER el mes
today = date.today()
col1, col2 = st.sidebar.columns(2)
with col1:
    year = st.number_input("Año", min_value=2020, max_value=2100, value=today.year, step=1)
with col2:
    month = st.number_input("Mes (1-12)", min_value=1, max_value=12, value=today.month, step=1)
start_dt, end_dt = month_range(int(year), int(month))

# ---------- Registrar una clase (selector + "nuevo") ----------
st.subheader("Registrar una clase")

existing_clients = fetch_distinct_clients()
SEL_NEW = "(Escribir nombre nuevo)"
cliente_sel = st.selectbox("Cliente", [SEL_NEW] + existing_clients, index=0)

if cliente_sel == SEL_NEW:
    cliente_input = st.text_input("Nombre del cliente*", placeholder="Ej: Juano Monroy")
else:
    cliente_input = cliente_sel  # ya normalizado

with st.form("form_registro", clear_on_submit=True):
    c1, c2 = st.columns(2)
    with c1:
        amount = st.number_input("Valor de la clase*", min_value=0.0, step=1000.0, value=30000.0)
    with c2:
        now_time = datetime.now().time().replace(second=0, microsecond=0)
        class_time = st.time_input("Hora*", value=now_time)
    class_date = st.date_input("Fecha*", value=today)

    submitted = st.form_submit_button("Guardar clase")
    if submitted:
        if not cliente_input or not cliente_input.strip():
            st.error("Por favor, escribe o selecciona el nombre del cliente.")
        else:
            ts = datetime.combine(class_date, class_time)
            add_session(cliente_input, ts, amount)
            st.success(f"Clase guardada para **{normalize_client(cliente_input)}** el {class_date} a las {class_time} por **{fmt_money(amount)}**.")

# ---------- Clases del mes ----------
df_mes = fetch_sessions_between(start_dt, end_dt)
st.subheader(f"Clases del mes: {month_label_es(int(year), int(month))}")
if df_mes.empty:
    st.info("No hay registros en este mes.")
else:
    df_mes["Cliente"] = df_mes["client"].apply(normalize_client)
    vista = df_mes.copy()
    vista["N°"] = range(1, len(vista) + 1)
    vista["Valor"] = vista["amount"].apply(fmt_money)
    vista = vista[["N°", "Cliente", "fecha", "hora", "Valor", "id"]].rename(columns={
        "fecha": "Fecha",
        "hora": "Hora"
    })
    st.dataframe(vista[["N°","Cliente","Fecha","Hora","Valor"]], use_container_width=True)

    # Borrado amigable
    with st.expander("🧹 Borrar un registro"):
        opciones = []
        for _, r in vista.iterrows():
            label = f"N° {int(r['N°'])} — {r['Cliente']} — {r['Fecha']} {r['Hora']} — {r['Valor']}"
            opciones.append((label, int(r["id"])))
        if opciones:
            sel_label = st.selectbox("Selecciona el registro a borrar", [o[0] for o in opciones])
            label2id = {lbl: rid for lbl, rid in opciones}
            if st.button("Borrar seleccionado"):
                delete_session(label2id[sel_label])
                st.success("Registro borrado. Cambia el mes o actualiza para refrescar.")

# ---------- Resumen por persona (mes seleccionado) ----------
st.subheader("Resumen por persona (mes seleccionado)")
if df_mes.empty:
    st.info("No hay datos para resumir en este mes.")
else:
    resumen = (
        df_mes.groupby(df_mes["client"].apply(normalize_client))
        .agg(Clases=("id", "count"), Monto=("amount", "sum"))
        .reset_index().rename(columns={"index":"Cliente", "client":"Cliente"})
    )
    resumen = resumen.rename(columns={"client": "Cliente"})
    resumen["Cliente"] = resumen["client"] if "client" in resumen.columns else resumen["index"] if "index" in resumen.columns else resumen.columns[0]
    if "client" in resumen.columns: resumen = resumen.drop(columns=["client"])
    if "index" in resumen.columns: resumen = resumen.drop(columns=["index"])
    resumen = resumen.rename(columns={resumen.columns[0]: "Cliente"})
    resumen["Monto"] = resumen["Monto"].apply(fmt_money)

    estados = []
    for _, row in resumen.iterrows():
        m = get_monthly_payment(row["Cliente"], int(year), int(month))
        estados.append("Pagado" if m["paid"] == 1 else "Pendiente")
    resumen["Estado mes"] = estados

    total_global = df_mes["amount"].sum()
    total_clases_global = df_mes.shape[0]
    st.write(f"**Total de clases del mes:** {total_clases_global} | **Total a cobrar:** {fmt_money(total_global)}")
    st.dataframe(resumen[["Cliente","Clases","Monto","Estado mes"]], use_container_width=True)

# ---------- Actualizar estado de pago mensual ----------
st.markdown("### Actualizar estado de pago mensual")
all_clients = fetch_distinct_clients()
if not all_clients:
    st.info("Aún no hay clientes registrados.")
else:
    ccol1, ccol2, ccol3 = st.columns([2,1,1])
    with ccol1:
        cliente_pago = st.selectbox("Cliente", all_clients, key="cliente_pago_mensual")
    with ccol2:
        year_sel = st.number_input("Año del pago", min_value=2020, max_value=2100, value=int(year), step=1)
    with ccol3:
        mes_nombre_sel = st.selectbox("Mes del pago", MESES_ES, index=int(month)-1)
        month_sel = MES_A_NUM[mes_nombre_sel]

    start_x, end_x = month_range(int(year_sel), int(month_sel))
    df_cliente_mes = fetch_sessions_between(start_x, end_x)
    df_cliente_mes["client"] = df_cliente_mes["client"].apply(normalize_client)
    total_cliente_mes = df_cliente_mes[df_cliente_mes["client"] == normalize_client(cliente_pago)]["amount"].sum() if not df_cliente_mes.empty else 0.0
    st.write(f"**Total de {cliente_pago} en {month_label_es(int(year_sel), int(month_sel))}: {fmt_money(total_cliente_mes)}**")

    estado_actual_info = get_monthly_payment(cliente_pago, int(year_sel), int(month_sel))
    estado_actual = (estado_actual_info["paid"] == 1)
    pagado_mes_ui = st.checkbox("Marcar este mes como pagado", value=estado_actual, key="chk_pagado_mes")

    default_fecha_pago = date.today() if estado_actual_info["paid_on"] is None else date.fromisoformat(estado_actual_info["paid_on"])
    fecha_pago_ui = st.date_input("Fecha de pago (exacta)", value=default_fecha_pago)

    if st.button("Guardar estado de pago mensual"):
        set_monthly_payment(cliente_pago, int(year_sel), int(month_sel), pagado_mes_ui, fecha_pago_ui)
        st.success(
            f"Estado del mes para **{cliente_pago}** "
            f"({month_label_es(int(year_sel), int(month_sel))}) actualizado a: "
            f"{'Pagado' if pagado_mes_ui else 'Pendiente'}."
        )

# ---------- Historial por cliente ----------
st.markdown("### Historial de meses por cliente")
if not all_clients:
    st.info("Aún no hay clientes para mostrar historial.")
else:
    cliente_hist = st.selectbox("Cliente", all_clients, key="cliente_hist")
    agg_cli = sessions_agg_by_client_month(cliente_hist)
    if agg_cli.empty:
        st.info("Ese cliente todavía no tiene clases registradas.")
    else:
        hist = join_with_payments(agg_cli)
        hist["Mes (texto)"] = hist.apply(lambda r: month_label_es(int(r["Año"]), int(r["Mes"])), axis=1)
        hist["Monto"] = hist["Monto"].apply(fmt_money)
        hist = hist[["Mes (texto)", "Clases", "Monto", "Estado mes", "Fecha pago mes"]].sort_values(["Mes (texto)"])
        st.dataframe(hist, use_container_width=True)

        csv_bytes = hist.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label=f"⬇️ Descargar historial de {cliente_hist}",
            data=csv_bytes,
            file_name=f"historial_{cliente_hist.replace(' ', '_')}.csv",
            mime="text/csv"
        )
