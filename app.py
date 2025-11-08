# app.py — Supabase + Auth (OTP por email) + Tabs + Calendario

import os
import time
import calendar
from datetime import datetime, date

import pandas as pd
import streamlit as st
from supabase import create_client, Client

# ==============================
# Configuración de página
# ==============================
st.set_page_config(page_title="Entrenos - Registro y Resumen", page_icon="💪", layout="wide")

# ==============================
# Utilidades generales
# ==============================
MESES_ES = [
    "Enero","Febrero","Marzo","Abril","Mayo","Junio",
    "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"
]
MES_A_NUM = {name: i+1 for i, name in enumerate(MESES_ES)}

def month_label_es(y: int, m: int) -> str:
    return f"{MESES_ES[m-1]} {y}"

def month_range(year: int, month: int):
    start_dt = datetime(year, month, 1, 0, 0, 0)
    end_dt = datetime(year + (1 if month == 12 else 0), (1 if month == 12 else month + 1), 1, 0, 0, 0)
    return start_dt, end_dt

def fmt_money(x: float) -> str:
    try:
        return f"${x:,.0f}".replace(",", ".")  # estilo COP $30.000
    except Exception:
        return str(x)

def normalize_client(raw: str) -> str:
    if not raw:
        return ""
    s = " ".join(raw.split())
    return s.title()

# ==============================
# Supabase: cliente y Auth
# ==============================
# --- Login con Supabase Auth (email + OTP) + allow-list ---

def supa() -> Client:
    url = os.environ.get("SUPABASE_URL") or st.secrets["SUPABASE_URL"]
    key = os.environ.get("SUPABASE_ANON_KEY") or st.secrets["SUPABASE_ANON_KEY"]
    return create_client(url, key)

def supa_authd() -> Client:
    c = supa()
    sess = st.session_state.get("sb_session")
    if sess:
        c.postgrest.auth(sess["access_token"])
    return c

def logged_in() -> bool:
    return st.session_state.get("sb_session") is not None

def current_user_email() -> str | None:
    """Obtiene el email del usuario autenticado usando el access_token guardado."""
    try:
        sess = st.session_state.get("sb_session")
        if not sess:
            return None
        token = sess.get("access_token")
        if not token:
            return None
        u = supa().auth.get_user(token)
        return (u.user.email if u and u.user else None)
    except:
        return None


def is_current_admin() -> bool:
    """Devuelve True si el usuario autenticado está en admin_emails."""
    try:
        em = current_user_email()
        if not em:
            return False
        resp = (supa_authd().table("admin_emails")
                .select("email").eq("email", em).execute())
        return bool(resp.data)
    except:
        return False

def login_ui():
    st.markdown("### 🔐 Inicia sesión")
    email = st.text_input("Correo", placeholder="tu-correo@ejemplo.com")

    if st.button("Enviar código al correo"):
        if not email:
            st.error("Escribe tu correo.")
            st.stop()

        # 1) Pre-check en allow-list (RPC seguro)
        try:
            chk = supa().rpc("is_allowed", {"email_input": email.strip().lower()}).execute()
            allowed = bool(chk.data)
        except Exception as e:
            st.error(f"No se pudo validar el acceso: {e}")
            st.stop()

        if not allowed:
            st.error("Este correo no está autorizado. Pídele acceso al administrador.")
            st.stop()

        # 2) Enviar OTP (se crea usuario si no existe)
        try:
            supa().auth.sign_in_with_otp({"email": email.strip(), "should_create_user": True})
            st.session_state["pending_email"] = email.strip()
            st.success("Te enviamos un código. Revisa tu correo (principal/SPAM).")
        except Exception as e:
            st.error(f"No se pudo enviar el código: {e}")
            st.stop()

    if "pending_email" in st.session_state:
        code = st.text_input("Código recibido (6 dígitos)", max_chars=10)
        if st.button("Verificar código"):
            try:
                resp = supa().auth.verify_otp({
                    "email": st.session_state["pending_email"],
                    "token": code.strip(),
                    "type": "email"
                })
                st.session_state["sb_session"] = {
                    "access_token": resp.session.access_token,
                    "refresh_token": resp.session.refresh_token
                }
                st.success("Sesión iniciada ✅")
                time.sleep(0.4)
                st.rerun()
            except Exception as e:
                st.error(f"Código incorrecto o vencido: {e}")
                st.stop()

def require_login():
    if not logged_in():
        login_ui()
        st.stop()
    else:
        # Barra lateral: estado + logout
        user_em = current_user_email() or ""
        st.sidebar.success(f"Sesión: {user_em}")
        if st.sidebar.button("Cerrar sesión"):
            try:
                supa().auth.sign_out()
            except:
                pass
            st.session_state.pop("sb_session", None)
            st.rerun()


# Exige login antes de mostrar la app
require_login()

# ==============================
# Capa de datos (todas con supa_authd)
# ==============================
def add_session(client: str, ts: datetime, amount: float):
    client = normalize_client(client)
    supa_authd().table("sessions").insert({
        "client": client,
        "ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "amount": float(amount)
    }).execute()

def delete_session(row_id: int):
    supa_authd().table("sessions").delete().eq("id", row_id).execute()

def fetch_sessions_between(start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    res = (supa_authd().table("sessions")
           .select("*")
           .gte("ts", start_dt.strftime("%Y-%m-%d %H:%M:%S"))
           .lt("ts", end_dt.strftime("%Y-%m-%d %H:%M:%S"))
           .order("ts", desc=False)
           .execute())
    df = pd.DataFrame(res.data)
    if df.empty:
        return df
    df["client"] = df["client"].apply(normalize_client)
    df["ts"] = pd.to_datetime(df["ts"])
    df["fecha"] = df["ts"].dt.date
    df["hora"] = df["ts"].dt.strftime("%H:%M")
    return df[["id","client","fecha","hora","amount","ts"]]

def fetch_distinct_clients() -> list:
    res = supa_authd().table("sessions").select("client").execute()
    df = pd.DataFrame(res.data)
    if df.empty:
        return []
    df["client"] = df["client"].apply(normalize_client)
    return sorted(df["client"].dropna().unique().tolist())

def get_monthly_payment(client: str, year: int, month: int) -> dict:
    client = normalize_client(client)
    res = (supa_authd().table("monthly_payments")
           .select("paid, paid_on")
           .eq("client", client).eq("year", int(year)).eq("month", int(month))
           .execute())
    if not res.data:
        return {"paid": 0, "paid_on": None}
    row = res.data[0]
    return {"paid": 1 if row.get("paid") else 0, "paid_on": row.get("paid_on")}

def set_monthly_payment(client: str, year: int, month: int, paid: bool, paid_on: date | None):
    client = normalize_client(client)
    payload = {
        "client": client,
        "year": int(year),
        "month": int(month),
        "paid": bool(paid),
        "paid_on": paid_on.isoformat() if (paid and paid_on) else None
    }
    supa_authd().table("monthly_payments").upsert(payload, on_conflict="client,year,month").execute()

def sessions_agg_by_client_month(client: str | None = None) -> pd.DataFrame:
    res = supa_authd().table("sessions").select("client, ts, amount").execute()
    df = pd.DataFrame(res.data)
    if df.empty:
        return pd.DataFrame(columns=["Cliente","Año","Mes","Clases","Monto"])
    df["client"] = df["client"].apply(normalize_client)
    df["ts"] = pd.to_datetime(df["ts"])
    df["Año"] = df["ts"].dt.year
    df["Mes"] = df["ts"].dt.month
    if client:
        df = df[df["client"] == normalize_client(client)]
    agg = (df.groupby(["client","Año","Mes"])
             .agg(Clases=("ts","count"), Monto=("amount","sum"))
             .reset_index()
             .rename(columns={"client":"Cliente"}))
    return agg.sort_values(["Cliente","Año","Mes"])

def join_with_payments(agg_df: pd.DataFrame) -> pd.DataFrame:
    if agg_df.empty:
        return agg_df
    res = supa_authd().table("monthly_payments").select("client, year, month, paid, paid_on").execute()
    pays = pd.DataFrame(res.data)
    if pays.empty:
        agg_df["Estado mes"] = "Pendiente"
        agg_df["Fecha pago mes"] = "—"
        return agg_df
    pays = pays.rename(columns={"client":"Cliente","year":"Año","month":"Mes"})
    pays["Cliente"] = pays["Cliente"].apply(normalize_client)
    merged = pd.merge(agg_df, pays, on=["Cliente","Año","Mes"], how="left")
    merged["paid"] = merged["paid"].fillna(0).astype(int)
    merged["Estado mes"] = merged["paid"].map({0:"Pendiente", 1:"Pagado"})
    merged["Fecha pago mes"] = merged["paid_on"].fillna("—")
    return merged.drop(columns=["paid","paid_on"])

# ==============================
# UI
# ==============================
st.title("💪 Registro de Entrenos para Cobro")
st.caption("Registra clases y lleva el pago por **mes** y por **persona**. Persistencia en Supabase.")

# ===== Panel admin: gestionar lista blanca =====
with st.expander("🔑 Gestión de accesos (solo admin)"):
    if is_current_admin():
        st.info("Solo los correos de esta lista podrán iniciar sesión en la app.")
        col_a, col_b = st.columns([2,1])
        with col_a:
            nuevo = st.text_input("Agregar correo a la lista", placeholder="correo@ejemplo.com", key="add_allow")
        with col_b:
            if st.button("➕ Agregar"):
                if not nuevo:
                    st.warning("Escribe un correo.")
                else:
                    try:
                        supa_authd().table("allowed_emails").upsert({
                            "email": nuevo.strip().lower(),
                            "created_by": current_user_email()
                        }).execute()
                        st.success("Correo agregado a la lista blanca.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"No se pudo agregar: {e}")

        # Listado y borrado
        try:
            lista = (supa_authd()
                     .table("allowed_emails")
                     .select("email, created_at, created_by")
                     .order("created_at", desc=True)
                     .execute())
            df_allow = pd.DataFrame(lista.data)
            if df_allow.empty:
                st.caption("No hay correos permitidos aún.")
            else:
                st.dataframe(df_allow, use_container_width=True)
                borrar = st.text_input("Correo a borrar", placeholder="correo@ejemplo.com", key="del_allow")
                if st.button("🗑️ Borrar"):
                    try:
                        supa_authd().table("allowed_emails").delete().eq("email", borrar.strip().lower()).execute()
                        st.success("Correo eliminado de la lista.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"No se pudo eliminar: {e}")
        except Exception as e:
            st.error(f"No se pudo cargar la lista: {e}")
    else:
        st.caption("Debes ser administrador para ver/editar esta sección.")


# Sidebar: filtros de mes/año (mes por nombre)
today = date.today()
col1, col2 = st.sidebar.columns(2)
with col1:
    year = st.number_input("Año", min_value=2020, max_value=2100, value=today.year, step=1)
with col2:
    mes_nombre_sidebar = st.selectbox("Mes", MESES_ES, index=today.month - 1)
    month = MES_A_NUM[mes_nombre_sidebar]
start_dt, end_dt = month_range(int(year), int(month))

# Datos del mes
df_mes = fetch_sessions_between(start_dt, end_dt)

# Tabs
tab_registro, tab_calendario = st.tabs(["📋 Registro & Resumen", "📆 Calendario"])

# ---------- TAB 1: Registro & Resumen ----------
with tab_registro:
    # Formulario: Registrar una clase
    st.subheader("Registrar una clase")
    existing_clients = fetch_distinct_clients()
    SEL_NEW = "(Escribir nombre nuevo)"
    cliente_sel = st.selectbox("Cliente", [SEL_NEW] + existing_clients, index=0)
    cliente_input = st.text_input("Nombre del cliente*", placeholder="Ej: Juano Monroy") if cliente_sel == SEL_NEW else cliente_sel

    with st.form("form_registro", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            amount = st.number_input("Valor de la clase*", min_value=0.0, step=1000.0, value=30000.0)
        with c2:
            now_time = datetime.now().time().replace(second=0, microsecond=0)
            class_time = st.time_input("Hora*", value=now_time)
        class_date = st.date_input("Fecha*", value=today)

        if st.form_submit_button("Guardar clase"):
            if not cliente_input or not cliente_input.strip():
                st.error("Por favor, escribe o selecciona el nombre del cliente.")
            else:
                ts = datetime.combine(class_date, class_time)
                add_session(cliente_input, ts, amount)
                st.success(f"Clase guardada para **{normalize_client(cliente_input)}** el {class_date} a las {class_time} por **{fmt_money(amount)}**.")

    # Clases del mes
    st.subheader(f"Clases del mes: {month_label_es(int(year), int(month))}")
    if df_mes.empty:
        st.info("No hay registros en este mes.")
    else:
        vista = df_mes.copy()
        vista["Cliente"] = vista["client"].apply(normalize_client)
        vista["N°"] = range(1, len(vista) + 1)
        vista["Valor"] = vista["amount"].apply(fmt_money)
        vista = vista[["N°","Cliente","fecha","hora","Valor","id"]].rename(columns={"fecha":"Fecha","hora":"Hora"})
        st.dataframe(vista[["N°","Cliente","Fecha","Hora","Valor"]], use_container_width=True)

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
                    st.success("Registro borrado. Refresca (R) para actualizar la tabla.")

    # Resumen por persona
    st.subheader("Resumen por persona (mes seleccionado)")
    if df_mes.empty:
        st.info("No hay datos para resumir en este mes.")
    else:
        resumen = (
            df_mes.groupby(df_mes["client"].apply(normalize_client))
            .agg(Clases=("id","count"), Monto=("amount","sum"))
            .reset_index().rename(columns={"index":"Cliente", 0:"Cliente"})
        )
        if "client" in resumen.columns:
            resumen = resumen.rename(columns={"client":"Cliente"})
        elif "index" in resumen.columns:
            resumen = resumen.rename(columns={"index":"Cliente"})
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

    # Actualizar pago mensual
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
        total_cliente_mes = 0.0
        if not df_cliente_mes.empty:
            df_cliente_mes["client"] = df_cliente_mes["client"].apply(normalize_client)
            total_cliente_mes = df_cliente_mes[df_cliente_mes["client"] == normalize_client(cliente_pago)]["amount"].sum()
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

    # Historial por cliente
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
            hist = hist[["Mes (texto)","Clases","Monto","Estado mes","Fecha pago mes"]].sort_values(["Mes (texto)"])
            st.dataframe(hist, use_container_width=True)

            csv_bytes = hist.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                label=f"⬇️ Descargar historial de {cliente_hist}",
                data=csv_bytes,
                file_name=f"historial_{cliente_hist.replace(' ', '_')}.csv",
                mime="text/csv"
            )

# ---------- TAB 2: Calendario ----------
with tab_calendario:
    st.subheader(f"Calendario de {MESES_ES[int(month)-1]} {int(year)}")
    if df_mes.empty:
        st.info("No hay clases en este mes.")
    else:
        # Prepara mapa {fecha -> lista de clases} (robusto)
        df_cal = df_mes.sort_values(["fecha", "hora"]).copy()
        clases_por_dia = {}
        for f, g in df_cal.groupby("fecha"):
            clases_por_dia[f] = [
                {"client": r["client"], "hora": r["hora"], "amount": r["amount"]}
                for _, r in g.iterrows()
            ]

        # Lunes a domingo
        calendar.setfirstweekday(calendar.MONDAY)
        weeks = calendar.monthcalendar(int(year), int(month))

        # Encabezado
        cols = st.columns(7)
        for i, nombre in enumerate(["Lun","Mar","Mié","Jue","Vie","Sáb","Dom"]):
            with cols[i]:
                st.markdown(f"**{nombre}**")

        # Celdas del mes
        for week in weeks:
            cols = st.columns(7)
            for i, day in enumerate(week):
                with cols[i]:
                    if day == 0:
                        st.write("")  # celda vacía
                        continue
                    f = date(int(year), int(month), int(day))
                    st.markdown(f"### {day}")
                    if f in clases_por_dia:
                        for item in clases_por_dia[f]:
                            cli = normalize_client(item["client"])
                            hora = item["hora"]
                            val = fmt_money(item["amount"])
                            st.markdown(f"- **{hora}** · {cli} · {val}")
                        total_dia = sum(x["amount"] for x in clases_por_dia[f] if "amount" in x and pd.notna(x["amount"]))
                        st.caption(f"Total del día: {fmt_money(total_dia)}")
                    else:
                        st.caption("— sin clases —")
