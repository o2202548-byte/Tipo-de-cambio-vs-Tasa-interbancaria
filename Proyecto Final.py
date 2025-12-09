import os
import sys
import subprocess
import requests
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import urllib
from sqlalchemy import create_engine, text
import streamlit as st

# Configuracion principal banxico

TOKEN = "f53ab21f2bb4688face839292514375d74bbfe726d4d8a36ff7637a633032f87"
SERIES = "SF43718,SF46410"
NOMBRES_SERIES = {"SF43718": "cambio", "SF46410": "tasa"}

FECHA_INICIO = "2010-01-01"
FECHA_FIN = datetime.now().strftime("%Y-%m-%d")
CARPETA = "datos_banxico"
if not os.path.exists(CARPETA):
    os.makedirs(CARPETA)

URL_API = f"https://www.banxico.org.mx/SieAPIRest/service/v1/series/{SERIES}/datos/{FECHA_INICIO}/{FECHA_FIN}?token={TOKEN}"

# Conexion sql server

SERVIDOR = r'DRIPPY\SQLEXPRESS'
BASE_DATOS = 'banxico'

params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVIDOR};"
    f"DATABASE={BASE_DATOS};"
    f"Trusted_Connection=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# Funciones sql

def asegurar_series(engine, nombres_series):
    """Inserta las series en la tabla 'series' si no existen."""
    with engine.begin() as conn:
        for nombre, desc in nombres_series.items():
            resultado = conn.execute(
                text("SELECT COUNT(*) FROM series WHERE nombre=:nombre"),
                {"nombre": nombre}
            ).fetchone()
            if resultado[0] == 0:
                conn.execute(
                    text("INSERT INTO series (nombre, descripcion) VALUES (:nombre, :desc)"),
                    {"nombre": nombre, "desc": desc}
                )
                print(f"Serie '{nombre}' insertada automáticamente.")
            else:
                print(f"Serie '{nombre}' ya existe.")

# Funciones 

def bajar_datos(url):
    try:
        print(f"Conectando a API Banxico para el periodo {FECHA_INICIO} a {FECHA_FIN}...")
        r = requests.get(url)
        if r.status_code == 200:
            return r.json()
        else:
            print(f"[Error API] Status Code: {r.status_code}")
            return None
    except Exception as e:
        print(f"[Error Conexión] {e}")
        return None

def guardar_csv_y_sql(json_datos, carpeta, nombres, engine=None):
    """Procesa los datos del JSON, guarda CSV y carga en SQL Server."""
    series_data = json_datos.get('bmx', {}).get('series', [])
    if not series_data:
        print("[Error] El JSON no contiene series.")
        return None

    dfs = []

    for serie in series_data:
        id_serie_nombre = serie.get('idSerie')
        observaciones = serie.get('datos')
        if not id_serie_nombre or not observaciones:
            continue
        nombre = nombres.get(id_serie_nombre, f"serie_{id_serie_nombre}")

        df = pd.DataFrame(observaciones)
        df = df.rename(columns={"fecha": "Fecha", "dato": nombre})
        df[nombre] = pd.to_numeric(df[nombre], errors="coerce")
        df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce", dayfirst=True).dt.date
        df = df.dropna(subset=[nombre, "Fecha"])

        path_csv = os.path.join(carpeta, f"{nombre}.csv")
        df.to_csv(path_csv, index=False)
        print(f"{nombre}: {len(df)} registros guardados en {path_csv}")

        if engine is not None:
            with engine.begin() as conn:
                resultado = conn.execute(
                    text("SELECT id FROM series WHERE nombre=:nombre"),
                    {"nombre": nombre}
                ).fetchone()
                if resultado:
                    id_serie = resultado[0]
                    valores_list = [(id_serie, row['Fecha'], row[nombre]) for _, row in df.iterrows()]
                    conn.execute(
                        text("INSERT INTO valores (id_serie, fecha, valor) VALUES (:id, :fecha, :valor)"),
                        [{"id": v[0], "fecha": v[1], "valor": v[2]} for v in valores_list]
                    )
                    print(f"{nombre} cargado en SQL Server.")
                else:
                    print(f"[Error] No se encontró id de serie para {nombre}.")

        dfs.append(df)

    if len(dfs) >= 2:
        df_combinado = dfs[0]
        for df2 in dfs[1:]:
            df_combinado = pd.merge(df_combinado, df2, on="Fecha", how="inner")
        path_combinado = os.path.join(carpeta, "cambio_vs_tasa.csv")
        df_combinado.to_csv(path_combinado, index=False)
        print(f"CSV combinado guardado en {path_combinado}")
        return df_combinado
    return None

# Dashboards de streamlit

def dashboard():
    st.set_page_config(page_title="Banxico Dashboard", layout="wide")
    st.title("Dashboard Financiero Banxico")
    st.markdown("Visualización de la evolución del Tipo de Cambio y la Tasa Interbancaria.")

    def cargar_csv(nombre):
        ruta = os.path.join(CARPETA, nombre)
        if os.path.exists(ruta):
            df = pd.read_csv(ruta)
            df[df.columns[1]] = pd.to_numeric(df[df.columns[1]], errors="coerce")
            df[df.columns[0]] = pd.to_datetime(df[df.columns[0]], errors="coerce", dayfirst=True).dt.date
            df = df.dropna(subset=[df.columns[0], df.columns[1]])
            return df
        return None

    def graficar(df1, df2, titulo, label1, label2):
        fig, ax = plt.subplots(figsize=(12,5))
        ax.plot(df1.iloc[:,0], df1.iloc[:,1], marker="o", label=label1)
        ax.plot(df2.iloc[:,0], df2.iloc[:,1], marker="o", label=label2)
        ax.set_title(titulo)
        ax.set_xlabel("Fecha")
        ax.set_ylabel("Valor")
        ax.legend()
        ax.grid(True)
        st.pyplot(fig)
        plt.close(fig)

    cambio = cargar_csv("cambio.csv")
    tasa = cargar_csv("tasa.csv")
    combinado = cargar_csv("cambio_vs_tasa.csv")

    st.header("1️. Evolución histórica")
    if cambio is not None and tasa is not None:
        graficar(cambio, tasa, "Tipo de Cambio vs Tasa Interbancaria", "Tipo de Cambio", "Tasa Interbancaria")

    st.header("2️. Variación diaria")
    if combinado is not None:
        combinado_diff = combinado.copy()
        combinado_diff["cambio_var"] = combinado_diff["cambio"].pct_change() * 100
        combinado_diff["tasa_var"] = combinado_diff["tasa"].pct_change() * 100

        fig, ax = plt.subplots(figsize=(12,5))
        ax.plot(combinado_diff["Fecha"], combinado_diff["cambio_var"], marker="o", label="Tipo de Cambio %")
        ax.plot(combinado_diff["Fecha"], combinado_diff["tasa_var"], marker="o", label="Tasa %")
        ax.set_title("Variación porcentual diaria")
        ax.set_xlabel("Fecha")
        ax.set_ylabel("%")
        ax.legend()
        ax.grid(True)
        st.pyplot(fig)
        plt.close(fig)

    st.header("3️. Cambio acumulado")
    if combinado is not None:
        combinado["cambio_acum"] = combinado["cambio"] - combinado.iloc[0]["cambio"]
        combinado["tasa_acum"] = combinado["tasa"] - combinado.iloc[0]["tasa"]

        fig, ax = plt.subplots(figsize=(12,5))
        ax.plot(combinado["Fecha"], combinado["cambio_acum"], marker="o", label="Acumulado Tipo de Cambio")
        ax.plot(combinado["Fecha"], combinado["tasa_acum"], marker="o", label="Acumulado Tasa")
        ax.set_title("Cambio acumulado desde inicio")
        ax.set_xlabel("Fecha")
        ax.set_ylabel("Valor acumulado")
        ax.legend()
        ax.grid(True)
        st.pyplot(fig)
        plt.close(fig)

    st.header("Tablas de estadísticas")
    if combinado is not None:
        st.markdown("**Resumen estadístico de las series combinadas:**")
        st.write(combinado.describe())
        st.markdown("**Últimos 5 registros:**")
        st.write(combinado.tail())

# Ejecucion del main

if __name__ == "__main__":
    asegurar_series(engine, {"cambio": "Tipo de Cambio MXN/USD", "tasa": "Tasa Interbancaria promedio diaria"})

    datos = bajar_datos(URL_API)
    if datos:
        guardar_csv_y_sql(datos, CARPETA, NOMBRES_SERIES, engine)

    if os.environ.get("RUNNING_STREAMLIT") != "1":
        archivo_actual = os.path.abspath(__file__)
        env = os.environ.copy()
        env["RUNNING_STREAMLIT"] = "1"
        print("Iniciando Streamlit...")
        subprocess.run([sys.executable, "-m", "streamlit", "run", archivo_actual], env=env)
    else:
        dashboard()
