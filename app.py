from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel
from typing import Optional
import os
from datetime import datetime
import calendar
from contextlib import contextmanager
import tempfile

app = FastAPI(title="Sistema de Viáticos ATE")

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
EXPORTS_DIR = os.path.join(tempfile.gettempdir(), "viaticos_exports")
os.makedirs(EXPORTS_DIR, exist_ok=True)

static_dir = os.path.join(BASE_DIR, "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

# ── DATABASE: PostgreSQL en Railway, SQLite en local ─────────────────────────

DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

USE_PG = bool(DATABASE_URL)
PH     = "%s" if USE_PG else "?"

if USE_PG:
    import psycopg2, psycopg2.extras

    @contextmanager
    def get_db():
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _rows(cur):
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def _row(cur):
        if not cur.description:
            return None
        cols = [d[0] for d in cur.description]
        r = cur.fetchone()
        return dict(zip(cols, r)) if r else None

    def _insert(conn, sql, params):
        cur = conn.cursor()
        cur.execute(sql + " RETURNING id", params)
        return cur.fetchone()[0]

    def _year_filter(col):  return f"EXTRACT(YEAR  FROM {col}) = {PH}"
    def _month_filter(col): return f"EXTRACT(MONTH FROM {col}) = {PH}"
    def _yp(v): return v          # año/mes como int
    def _mp(v): return v

    def init_db():
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS agentes (
                    id SERIAL PRIMARY KEY, nombre TEXT NOT NULL,
                    cuit TEXT, cbu TEXT, banco TEXT, alias TEXT,
                    tope_mensual REAL DEFAULT 0, activo INTEGER DEFAULT 1,
                    creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS categorias (
                    id SERIAL PRIMARY KEY, nombre TEXT NOT NULL UNIQUE, activa INTEGER DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS tickets (
                    id SERIAL PRIMARY KEY,
                    agente_id INTEGER NOT NULL REFERENCES agentes(id),
                    fecha_gasto DATE NOT NULL, categoria_id INTEGER REFERENCES categorias(id),
                    comprobante TEXT, descripcion TEXT, valor REAL NOT NULL,
                    estado TEXT DEFAULT 'pendiente', motivo_rechazo TEXT, valor_aprobado REAL,
                    creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    revisado_en TIMESTAMP, revisado_por TEXT
                );
                CREATE TABLE IF NOT EXISTS periodos (
                    id SERIAL PRIMARY KEY,
                    agente_id INTEGER NOT NULL REFERENCES agentes(id),
                    anio INTEGER NOT NULL, mes INTEGER NOT NULL,
                    tope_override REAL, cerrado INTEGER DEFAULT 0,
                    UNIQUE(agente_id, anio, mes)
                );
            """)
            cur.execute("""
                INSERT INTO categorias (nombre)
                VALUES ('Transporte'),('Alojamiento'),('Alimentación'),
                       ('Combustible'),('Peajes'),('Materiales'),('Otro')
                ON CONFLICT (nombre) DO NOTHING;
            """)

else:
    import sqlite3
    DB_PATH = os.path.join(BASE_DIR, "viaticos.db")

    @contextmanager
    def get_db():
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _rows(cur): return [dict(r) for r in cur.fetchall()]
    def _row(cur):
        r = cur.fetchone()
        return dict(r) if r else None

    def _insert(conn, sql, params):
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.lastrowid

    def _year_filter(col):  return f"strftime('%Y', {col}) = {PH}"
    def _month_filter(col): return f"strftime('%m', {col}) = {PH}"
    def _yp(v): return str(v)
    def _mp(v): return f"{v:02d}"

    def init_db():
        with get_db() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS agentes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL,
                    cuit TEXT, cbu TEXT, banco TEXT, alias TEXT,
                    tope_mensual REAL DEFAULT 0, activo INTEGER DEFAULT 1,
                    creado_en TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS categorias (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nombre TEXT NOT NULL UNIQUE, activa INTEGER DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS tickets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    agente_id INTEGER NOT NULL REFERENCES agentes(id),
                    fecha_gasto TEXT NOT NULL, categoria_id INTEGER REFERENCES categorias(id),
                    comprobante TEXT, descripcion TEXT, valor REAL NOT NULL,
                    estado TEXT DEFAULT 'pendiente', motivo_rechazo TEXT, valor_aprobado REAL,
                    creado_en TEXT DEFAULT CURRENT_TIMESTAMP, revisado_en TEXT, revisado_por TEXT
                );
                CREATE TABLE IF NOT EXISTS periodos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    agente_id INTEGER NOT NULL REFERENCES agentes(id),
                    anio INTEGER NOT NULL, mes INTEGER NOT NULL,
                    tope_override REAL, cerrado INTEGER DEFAULT 0,
                    UNIQUE(agente_id, anio, mes)
                );
                INSERT OR IGNORE INTO categorias (nombre) VALUES
                    ('Transporte'),('Alojamiento'),('Alimentación'),
                    ('Combustible'),('Peajes'),('Materiales'),('Otro');
            """)

init_db()

# ── MODELS ────────────────────────────────────────────────────────────────────

class AgenteCreate(BaseModel):
    nombre: str
    cuit: Optional[str] = None
    cbu: Optional[str] = None
    banco: Optional[str] = None
    alias: Optional[str] = None
    tope_mensual: float = 0

class AgenteUpdate(BaseModel):
    nombre: Optional[str] = None
    cuit: Optional[str] = None
    cbu: Optional[str] = None
    banco: Optional[str] = None
    alias: Optional[str] = None
    tope_mensual: Optional[float] = None
    activo: Optional[int] = None

class TicketCreate(BaseModel):
    agente_id: int
    fecha_gasto: str
    categoria_id: Optional[int] = None
    comprobante: Optional[str] = None
    descripcion: Optional[str] = None
    valor: float

class TicketRevision(BaseModel):
    estado: str
    motivo_rechazo: Optional[str] = None
    valor_aprobado: Optional[float] = None
    revisado_por: Optional[str] = None

class PeriodoTope(BaseModel):
    agente_id: int
    anio: int
    mes: int
    tope_override: Optional[float] = None

# ── AGENTES ───────────────────────────────────────────────────────────────────

@app.get("/api/agentes")
def listar_agentes(solo_activos: bool = True):
    with get_db() as conn:
        cur = conn.cursor()
        q = "SELECT * FROM agentes" + (" WHERE activo=1" if solo_activos else "") + " ORDER BY nombre"
        cur.execute(q)
        return _rows(cur)

@app.post("/api/agentes")
def crear_agente(data: AgenteCreate):
    with get_db() as conn:
        new_id = _insert(conn,
            f"INSERT INTO agentes (nombre,cuit,cbu,banco,alias,tope_mensual) VALUES ({PH},{PH},{PH},{PH},{PH},{PH})",
            (data.nombre, data.cuit, data.cbu, data.banco, data.alias, data.tope_mensual))
        return {"id": new_id, "mensaje": "Agente creado"}

@app.put("/api/agentes/{agente_id}")
def actualizar_agente(agente_id: int, data: AgenteUpdate):
    campos = {k: v for k, v in data.dict().items() if v is not None}
    if not campos:
        raise HTTPException(400, "Sin datos para actualizar")
    with get_db() as conn:
        cur = conn.cursor()
        sets = ", ".join(f"{k}={PH}" for k in campos)
        cur.execute(f"UPDATE agentes SET {sets} WHERE id={PH}", (*campos.values(), agente_id))
    return {"mensaje": "Actualizado"}

@app.get("/api/agentes/{agente_id}")
def obtener_agente(agente_id: int):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM agentes WHERE id={PH}", (agente_id,))
        row = _row(cur)
        if not row:
            raise HTTPException(404, "Agente no encontrado")
        return row

# ── TICKETS ───────────────────────────────────────────────────────────────────

@app.get("/api/tickets")
def listar_tickets(agente_id: Optional[int] = None, estado: Optional[str] = None,
                   anio: Optional[int] = None, mes: Optional[int] = None):
    with get_db() as conn:
        cur = conn.cursor()
        q = """SELECT t.*, a.nombre as agente_nombre, c.nombre as categoria_nombre
               FROM tickets t
               LEFT JOIN agentes a ON t.agente_id = a.id
               LEFT JOIN categorias c ON t.categoria_id = c.id
               WHERE 1=1"""
        params = []
        if agente_id:
            q += f" AND t.agente_id={PH}"; params.append(agente_id)
        if estado:
            q += f" AND t.estado={PH}"; params.append(estado)
        if anio:
            q += f" AND {_year_filter('t.fecha_gasto')}"; params.append(_yp(anio))
        if mes:
            q += f" AND {_month_filter('t.fecha_gasto')}"; params.append(_mp(mes))
        q += " ORDER BY t.fecha_gasto DESC"
        cur.execute(q, params)
        rows = _rows(cur)
        for r in rows:
            if r.get("fecha_gasto") and not isinstance(r["fecha_gasto"], str):
                r["fecha_gasto"] = r["fecha_gasto"].strftime("%Y-%m-%d")
        return rows

@app.post("/api/tickets")
def crear_ticket(data: TicketCreate):
    with get_db() as conn:
        new_id = _insert(conn,
            f"INSERT INTO tickets (agente_id,fecha_gasto,categoria_id,comprobante,descripcion,valor) VALUES ({PH},{PH},{PH},{PH},{PH},{PH})",
            (data.agente_id, data.fecha_gasto, data.categoria_id, data.comprobante, data.descripcion, data.valor))
        return {"id": new_id, "mensaje": "Ticket creado"}

@app.put("/api/tickets/{ticket_id}/revision")
def revisar_ticket(ticket_id: int, data: TicketRevision):
    if data.estado not in ("aprobado", "rechazado", "debito_parcial"):
        raise HTTPException(400, "Estado inválido")
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM tickets WHERE id={PH}", (ticket_id,))
        ticket = _row(cur)
        if not ticket:
            raise HTTPException(404, "Ticket no encontrado")
        valor_aprobado = (data.valor_aprobado if data.estado == "debito_parcial"
                          else ticket["valor"] if data.estado == "aprobado" else 0)
        cur.execute(f"""UPDATE tickets SET estado={PH}, motivo_rechazo={PH}, valor_aprobado={PH},
                    revisado_en=CURRENT_TIMESTAMP, revisado_por={PH} WHERE id={PH}""",
                    (data.estado, data.motivo_rechazo, valor_aprobado, data.revisado_por, ticket_id))
    return {"mensaje": "Ticket revisado"}

@app.delete("/api/tickets/{ticket_id}")
def eliminar_ticket(ticket_id: int):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM tickets WHERE id={PH}", (ticket_id,))
    return {"mensaje": "Ticket eliminado"}

# ── CATEGORÍAS ────────────────────────────────────────────────────────────────

@app.get("/api/categorias")
def listar_categorias():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM categorias WHERE activa=1 ORDER BY nombre")
        return _rows(cur)

# ── RESUMEN ───────────────────────────────────────────────────────────────────

@app.get("/api/resumen")
def resumen(anio: int, mes: int):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT a.id, a.nombre, a.tope_mensual, a.cbu, a.banco, a.alias, a.cuit,
                COALESCE(SUM(CASE WHEN t.estado='pendiente'      THEN t.valor          END), 0) as total_pendiente,
                COALESCE(SUM(CASE WHEN t.estado='aprobado'       THEN t.valor_aprobado END), 0) as total_aprobado,
                COALESCE(SUM(CASE WHEN t.estado='debito_parcial' THEN t.valor_aprobado END), 0) as total_debito,
                COALESCE(SUM(CASE WHEN t.estado='rechazado'      THEN t.valor          END), 0) as total_rechazado,
                COUNT(CASE WHEN t.estado='pendiente' THEN 1 END)                                as cant_pendientes,
                COUNT(CASE WHEN t.estado IN ('aprobado','debito_parcial') THEN 1 END)           as cant_aprobados
            FROM agentes a
            LEFT JOIN tickets t ON a.id = t.agente_id
                AND {_year_filter('t.fecha_gasto')}
                AND {_month_filter('t.fecha_gasto')}
            WHERE a.activo = 1
            GROUP BY a.id, a.nombre, a.tope_mensual, a.cbu, a.banco, a.alias, a.cuit
            ORDER BY a.nombre
        """, (_yp(anio), _mp(mes)))
        rows = _rows(cur)

        result = []
        for d in rows:
            cur.execute(f"SELECT tope_override FROM periodos WHERE agente_id={PH} AND anio={PH} AND mes={PH}",
                        (d["id"], anio, mes))
            ov = _row(cur)
            tope = d["tope_mensual"]
            if ov and ov.get("tope_override") is not None:
                tope = ov["tope_override"]
            subtotal = d["total_aprobado"] + d["total_debito"]
            d["tope_efectivo"] = tope
            d["a_transferir"]  = min(subtotal, tope) if tope > 0 else subtotal
            d["excedente"]     = max(0, subtotal - tope) if tope > 0 else 0
            result.append(d)
        return result

@app.get("/api/resumen/semanal")
def resumen_semanal(anio: int, mes: int):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT t.*, a.nombre as agente_nombre
            FROM tickets t JOIN agentes a ON t.agente_id = a.id
            WHERE {_year_filter('t.fecha_gasto')} AND {_month_filter('t.fecha_gasto')}
              AND t.estado IN ('aprobado','debito_parcial')
            ORDER BY t.fecha_gasto
        """, (_yp(anio), _mp(mes)))
        tickets = _rows(cur)

    semanas = {1: [], 2: [], 3: [], 4: [], 5: []}
    for t in tickets:
        fg = t["fecha_gasto"]
        if not isinstance(fg, str):
            fg = fg.strftime("%Y-%m-%d")
            t["fecha_gasto"] = fg
        day    = int(fg.split("-")[2])
        semana = min((day - 1) // 7 + 1, 5)
        semanas[semana].append(t)

    return [{"semana": s, "tickets": items, "total": sum(i.get("valor_aprobado") or 0 for i in items)}
            for s, items in semanas.items() if items]

# ── TOPES ─────────────────────────────────────────────────────────────────────

@app.post("/api/topes")
def configurar_tope(data: PeriodoTope):
    with get_db() as conn:
        cur = conn.cursor()
        if USE_PG:
            cur.execute("""
                INSERT INTO periodos (agente_id, anio, mes, tope_override) VALUES (%s,%s,%s,%s)
                ON CONFLICT (agente_id, anio, mes) DO UPDATE SET tope_override = EXCLUDED.tope_override
            """, (data.agente_id, data.anio, data.mes, data.tope_override))
        else:
            cur.execute("""
                INSERT INTO periodos (agente_id, anio, mes, tope_override) VALUES (?,?,?,?)
                ON CONFLICT(agente_id,anio,mes) DO UPDATE SET tope_override=excluded.tope_override
            """, (data.agente_id, data.anio, data.mes, data.tope_override))
    return {"mensaje": "Tope configurado"}

# ── EXPORTACIÓN EXCEL ─────────────────────────────────────────────────────────

@app.get("/api/exportar/excel")
def exportar_excel(anio: int, mes: int):
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter

    resumen_data = resumen(anio, mes)
    nombre_mes   = calendar.month_name[mes]
    wb = openpyxl.Workbook()

    # Hoja 1: Transferencias
    ws1 = wb.active; ws1.title = "Transferencias"
    ws1.merge_cells("A1:H1")
    ws1["A1"] = f"SINDICATO ATE — REINTEGRO DE VIÁTICOS {nombre_mes.upper()} {anio}"
    ws1["A1"].font = Font(bold=True, size=14, color="FFFFFF")
    ws1["A1"].fill = PatternFill("solid", fgColor="1a3a5c")
    ws1["A1"].alignment = Alignment(horizontal="center", vertical="center")
    ws1.row_dimensions[1].height = 30

    hdrs = ["Agente","CUIT","CBU","Banco / Alias","Total Presentado","Total Aprobado","Tope Mensual","A TRANSFERIR"]
    for c, h in enumerate(hdrs, 1):
        cell = ws1.cell(row=2, column=c, value=h)
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor="2d6a9f")
        cell.alignment = Alignment(horizontal="center")

    total_transf = 0
    for ri, ag in enumerate(resumen_data, 3):
        tp = ag["total_aprobado"] + ag["total_debito"] + ag["total_pendiente"]
        vals = [ag["nombre"], ag.get("cuit",""), ag.get("cbu",""),
                ag.get("banco","") or ag.get("alias",""),
                tp, ag["total_aprobado"]+ag["total_debito"], ag["tope_efectivo"], ag["a_transferir"]]
        for c, v in enumerate(vals, 1):
            cell = ws1.cell(row=ri, column=c, value=v)
            if c in (5,6,7,8): cell.number_format = '"$"#,##0.00'
            if ri % 2 == 0:    cell.fill = PatternFill("solid", fgColor="EBF3FB")
        total_transf += ag["a_transferir"]

    tr = len(resumen_data) + 3
    ws1.cell(row=tr, column=1, value="TOTAL").font = Font(bold=True)
    ct = ws1.cell(row=tr, column=8, value=total_transf)
    ct.font = Font(bold=True, color="FFFFFF"); ct.fill = PatternFill("solid", fgColor="1a3a5c")
    ct.number_format = '"$"#,##0.00'
    for i, w in enumerate([30,16,26,20,16,16,14,16], 1):
        ws1.column_dimensions[get_column_letter(i)].width = w

    # Hoja 2: Detalle de Tickets
    ws2 = wb.create_sheet("Detalle de Tickets")
    ws2.merge_cells("A1:H1")
    ws2["A1"] = f"DETALLE — {nombre_mes.upper()} {anio}"
    ws2["A1"].font = Font(bold=True, size=12, color="FFFFFF")
    ws2["A1"].fill = PatternFill("solid", fgColor="1a3a5c")
    ws2["A1"].alignment = Alignment(horizontal="center", vertical="center")
    for c, h in enumerate(["Agente","Fecha","Categoría","Comprobante","Descripción","Valor","Estado","Aprobado"], 1):
        cell = ws2.cell(row=2, column=c, value=h)
        cell.font = Font(bold=True, color="FFFFFF"); cell.fill = PatternFill("solid", fgColor="2d6a9f")

    colores = {"aprobado":"D4EDDA","rechazado":"F8D7DA","debito_parcial":"FFF3CD","pendiente":"FFFFFF"}
    for ri, t in enumerate(listar_tickets(anio=anio, mes=mes), 3):
        color = colores.get(t["estado"], "FFFFFF")
        for c, v in enumerate([t["agente_nombre"],t["fecha_gasto"],t.get("categoria_nombre",""),
                                t.get("comprobante",""),t.get("descripcion",""),t["valor"],
                                t["estado"].upper(),t.get("valor_aprobado") or ""], 1):
            cell = ws2.cell(row=ri, column=c, value=v)
            cell.fill = PatternFill("solid", fgColor=color)
            if c in (6,8): cell.number_format = '"$"#,##0.00'
    for col, w in zip("ABCDEFGH",[28,12,16,16,30,14,14,14]):
        ws2.column_dimensions[col].width = w

    # Hoja 3: Resumen Semanal
    ws3 = wb.create_sheet("Resumen Semanal")
    ws3.merge_cells("A1:D1")
    ws3["A1"] = f"RESUMEN SEMANAL — {nombre_mes.upper()} {anio}"
    ws3["A1"].font = Font(bold=True, size=12, color="FFFFFF")
    ws3["A1"].fill = PatternFill("solid", fgColor="1a3a5c")
    ws3["A1"].alignment = Alignment(horizontal="center", vertical="center")
    row = 2
    for s in resumen_semanal(anio, mes):
        ws3.cell(row=row, column=1, value=f"Semana {s['semana']}").font = Font(bold=True, color="FFFFFF")
        ws3.cell(row=row, column=1).fill = PatternFill("solid", fgColor="2d6a9f")
        ws3.merge_cells(f"A{row}:D{row}"); row += 1
        for c, h in enumerate(["Agente","Fecha","Descripción","Aprobado"], 1):
            ws3.cell(row=row, column=c, value=h).font = Font(bold=True)
        row += 1
        for t in s["tickets"]:
            ws3.cell(row=row,column=1,value=t["agente_nombre"])
            ws3.cell(row=row,column=2,value=t["fecha_gasto"])
            ws3.cell(row=row,column=3,value=t.get("descripcion",""))
            ws3.cell(row=row,column=4,value=t.get("valor_aprobado",0)).number_format = '"$"#,##0.00'
            row += 1
        ws3.cell(row=row,column=3,value="TOTAL SEMANA").font = Font(bold=True)
        ws3.cell(row=row,column=4,value=s["total"]).font = Font(bold=True)
        ws3.cell(row=row,column=4).number_format = '"$"#,##0.00'
        row += 2
    for col in "ABCD": ws3.column_dimensions[col].width = 28

    filename = f"viaticos_ATE_{anio}_{mes:02d}.xlsx"
    path = os.path.join(EXPORTS_DIR, filename)
    wb.save(path)
    return FileResponse(path, filename=filename,
                        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ── EXPORTACIÓN PDF ───────────────────────────────────────────────────────────

@app.get("/api/exportar/pdf")
def exportar_pdf(anio: int, mes: int):
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib import colors
    from reportlab.lib.units import cm
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_CENTER

    resumen_data = resumen(anio, mes)
    nombre_mes   = calendar.month_name[mes]
    filename     = f"reintegros_ATE_{anio}_{mes:02d}.pdf"
    path         = os.path.join(EXPORTS_DIR, filename)

    doc = SimpleDocTemplate(path, pagesize=landscape(A4),
                            leftMargin=1.5*cm, rightMargin=1.5*cm, topMargin=2*cm, bottomMargin=2*cm)
    AZ  = colors.HexColor("#1a3a5c")
    AM  = colors.HexColor("#2d6a9f")
    AC  = colors.HexColor("#EBF3FB")
    ts  = ParagraphStyle("t", fontSize=18, textColor=colors.white, alignment=TA_CENTER, fontName="Helvetica-Bold")
    ss  = ParagraphStyle("s", fontSize=10, textColor=colors.white, alignment=TA_CENTER, fontName="Helvetica")

    ht = Table([[Paragraph("SINDICATO ATE", ts)],
                [Paragraph(f"REINTEGRO DE VIÁTICOS — {nombre_mes.upper()} {anio}", ss)],
                [Paragraph(f"Generado el {datetime.now().strftime('%d/%m/%Y %H:%M')}", ss)]],
               colWidths=[26*cm])
    ht.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),AZ),("ROWPADDING",(0,0),(-1,-1),8),
                             ("TOPPADDING",(0,0),(-1,0),14),("BOTTOMPADDING",(0,-1),(-1,-1),14)]))

    hdrs = ["Agente","CUIT","CBU","Banco/Alias","Total Presentado","Aprobado","Tope","A TRANSFERIR"]
    td = [hdrs]; total = 0
    for ag in resumen_data:
        tp = ag["total_aprobado"] + ag["total_debito"] + ag["total_pendiente"]
        ta = ag["total_aprobado"] + ag["total_debito"]
        td.append([ag["nombre"], ag.get("cuit") or "-", ag.get("cbu") or "-",
                   ag.get("banco") or ag.get("alias") or "-",
                   f"${tp:,.2f}", f"${ta:,.2f}", f"${ag['tope_efectivo']:,.2f}", f"${ag['a_transferir']:,.2f}"])
        total += ag["a_transferir"]
    td.append(["","","","TOTAL","","","", f"${total:,.2f}"])

    t = Table(td, colWidths=[5.5*cm,2.8*cm,4.2*cm,3.5*cm,2.8*cm,2.8*cm,2.3*cm,2.8*cm], repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0),AM),("TEXTCOLOR",(0,0),(-1,0),colors.white),
        ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,0),9),
        ("ALIGN",(0,0),(-1,-1),"CENTER"),("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("FONTSIZE",(0,1),(-1,-1),8),("ROWBACKGROUNDS",(0,1),(-1,-2),[colors.white,AC]),
        ("BACKGROUND",(0,-1),(-1,-1),AZ),("TEXTCOLOR",(0,-1),(-1,-1),colors.white),
        ("FONTNAME",(0,-1),(-1,-1),"Helvetica-Bold"),
        ("GRID",(0,0),(-1,-1),0.3,colors.HexColor("#CCCCCC")),("ROWPADDING",(0,0),(-1,-1),6),
    ]))
    doc.build([ht, Spacer(1, 0.5*cm), t])
    return FileResponse(path, filename=filename, media_type="application/pdf")


# ── BACKUP ───────────────────────────────────────────────────────────────────

@app.get("/api/backup")
def descargar_backup():
    import json
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM agentes ORDER BY id")
        agentes = _rows(cur)
        cur.execute("SELECT * FROM categorias ORDER BY id")
        categorias = _rows(cur)
        cur.execute("SELECT * FROM tickets ORDER BY id")
        tickets = _rows(cur)
        for t in tickets:
            for k in ("fecha_gasto", "creado_en", "revisado_en"):
                if t.get(k) and not isinstance(t[k], str):
                    t[k] = t[k].isoformat()
        cur.execute("SELECT * FROM periodos ORDER BY id")
        periodos = _rows(cur)
        for a in agentes:
            if a.get("creado_en") and not isinstance(a["creado_en"], str):
                a["creado_en"] = a["creado_en"].isoformat()

    backup = {
        "metadata": {
            "sistema": "Viaticos ATE",
            "version": "1.0",
            "fecha_backup": datetime.now().isoformat(),
            "motor": "postgresql" if USE_PG else "sqlite",
            "totales": {
                "agentes": len(agentes),
                "categorias": len(categorias),
                "tickets": len(tickets),
                "periodos": len(periodos),
            }
        },
        "agentes":    agentes,
        "categorias": categorias,
        "tickets":    tickets,
        "periodos":   periodos,
    }
    filename = f"backup_viaticos_ATE_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(backup, f, ensure_ascii=False, indent=2)
    return FileResponse(path, filename=filename, media_type="application/json")


@app.post("/api/restaurar")
async def restaurar_backup(request: dict):
    import json
    if "tickets" not in request or "agentes" not in request:
        raise HTTPException(400, "Archivo de backup invalido")
    with get_db() as conn:
        cur = conn.cursor()
        for tabla in ("periodos", "tickets", "agentes"):
            cur.execute(f"DELETE FROM {tabla}")
        for a in request.get("agentes", []):
            if USE_PG:
                cur.execute("""
                    INSERT INTO agentes (id,nombre,cuit,cbu,banco,alias,tope_mensual,activo,creado_en)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id) DO UPDATE SET nombre=EXCLUDED.nombre,cuit=EXCLUDED.cuit,
                    cbu=EXCLUDED.cbu,banco=EXCLUDED.banco,alias=EXCLUDED.alias,
                    tope_mensual=EXCLUDED.tope_mensual,activo=EXCLUDED.activo
                """, (a["id"],a["nombre"],a.get("cuit"),a.get("cbu"),a.get("banco"),
                      a.get("alias"),a.get("tope_mensual",0),a.get("activo",1),a.get("creado_en")))
            else:
                cur.execute("""INSERT OR REPLACE INTO agentes
                    (id,nombre,cuit,cbu,banco,alias,tope_mensual,activo,creado_en)
                    VALUES (?,?,?,?,?,?,?,?,?)""",
                    (a["id"],a["nombre"],a.get("cuit"),a.get("cbu"),a.get("banco"),
                     a.get("alias"),a.get("tope_mensual",0),a.get("activo",1),a.get("creado_en")))
        for t in request.get("tickets", []):
            if USE_PG:
                cur.execute("""INSERT INTO tickets
                    (id,agente_id,fecha_gasto,categoria_id,comprobante,descripcion,valor,estado,
                    motivo_rechazo,valor_aprobado,creado_en,revisado_en,revisado_por)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING""",
                    (t["id"],t["agente_id"],t["fecha_gasto"],t.get("categoria_id"),t.get("comprobante"),
                     t.get("descripcion"),t["valor"],t.get("estado","pendiente"),t.get("motivo_rechazo"),
                     t.get("valor_aprobado"),t.get("creado_en"),t.get("revisado_en"),t.get("revisado_por")))
            else:
                cur.execute("""INSERT OR REPLACE INTO tickets
                    (id,agente_id,fecha_gasto,categoria_id,comprobante,descripcion,valor,estado,
                    motivo_rechazo,valor_aprobado,creado_en,revisado_en,revisado_por)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (t["id"],t["agente_id"],t["fecha_gasto"],t.get("categoria_id"),t.get("comprobante"),
                     t.get("descripcion"),t["valor"],t.get("estado","pendiente"),t.get("motivo_rechazo"),
                     t.get("valor_aprobado"),t.get("creado_en"),t.get("revisado_en"),t.get("revisado_por")))
        for p in request.get("periodos", []):
            if USE_PG:
                cur.execute("""INSERT INTO periodos (id,agente_id,anio,mes,tope_override,cerrado)
                    VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING""",
                    (p["id"],p["agente_id"],p["anio"],p["mes"],p.get("tope_override"),p.get("cerrado",0)))
            else:
                cur.execute("""INSERT OR REPLACE INTO periodos (id,agente_id,anio,mes,tope_override,cerrado)
                    VALUES (?,?,?,?,?,?)""",
                    (p["id"],p["agente_id"],p["anio"],p["mes"],p.get("tope_override"),p.get("cerrado",0)))
        if USE_PG:
            for tabla in ("agentes", "tickets", "periodos"):
                cur.execute(f"SELECT setval(pg_get_serial_sequence('{tabla}','id'), COALESCE(MAX(id),1)) FROM {tabla}")
    meta = request.get("metadata", {})
    return {
        "mensaje": "Backup restaurado correctamente",
        "agentes_restaurados": len(request.get("agentes", [])),
        "tickets_restaurados": len(request.get("tickets", [])),
        "fecha_backup_original": meta.get("fecha_backup", "desconocida"),
    }

# ── FRONTEND ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def frontend():
    with open(os.path.join(BASE_DIR, "templates", "index.html"), encoding="utf-8") as f:
        return f.read()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=True)
