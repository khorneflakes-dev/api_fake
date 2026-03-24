import csv
import os
from math import ceil
from fastapi import FastAPI, Depends, HTTPException, Query, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

# ──────────────────────────────────────────────
#  Configuración
# ──────────────────────────────────────────────
VALID_TOKEN = os.getenv("API_TOKEN", "mi_token")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VENTAS_CSV = os.path.join(BASE_DIR, "ventas.csv")
PRODUCTOS_CSV = os.path.join(BASE_DIR, "productos.csv")
INVENTARIO_CSV = os.path.join(BASE_DIR, "inventario.csv")

app = FastAPI(
    title="API Fake — Ventas y Productos",
    version="1.0.0",
    description="API de ejemplo con paginación y autenticación Bearer Token.",
)

security = HTTPBearer()


# ──────────────────────────────────────────────
#  Helpers
# ──────────────────────────────────────────────
def verificar_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Valida el Bearer Token recibido en el header Authorization."""
    if credentials.credentials != VALID_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido o expirado.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return credentials.credentials


def leer_csv(ruta: str) -> list[dict]:
    """Lee un CSV y devuelve una lista de diccionarios."""
    if not os.path.exists(ruta):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Archivo de datos no encontrado: {ruta}",
        )
    with open(ruta, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def paginar(datos: list[dict], desde: int, limit: int) -> dict:
    """Aplica paginación y construye la respuesta estándar."""
    total = len(datos)
    pagina_actual = (desde // limit) + 1 if limit > 0 else 1
    total_paginas = ceil(total / limit) if limit > 0 else 1

    slice_datos = datos[desde: desde + limit]

    return {
        "total": total,
        "desde": desde,
        "limit": limit,
        "pagina_actual": pagina_actual,
        "total_paginas": total_paginas,
        "siguiente": desde + limit if desde + limit < total else None,
        "anterior": desde - limit if desde - limit >= 0 else None,
        "datos": slice_datos,
    }


# ──────────────────────────────────────────────
#  Endpoints
# ──────────────────────────────────────────────
@app.get("/ventas", summary="Listado de ventas paginado")
def get_ventas(
    desde: int = Query(default=0, ge=0, description="Índice de inicio (offset)"),
    limit: int = Query(default=10, ge=1, le=100, description="Cantidad máxima de registros a devolver"),
    prueba_lote: int | None = Query(default=None, ge=1, description="Número máximo de filas del CSV a usar (para pruebas de carga incremental)"),
    _token: str = Depends(verificar_token),
):
    """
    Devuelve el listado de ventas con paginación.

    - **desde**: posición de inicio en el listado (por defecto 0).
    - **limit**: número de registros por página (por defecto 10, máximo 100).
    - **prueba_lote**: limita el total de filas leídas del CSV; útil para probar cargas incrementales sin modificar el archivo.
    """
    datos = leer_csv(VENTAS_CSV)
    if prueba_lote is not None:
        datos = datos[:prueba_lote]
    return paginar(datos, desde, limit)


@app.get("/ventas/{venta_id}", summary="Detalle de una venta por ID")
def get_venta(
    venta_id: int,
    _token: str = Depends(verificar_token),
):
    """Devuelve los datos de una venta específica según su `id`."""
    datos = leer_csv(VENTAS_CSV)
    for item in datos:
        if item.get("id") == str(venta_id):
            return item
    raise HTTPException(status_code=404, detail=f"Venta con id={venta_id} no encontrada.")


@app.get("/productos", summary="Listado de productos paginado")
def get_productos(
    desde: int = Query(default=0, ge=0, description="Índice de inicio (offset)"),
    limit: int = Query(default=10, ge=1, le=100, description="Cantidad máxima de registros a devolver"),
    _token: str = Depends(verificar_token),
):
    """
    Devuelve el listado de productos con paginación.

    - **desde**: posición de inicio en el listado (por defecto 0).
    - **limit**: número de registros por página (por defecto 10, máximo 100).
    """
    datos = leer_csv(PRODUCTOS_CSV)
    return paginar(datos, desde, limit)


@app.get("/productos/{producto_id}", summary="Detalle de un producto por ID")
def get_producto(
    producto_id: int,
    _token: str = Depends(verificar_token),
):
    """Devuelve los datos de un producto específico según su `id`."""
    datos = leer_csv(PRODUCTOS_CSV)
    for item in datos:
        if item.get("id") == str(producto_id):
            return item
    raise HTTPException(status_code=404, detail=f"Producto con id={producto_id} no encontrado.")


@app.get("/inventario", summary="Listado de inventario con filtros de fecha")
def get_inventario(
    desde: int = Query(default=0, ge=0, description="Índice de inicio (offset)"),
    limit: int = Query(default=10, ge=1, le=100, description="Cantidad máxima de registros a devolver"),
    created_at: str | None = Query(default=None, description="Filtra registros cuyo created_at es mayor o igual a este valor (ej. '2024-12-07' o '2024-12-07T09:01:00')"),
    updated_at: str | None = Query(default=None, description="Filtra registros cuyo updated_at es mayor o igual a este valor (ej. '2025-01-08' o '2025-01-08T12:01:00')"),
    prueba_lote: int | None = Query(default=None, ge=1, description="Número máximo de filas del CSV a usar (para pruebas de carga incremental)"),
    _token: str = Depends(verificar_token),
):
    """
    Devuelve el inventario con filtros de fecha.

    - **created_at**: filtra registros cuyo campo `created_at` comienza con el valor indicado.
    - **updated_at**: filtra registros cuyo campo `updated_at` comienza con el valor indicado.
    - **prueba_lote**: limita el total de filas leídas del CSV antes de aplicar filtros; útil para probar cargas incrementales.
    - **desde** / **limit**: paginación estándar.
    """
    datos = leer_csv(INVENTARIO_CSV)

    if prueba_lote is not None:
        datos = datos[:prueba_lote]

    if created_at is not None:
        datos = [r for r in datos if r.get("created_at", "") >= created_at]

    if updated_at is not None:
        datos = [r for r in datos if r.get("updated_at", "") >= updated_at]

    return paginar(datos, desde, limit)


@app.get("/inventario/{item_id}", summary="Detalle de un ítem del inventario por ID")
def get_inventario_item(
    item_id: int,
    incluir_eliminados: bool = Query(default=False, description="Si es true permite recuperar registros con is_deleted=true"),
    _token: str = Depends(verificar_token),
):
    """
    Devuelve un ítem del inventario por su `id`.
    Por defecto no retorna ítems eliminados; usa `incluir_eliminados=true` para acceder a ellos.
    """
    datos = leer_csv(INVENTARIO_CSV)
    for item in datos:
        if item.get("id") == str(item_id):
            if not incluir_eliminados and item.get("is_deleted", "false").lower() == "true":
                raise HTTPException(status_code=404, detail=f"Ítem con id={item_id} fue eliminado. Usa incluir_eliminados=true para verlo.")
            return item
    raise HTTPException(status_code=404, detail=f"Ítem con id={item_id} no encontrado en inventario.")
