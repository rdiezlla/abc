from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import re

import pandas as pd


MOVIMIENTOS_FILE = Path("movimientos.xlsx")
OUTPUT_DIR = Path("output")
PARQUET_DIR = OUTPUT_DIR / "parquet"
JSON_DIR = OUTPUT_DIR / "json"
AUDIT_DIR = OUTPUT_DIR / "auditoria"
AUDIT_FILE = AUDIT_DIR / "analisis_rotacion_abc_auditoria.xlsx"
RECENT_DAYS = 14
ROLLING_DAYS = 30


@dataclass(frozen=True)
class Columns:
    movement_type: str = "Tipo movimiento"
    movement_date: str = "Fecha inicio"
    article: str = "Artículo"
    article_desc: str = "Denominación artículo"
    quantity: str = "Cantidad"
    owner: str = "Propietario"
    stock_owner_name: str = "Denominación propietario"
    stock_owner: str = "Propie."
    stock_article: str = "Art._y"
    stock_desc: str = "Denominación"
    stock_qty: str = "Stock pal."
    stock_location: str = "Ubicacion"
    stock_status: str = "Ocupacion"


C = Columns()

MONTH_NAMES_ES = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre",
}


def detect_stock_file(workdir: Path) -> Path:
    candidates: list[tuple[pd.Timestamp, Path]] = []
    excluded = {
        MOVIMIENTOS_FILE.name.lower(),
        AUDIT_FILE.name.lower(),
        "analisis_rotacion_abc_2026.xlsx",
    }
    for path in workdir.glob("*.xlsx"):
        if path.name.lower() in excluded:
            continue
        snapshot_date = parse_snapshot_date(path)
        if snapshot_date is not None:
            candidates.append((snapshot_date, path))

    if not candidates:
        raise FileNotFoundError("No se ha encontrado ningún fichero de stock con formato dd-mm-yyyy.xlsx")

    candidates.sort(key=lambda item: (item[0], item[1].stat().st_mtime))
    return candidates[-1][1]


def parse_snapshot_date(path: Path) -> pd.Timestamp | None:
    match = re.search(r"(\d{2})-(\d{2})-(\d{4})", path.stem)
    if not match:
        return None
    day, month, year = map(int, match.groups())
    return pd.Timestamp(year=year, month=month, day=day).normalize()


def end_of_day(value: pd.Timestamp) -> pd.Timestamp:
    return value.normalize() + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)


def normalize_key(value: object) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    if re.fullmatch(r"\d+\.0", text):
        return text[:-2]
    return text


def first_non_empty(series: pd.Series) -> str | None:
    for value in series:
        if pd.notna(value):
            text = str(value).strip()
            if text and text.lower() != "nan":
                return text
    return None


def to_yes_no(series: pd.Series) -> pd.Series:
    return series.map(lambda value: "Sí" if bool(value) else "No")


def days_since(reference_date: pd.Timestamp, series: pd.Series) -> pd.Series:
    values = pd.to_datetime(series, errors="coerce")
    return (reference_date.normalize() - values.dt.normalize()).dt.days


def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    num = pd.to_numeric(numerator, errors="coerce")
    den = pd.to_numeric(denominator, errors="coerce")
    return num.div(den.where(den != 0))


def concat_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    valid_frames = [frame for frame in frames if not frame.empty]
    if not valid_frames:
        return pd.DataFrame()

    columns = valid_frames[0].columns.tolist()
    records: list[dict[str, object]] = []
    for frame in valid_frames:
        records.extend(frame.reindex(columns=columns).to_dict("records"))
    return pd.DataFrame.from_records(records, columns=columns)


def ensure_output_dirs() -> None:
    for directory in [OUTPUT_DIR, PARQUET_DIR, JSON_DIR, AUDIT_DIR]:
        directory.mkdir(parents=True, exist_ok=True)


def require_columns(frame: pd.DataFrame, required: list[str], dataset_name: str) -> None:
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"Faltan columnas en {dataset_name}: {missing}")


def require_excel_columns(path: Path, frame: pd.DataFrame, required: list[str], context: str) -> None:
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(
            f"El fichero {path.name} no contiene las columnas obligatorias para {context}: {missing}"
        )


def add_metadata_columns(frame: pd.DataFrame, snapshot_date: pd.Timestamp, generated_at: str) -> pd.DataFrame:
    output = frame.copy()
    output.insert(0, "generated_at", generated_at)
    output.insert(0, "snapshot_date", snapshot_date.strftime("%Y-%m-%d"))
    return output


def quarter_label(quarter: int) -> str:
    return f"Q{int(quarter)}"


def month_name_list(month_numbers: list[int]) -> str | None:
    cleaned = [MONTH_NAMES_ES.get(int(month)) for month in month_numbers if pd.notna(month)]
    cleaned = [name for name in cleaned if name]
    return ", ".join(cleaned) if cleaned else None


def quarter_name_list(quarters: list[int]) -> str | None:
    cleaned = [quarter_label(int(quarter)) for quarter in quarters if pd.notna(quarter)]
    return ", ".join(cleaned) if cleaned else None


def compute_next_peak_month(current_month: int, peak_months: list[int]) -> tuple[str | None, float | None]:
    valid_peaks = sorted({int(month) for month in peak_months if pd.notna(month)})
    if not valid_peaks:
        return None, None

    distances = [((month - current_month) % 12, month) for month in valid_peaks]
    distances.sort(key=lambda item: (item[0], item[1]))
    months_to_peak, next_month = distances[0]
    return MONTH_NAMES_ES.get(next_month), float(months_to_peak)


def compute_next_peak_quarter(current_quarter: int, peak_quarters: list[int]) -> tuple[str | None, float | None]:
    valid_peaks = sorted({int(quarter) for quarter in peak_quarters if pd.notna(quarter)})
    if not valid_peaks:
        return None, None

    distances = [((quarter - current_quarter) % 4, quarter) for quarter in valid_peaks]
    distances.sort(key=lambda item: (item[0], item[1]))
    quarters_to_peak, next_quarter = distances[0]
    return quarter_label(next_quarter), float(quarters_to_peak)


def add_pareto_classification(
    frame: pd.DataFrame,
    line_col: str,
    qty_col: str,
    sort_cols: list[str],
    abc_col: str,
    pct_col: str,
    cum_col: str,
) -> pd.DataFrame:
    ranked = frame.copy()
    if ranked.empty:
        ranked[abc_col] = pd.Series(dtype="object")
        ranked[pct_col] = pd.Series(dtype="float64")
        ranked[cum_col] = pd.Series(dtype="float64")
        return ranked

    ranked = ranked.sort_values(
        by=[line_col, qty_col, *sort_cols],
        ascending=[False, False, *([True] * len(sort_cols))],
    ).reset_index(drop=True)

    total_lines = ranked[line_col].sum()
    if total_lines <= 0:
        ranked[pct_col] = 0.0
        ranked[cum_col] = 0.0
        ranked[abc_col] = "D"
        return ranked

    ranked[pct_col] = ranked[line_col] / total_lines
    ranked[cum_col] = ranked[pct_col].cumsum()
    cumulative_before = ranked[cum_col] - ranked[pct_col]

    ranked[abc_col] = "C"
    ranked.loc[cumulative_before < 0.80, abc_col] = "A"
    ranked.loc[(cumulative_before >= 0.80) & (cumulative_before < 0.95), abc_col] = "B"
    return ranked


def aggregate_pi_metrics(
    pi_frame: pd.DataFrame,
    group_cols: list[str],
    line_col: str,
    qty_col: str,
    last_col: str,
    desc_col: str,
) -> pd.DataFrame:
    if pi_frame.empty:
        return pd.DataFrame(columns=[*group_cols, line_col, qty_col, last_col, desc_col])

    return (
        pi_frame.groupby(group_cols, dropna=False)
        .agg(
            **{
                line_col: (group_cols[0], "size"),
                qty_col: ("cantidad_movimiento", "sum"),
                last_col: ("fecha_movimiento", "max"),
                desc_col: ("descripcion_movimiento", first_non_empty),
            }
        )
        .reset_index()
    )


def aggregate_cr_metrics(
    movements: pd.DataFrame,
    group_cols: list[str],
    end_date: pd.Timestamp,
    last_col: str = "ultima_cr",
    line_col: str = "lineas_cr_historico",
    qty_col: str = "cantidad_cr_historico",
) -> pd.DataFrame:
    cr = movements[
        (movements[C.movement_type] == "CR")
        & (movements["fecha_movimiento"] <= end_date)
    ].copy()
    if cr.empty:
        return pd.DataFrame(columns=[*group_cols, line_col, qty_col, last_col])

    return (
        cr.groupby(group_cols, dropna=False)
        .agg(
            **{
                line_col: (group_cols[0], "size"),
                qty_col: ("cantidad_movimiento", "sum"),
                last_col: ("fecha_movimiento", "max"),
            }
        )
        .reset_index()
    )


def aggregate_last_pi(
    movements: pd.DataFrame,
    group_cols: list[str],
    end_date: pd.Timestamp,
    last_col: str,
) -> pd.DataFrame:
    pi = movements[
        (movements[C.movement_type] == "PI")
        & (movements["fecha_movimiento"] <= end_date)
    ].copy()
    if pi.empty:
        return pd.DataFrame(columns=[*group_cols, last_col])

    return (
        pi.groupby(group_cols, dropna=False)
        .agg(**{last_col: ("fecha_movimiento", "max")})
        .reset_index()
    )


def prepare_movements(snapshot_date: pd.Timestamp) -> pd.DataFrame:
    movements = pd.read_excel(MOVIMIENTOS_FILE)
    require_excel_columns(
        MOVIMIENTOS_FILE,
        movements,
        [
            C.movement_type,
            C.movement_date,
            C.article,
            C.article_desc,
            C.quantity,
            C.owner,
        ],
        "movimientos",
    )
    snapshot_ts = end_of_day(snapshot_date)

    movements["fecha_movimiento"] = pd.to_datetime(
        movements[C.movement_date], errors="coerce", dayfirst=True
    )
    movements["owner_key"] = movements[C.owner].map(normalize_key)
    movements["article_key"] = movements[C.article].map(normalize_key)
    movements["descripcion_movimiento"] = movements[C.article_desc]
    movements["cantidad_movimiento"] = pd.to_numeric(movements[C.quantity], errors="coerce").fillna(0)

    movements = movements[
        movements["fecha_movimiento"].notna()
        & (movements["fecha_movimiento"] <= snapshot_ts)
        & movements["article_key"].notna()
    ].copy()

    movements["year"] = movements["fecha_movimiento"].dt.year
    movements["quarter"] = movements["fecha_movimiento"].dt.quarter
    movements["period_label"] = (
        movements["year"].astype(str) + "Q" + movements["quarter"].astype(str)
    )
    return movements


def prepare_stock(stock_file: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    stock = pd.read_excel(stock_file)
    require_excel_columns(
        stock_file,
        stock,
        [
            C.stock_owner_name,
            C.stock_owner,
            C.stock_article,
            C.stock_desc,
            C.stock_qty,
            C.stock_location,
            C.stock_status,
        ],
        "stock",
    )
    stock["owner_key"] = stock[C.stock_owner].map(normalize_key)
    stock["article_key"] = stock[C.stock_article].map(normalize_key)
    stock["stock_actual"] = pd.to_numeric(stock[C.stock_qty], errors="coerce").fillna(0)

    occupied = stock[
        stock["owner_key"].notna()
        & stock["article_key"].notna()
        & (stock["stock_actual"] > 0)
        & (stock[C.stock_status] == "Ocupado")
    ].copy()

    owner_article_stock = (
        occupied.groupby(["owner_key", "article_key"], dropna=False)
        .agg(
            propietario=(C.stock_owner_name, first_non_empty),
            articulo=("article_key", "first"),
            descripcion_stock=(C.stock_desc, first_non_empty),
            stock_actual=("stock_actual", "sum"),
            ubicaciones_con_stock=(C.stock_location, "nunique"),
        )
        .reset_index()
    )

    article_only_stock = (
        occupied.groupby(["article_key"], dropna=False)
        .agg(
            articulo=("article_key", "first"),
            descripcion_stock=(C.stock_desc, first_non_empty),
            stock_actual_total=("stock_actual", "sum"),
            ubicaciones_con_stock=(C.stock_location, "nunique"),
            propietarios_distintos=("owner_key", "nunique"),
        )
        .reset_index()
    )
    return owner_article_stock, article_only_stock


def build_dimensions(
    movements: pd.DataFrame,
    owner_article_stock: pd.DataFrame,
    article_only_stock: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    movement_owner_dim = (
        movements[movements["owner_key"].notna()]
        .groupby(["owner_key", "article_key"], dropna=False)
        .agg(descripcion_movimiento=("descripcion_movimiento", first_non_empty))
        .reset_index()
    )
    movement_article_dim = (
        movements.groupby(["article_key"], dropna=False)
        .agg(descripcion_movimiento=("descripcion_movimiento", first_non_empty))
        .reset_index()
    )

    owner_dim = movement_owner_dim.merge(
        owner_article_stock,
        how="outer",
        on=["owner_key", "article_key"],
    )
    owner_dim["articulo"] = owner_dim["article_key"]
    owner_dim["descripcion"] = owner_dim["descripcion_stock"].combine_first(owner_dim["descripcion_movimiento"])
    owner_dim["propietario"] = owner_dim["propietario"].fillna(owner_dim["owner_key"])
    owner_dim["stock_actual"] = owner_dim["stock_actual"].fillna(0)
    owner_dim["ubicaciones_con_stock"] = owner_dim["ubicaciones_con_stock"].fillna(0).astype(int)
    owner_dim = owner_dim[
        [
            "propietario",
            "owner_key",
            "article_key",
            "articulo",
            "descripcion",
            "stock_actual",
            "ubicaciones_con_stock",
        ]
    ].drop_duplicates()

    article_owner_counts = (
        movements[movements["owner_key"].notna()]
        .groupby("article_key", dropna=False)
        .agg(propietarios_movimiento=("owner_key", "nunique"))
        .reset_index()
    )

    article_dim = movement_article_dim.merge(
        article_only_stock,
        how="outer",
        on=["article_key"],
    ).merge(
        article_owner_counts,
        how="left",
        on=["article_key"],
    )
    article_dim["articulo"] = article_dim["article_key"]
    article_dim["descripcion"] = article_dim["descripcion_stock"].combine_first(article_dim["descripcion_movimiento"])
    article_dim["stock_actual_total"] = article_dim["stock_actual_total"].fillna(0)
    article_dim["ubicaciones_con_stock"] = article_dim["ubicaciones_con_stock"].fillna(0).astype(int)
    article_dim["propietarios_distintos"] = (
        article_dim["propietarios_distintos"]
        .fillna(article_dim["propietarios_movimiento"])
        .fillna(0)
        .astype(int)
    )
    article_dim = article_dim[
        [
            "article_key",
            "articulo",
            "descripcion",
            "propietarios_distintos",
            "stock_actual_total",
            "ubicaciones_con_stock",
        ]
    ].drop_duplicates()
    return owner_dim, article_dim


def apply_rotation_logic(
    base: pd.DataFrame,
    pi_metrics: pd.DataFrame,
    cr_metrics: pd.DataFrame,
    group_cols: list[str],
    line_col: str,
    qty_col: str,
    pct_col: str,
    cum_col: str,
    abc_col: str,
    final_col: str,
    last_pi_col: str,
    reference_date: pd.Timestamp,
    recent_text: str,
    criteria_col: str | None = None,
    active_label: str | None = None,
    inactive_label: str | None = None,
) -> pd.DataFrame:
    detail = base.merge(pi_metrics, how="left", on=group_cols).merge(cr_metrics, how="left", on=group_cols)

    detail[line_col] = pd.to_numeric(detail[line_col], errors="coerce").fillna(0).astype(int)
    detail[qty_col] = pd.to_numeric(detail[qty_col], errors="coerce").fillna(0)
    detail[pct_col] = pd.to_numeric(detail[pct_col], errors="coerce").fillna(0)
    detail[cum_col] = pd.to_numeric(detail[cum_col], errors="coerce").fillna(0)
    detail[abc_col] = detail[abc_col].fillna("D")
    if "lineas_cr_historico" in detail.columns:
        detail["lineas_cr_historico"] = pd.to_numeric(detail["lineas_cr_historico"], errors="coerce").fillna(0).astype(int)
    if "cantidad_cr_historico" in detail.columns:
        detail["cantidad_cr_historico"] = pd.to_numeric(detail["cantidad_cr_historico"], errors="coerce").fillna(0)

    recent_cutoff = end_of_day(reference_date) - pd.Timedelta(days=RECENT_DAYS)
    no_rotation = detail[line_col].eq(0)
    recent_arrival = no_rotation & detail["ultima_cr"].notna() & detail["ultima_cr"].ge(recent_cutoff)

    detail[final_col] = detail[abc_col]
    detail.loc[no_rotation, final_col] = "D"
    detail.loc[recent_arrival, final_col] = recent_text

    if criteria_col is not None:
        detail[criteria_col] = active_label or "Pareto ABC"
        if inactive_label is not None:
            detail.loc[detail[final_col] == "D", criteria_col] = inactive_label
        detail.loc[recent_arrival, criteria_col] = (
            f"Sin PI y con última entrada CR en los últimos {RECENT_DAYS} días"
        )

    detail[f"dias_desde_{last_pi_col}"] = days_since(reference_date, detail[last_pi_col])
    detail["dias_desde_ultima_cr"] = days_since(reference_date, detail["ultima_cr"])
    return detail


def build_owner_article_30d(
    movements: pd.DataFrame,
    owner_article_stock: pd.DataFrame,
    snapshot_date: pd.Timestamp,
) -> pd.DataFrame:
    start_30d = snapshot_date - pd.Timedelta(days=ROLLING_DAYS - 1)
    snapshot_ts = end_of_day(snapshot_date)

    pi_30d = movements[
        (movements[C.movement_type] == "PI")
        & movements["owner_key"].notna()
        & (movements["fecha_movimiento"] >= start_30d)
        & (movements["fecha_movimiento"] <= snapshot_ts)
    ].copy()
    pi_30d = aggregate_pi_metrics(
        pi_30d,
        ["owner_key", "article_key"],
        "lineas_pi_30d",
        "cantidad_pi_30d",
        "ultima_pi_30d",
        "descripcion_pi_30d",
    )
    pi_30d = add_pareto_classification(
        pi_30d,
        "lineas_pi_30d",
        "cantidad_pi_30d",
        ["owner_key", "article_key"],
        "rotacion_abc_30d",
        "porcentaje_lineas_pi_30d",
        "porcentaje_acumulado_pi_30d",
    )

    cr_metrics = aggregate_cr_metrics(
        movements,
        ["owner_key", "article_key"],
        snapshot_ts,
    )

    detail = apply_rotation_logic(
        owner_article_stock.copy(),
        pi_30d,
        cr_metrics,
        ["owner_key", "article_key"],
        "lineas_pi_30d",
        "cantidad_pi_30d",
        "porcentaje_lineas_pi_30d",
        "porcentaje_acumulado_pi_30d",
        "rotacion_abc_30d",
        "rotacion_final_30d",
        "ultima_pi_30d",
        snapshot_date,
        "Sin rotación 30d, recién llegado",
    )

    detail["descripcion"] = detail["descripcion_stock"].combine_first(detail["descripcion_pi_30d"])
    return detail[
        [
            "propietario",
            "owner_key",
            "articulo",
            "descripcion",
            "stock_actual",
            "ubicaciones_con_stock",
            "lineas_pi_30d",
            "cantidad_pi_30d",
            "porcentaje_lineas_pi_30d",
            "porcentaje_acumulado_pi_30d",
            "rotacion_abc_30d",
            "rotacion_final_30d",
            "ultima_pi_30d",
            "dias_desde_ultima_pi_30d",
            "ultima_cr",
            "dias_desde_ultima_cr",
            "lineas_cr_historico",
            "cantidad_cr_historico",
        ]
    ]


def build_article_only_30d(
    movements: pd.DataFrame,
    article_only_stock: pd.DataFrame,
    snapshot_date: pd.Timestamp,
) -> pd.DataFrame:
    start_30d = snapshot_date - pd.Timedelta(days=ROLLING_DAYS - 1)
    snapshot_ts = end_of_day(snapshot_date)

    pi_30d = movements[
        (movements[C.movement_type] == "PI")
        & (movements["fecha_movimiento"] >= start_30d)
        & (movements["fecha_movimiento"] <= snapshot_ts)
    ].copy()
    pi_30d = aggregate_pi_metrics(
        pi_30d,
        ["article_key"],
        "lineas_pi_30d",
        "cantidad_pi_30d",
        "ultima_pi_30d",
        "descripcion_pi_30d",
    )
    pi_30d = add_pareto_classification(
        pi_30d,
        "lineas_pi_30d",
        "cantidad_pi_30d",
        ["article_key"],
        "rotacion_abc_30d",
        "porcentaje_lineas_pi_30d",
        "porcentaje_acumulado_pi_30d",
    )

    cr_metrics = aggregate_cr_metrics(
        movements,
        ["article_key"],
        snapshot_ts,
    )

    detail = apply_rotation_logic(
        article_only_stock.copy(),
        pi_30d,
        cr_metrics,
        ["article_key"],
        "lineas_pi_30d",
        "cantidad_pi_30d",
        "porcentaje_lineas_pi_30d",
        "porcentaje_acumulado_pi_30d",
        "rotacion_abc_30d",
        "rotacion_final_30d",
        "ultima_pi_30d",
        snapshot_date,
        "Sin rotación 30d, recién llegado",
    )

    detail["descripcion"] = detail["descripcion_stock"].combine_first(detail["descripcion_pi_30d"])
    return detail[
        [
            "articulo",
            "descripcion",
            "propietarios_distintos",
            "stock_actual_total",
            "ubicaciones_con_stock",
            "lineas_pi_30d",
            "cantidad_pi_30d",
            "porcentaje_lineas_pi_30d",
            "porcentaje_acumulado_pi_30d",
            "rotacion_abc_30d",
            "rotacion_final_30d",
            "ultima_pi_30d",
            "dias_desde_ultima_pi_30d",
            "ultima_cr",
            "dias_desde_ultima_cr",
            "lineas_cr_historico",
            "cantidad_cr_historico",
        ]
    ]


def build_owner_article_ytd(
    movements: pd.DataFrame,
    owner_article_stock: pd.DataFrame,
    snapshot_date: pd.Timestamp,
) -> pd.DataFrame:
    snapshot_ts = end_of_day(snapshot_date)
    ytd_start = pd.Timestamp(year=snapshot_date.year, month=1, day=1)
    line_col = f"lineas_pi_{snapshot_date.year}"
    qty_col = f"cantidad_pi_{snapshot_date.year}"
    last_col = f"ultima_pi_{snapshot_date.year}"

    pi_ytd = movements[
        (movements[C.movement_type] == "PI")
        & movements["owner_key"].notna()
        & (movements["fecha_movimiento"] >= ytd_start)
        & (movements["fecha_movimiento"] <= snapshot_ts)
    ].copy()
    pi_ytd = aggregate_pi_metrics(
        pi_ytd,
        ["owner_key", "article_key"],
        line_col,
        qty_col,
        last_col,
        "descripcion_pi",
    )
    pi_ytd = add_pareto_classification(
        pi_ytd,
        line_col,
        qty_col,
        ["owner_key", "article_key"],
        "rotacion_abc",
        "porcentaje_lineas_pi",
        "porcentaje_acumulado_pi",
    )

    cr_metrics = aggregate_cr_metrics(movements, ["owner_key", "article_key"], snapshot_ts)
    last_pi_hist = aggregate_last_pi(
        movements,
        ["owner_key", "article_key"],
        snapshot_ts,
        "ultima_pi_historica",
    )

    detail = apply_rotation_logic(
        owner_article_stock.copy(),
        pi_ytd,
        cr_metrics,
        ["owner_key", "article_key"],
        line_col,
        qty_col,
        "porcentaje_lineas_pi",
        "porcentaje_acumulado_pi",
        "rotacion_abc",
        "rotacion_final",
        last_col,
        snapshot_date,
        "Sin rotación, recién llegado",
        "criterio_rotacion",
        f"Pareto ABC por líneas PI {snapshot_date.year}",
        f"Sin líneas PI {snapshot_date.year} y sin entrada CR en los últimos {RECENT_DAYS} días",
    )
    detail = detail.merge(last_pi_hist, how="left", on=["owner_key", "article_key"])
    detail["descripcion"] = detail["descripcion_stock"].combine_first(detail["descripcion_pi"])

    detail_30d = build_owner_article_30d(movements, owner_article_stock, snapshot_date)
    detail = detail.merge(
        detail_30d[
            [
                "owner_key",
                "articulo",
                "lineas_pi_30d",
                "cantidad_pi_30d",
                "porcentaje_lineas_pi_30d",
                "porcentaje_acumulado_pi_30d",
                "rotacion_abc_30d",
                "rotacion_final_30d",
                "ultima_pi_30d",
                "dias_desde_ultima_pi_30d",
            ]
        ],
        how="left",
        on=["owner_key", "articulo"],
    )

    days_hist = days_since(snapshot_date, detail["ultima_pi_historica"])
    detail["inactivo_30d"] = to_yes_no(days_hist.gt(30) | detail["ultima_pi_historica"].isna())
    detail["inactivo_90d"] = to_yes_no(days_hist.gt(90) | detail["ultima_pi_historica"].isna())
    detail["dispersion_stock"] = detail["ubicaciones_con_stock"]
    detail["densidad_stock"] = safe_divide(detail["stock_actual"], detail["ubicaciones_con_stock"])
    detail["cobertura_lineas_30d"] = safe_divide(detail["stock_actual"], detail["lineas_pi_30d"])
    detail["cobertura_cantidad_30d"] = safe_divide(detail["stock_actual"], detail["cantidad_pi_30d"])

    stock_positive = detail["stock_actual"] > 0
    stock_high_threshold = detail.loc[stock_positive, "stock_actual"].quantile(0.75) if stock_positive.any() else 0
    dispersion_threshold = detail.loc[stock_positive, "ubicaciones_con_stock"].quantile(0.75) if stock_positive.any() else 0
    density_threshold = (
        detail.loc[(detail["ubicaciones_con_stock"] > 1) & detail["densidad_stock"].notna(), "densidad_stock"].quantile(0.25)
        if ((detail["ubicaciones_con_stock"] > 1) & detail["densidad_stock"].notna()).any()
        else 0
    )

    recent_arrival_mask = detail["rotacion_final"].astype(str).str.contains("recién llegado", case=False, na=False)
    detail["flag_sobrestock"] = to_yes_no(
        (detail["stock_actual"] >= stock_high_threshold)
        & detail["rotacion_final"].isin(["C", "D"])
        & (detail["inactivo_90d"] == "Sí")
        & ~recent_arrival_mask
    )
    detail["flag_reubicar"] = to_yes_no(
        detail["rotacion_final"].isin(["A", "B"])
        & (detail["ubicaciones_con_stock"] >= max(4, dispersion_threshold))
        & (detail["densidad_stock"].fillna(0) <= density_threshold)
    )

    detail["lineas_pi_30d"] = detail["lineas_pi_30d"].fillna(0).astype(int)
    detail["cantidad_pi_30d"] = detail["cantidad_pi_30d"].fillna(0)
    detail["porcentaje_lineas_pi_30d"] = detail["porcentaje_lineas_pi_30d"].fillna(0)
    detail["porcentaje_acumulado_pi_30d"] = detail["porcentaje_acumulado_pi_30d"].fillna(0)
    detail["rotacion_abc_30d"] = detail["rotacion_abc_30d"].fillna("D")
    detail["rotacion_final_30d"] = detail["rotacion_final_30d"].fillna("D")

    detail["orden_rotacion"] = detail["rotacion_final"].map(
        {"A": 1, "B": 2, "C": 3, "Sin rotación, recién llegado": 4, "D": 5}
    ).fillna(9)

    detail = detail.sort_values(
        by=["orden_rotacion", line_col, "stock_actual", "propietario", "articulo"],
        ascending=[True, False, False, True, True],
    )
    return detail[
        [
            "propietario",
            "owner_key",
            "articulo",
            "descripcion",
            "stock_actual",
            "ubicaciones_con_stock",
            line_col,
            qty_col,
            "porcentaje_lineas_pi",
            "porcentaje_acumulado_pi",
            "rotacion_abc",
            "rotacion_final",
            "criterio_rotacion",
            last_col,
            f"dias_desde_{last_col}",
            "ultima_cr",
            "dias_desde_ultima_cr",
            "lineas_cr_historico",
            "cantidad_cr_historico",
            "lineas_pi_30d",
            "cantidad_pi_30d",
            "porcentaje_lineas_pi_30d",
            "porcentaje_acumulado_pi_30d",
            "rotacion_abc_30d",
            "rotacion_final_30d",
            "ultima_pi_30d",
            "dias_desde_ultima_pi_30d",
            "cobertura_lineas_30d",
            "cobertura_cantidad_30d",
            "inactivo_30d",
            "inactivo_90d",
            "dispersion_stock",
            "densidad_stock",
            "flag_sobrestock",
            "flag_reubicar",
        ]
    ]


def build_article_only_ytd(
    movements: pd.DataFrame,
    article_only_stock: pd.DataFrame,
    snapshot_date: pd.Timestamp,
) -> pd.DataFrame:
    snapshot_ts = end_of_day(snapshot_date)
    ytd_start = pd.Timestamp(year=snapshot_date.year, month=1, day=1)

    pi_ytd = movements[
        (movements[C.movement_type] == "PI")
        & (movements["fecha_movimiento"] >= ytd_start)
        & (movements["fecha_movimiento"] <= snapshot_ts)
    ].copy()
    pi_ytd = aggregate_pi_metrics(
        pi_ytd,
        ["article_key"],
        "lineas_pi_ytd",
        "cantidad_pi_ytd",
        "ultima_pi_ytd",
        "descripcion_pi",
    )
    pi_ytd = add_pareto_classification(
        pi_ytd,
        "lineas_pi_ytd",
        "cantidad_pi_ytd",
        ["article_key"],
        "rotacion_abc_ytd",
        "porcentaje_lineas_pi_ytd",
        "porcentaje_acumulado_pi_ytd",
    )

    cr_metrics = aggregate_cr_metrics(movements, ["article_key"], snapshot_ts)
    last_pi_hist = aggregate_last_pi(movements, ["article_key"], snapshot_ts, "ultima_pi_historica")

    detail = apply_rotation_logic(
        article_only_stock.copy(),
        pi_ytd,
        cr_metrics,
        ["article_key"],
        "lineas_pi_ytd",
        "cantidad_pi_ytd",
        "porcentaje_lineas_pi_ytd",
        "porcentaje_acumulado_pi_ytd",
        "rotacion_abc_ytd",
        "rotacion_final_ytd",
        "ultima_pi_ytd",
        snapshot_date,
        "Sin rotación, recién llegado",
        "criterio_rotacion_ytd",
        f"Pareto ABC por líneas PI YTD {snapshot_date.year}",
        f"Sin líneas PI YTD {snapshot_date.year} y sin entrada CR en los últimos {RECENT_DAYS} días",
    )
    detail = detail.merge(last_pi_hist, how="left", on=["article_key"])
    detail["descripcion"] = detail["descripcion_stock"].combine_first(detail["descripcion_pi"])

    detail_30d = build_article_only_30d(movements, article_only_stock, snapshot_date)
    detail = detail.merge(
        detail_30d[["articulo", "lineas_pi_30d", "cantidad_pi_30d", "rotacion_final_30d"]],
        how="left",
        on=["articulo"],
    )

    days_hist = days_since(snapshot_date, detail["ultima_pi_historica"])
    detail["inactivo_30d"] = to_yes_no(days_hist.gt(30) | detail["ultima_pi_historica"].isna())
    detail["inactivo_90d"] = to_yes_no(days_hist.gt(90) | detail["ultima_pi_historica"].isna())
    detail["dispersion_stock"] = detail["ubicaciones_con_stock"]
    detail["densidad_stock"] = safe_divide(detail["stock_actual_total"], detail["ubicaciones_con_stock"])
    detail["cobertura_lineas_30d"] = safe_divide(detail["stock_actual_total"], detail["lineas_pi_30d"])
    detail["cobertura_cantidad_30d"] = safe_divide(detail["stock_actual_total"], detail["cantidad_pi_30d"])

    stock_positive = detail["stock_actual_total"] > 0
    stock_high_threshold = detail.loc[stock_positive, "stock_actual_total"].quantile(0.75) if stock_positive.any() else 0
    dispersion_threshold = detail.loc[stock_positive, "ubicaciones_con_stock"].quantile(0.75) if stock_positive.any() else 0
    density_threshold = (
        detail.loc[(detail["ubicaciones_con_stock"] > 1) & detail["densidad_stock"].notna(), "densidad_stock"].quantile(0.25)
        if ((detail["ubicaciones_con_stock"] > 1) & detail["densidad_stock"].notna()).any()
        else 0
    )

    recent_arrival_mask = detail["rotacion_final_ytd"].astype(str).str.contains("recién llegado", case=False, na=False)
    detail["flag_sobrestock"] = to_yes_no(
        (detail["stock_actual_total"] >= stock_high_threshold)
        & detail["rotacion_final_ytd"].isin(["C", "D"])
        & (detail["inactivo_90d"] == "Sí")
        & ~recent_arrival_mask
    )
    detail["flag_reubicar"] = to_yes_no(
        detail["rotacion_final_ytd"].isin(["A", "B"])
        & (detail["ubicaciones_con_stock"] >= max(4, dispersion_threshold))
        & (detail["densidad_stock"].fillna(0) <= density_threshold)
    )

    detail["lineas_pi_30d"] = detail["lineas_pi_30d"].fillna(0).astype(int)
    detail["cantidad_pi_30d"] = detail["cantidad_pi_30d"].fillna(0)
    detail["rotacion_final_30d"] = detail["rotacion_final_30d"].fillna("D")

    detail["orden_rotacion"] = detail["rotacion_final_ytd"].map(
        {"A": 1, "B": 2, "C": 3, "Sin rotación, recién llegado": 4, "D": 5}
    ).fillna(9)
    detail = detail.sort_values(
        by=["orden_rotacion", "lineas_pi_ytd", "stock_actual_total", "articulo"],
        ascending=[True, False, False, True],
    )
    return detail[
        [
            "articulo",
            "descripcion",
            "propietarios_distintos",
            "stock_actual_total",
            "lineas_pi_ytd",
            "cantidad_pi_ytd",
            "porcentaje_lineas_pi_ytd",
            "porcentaje_acumulado_pi_ytd",
            "rotacion_abc_ytd",
            "rotacion_final_ytd",
            "criterio_rotacion_ytd",
            "ultima_pi_ytd",
            "dias_desde_ultima_pi_ytd",
            "ultima_cr",
            "dias_desde_ultima_cr",
            "lineas_cr_historico",
            "cantidad_cr_historico",
            "cobertura_lineas_30d",
            "cobertura_cantidad_30d",
            "inactivo_30d",
            "inactivo_90d",
            "dispersion_stock",
            "densidad_stock",
            "flag_sobrestock",
            "flag_reubicar",
        ]
    ]


def list_quarter_periods(movements: pd.DataFrame, snapshot_date: pd.Timestamp) -> list[dict[str, object]]:
    start_period = movements["fecha_movimiento"].min().to_period("Q")
    end_period = snapshot_date.to_period("Q")
    snapshot_ts = end_of_day(snapshot_date)
    periods: list[dict[str, object]] = []

    for period in pd.period_range(start_period, end_period, freq="Q"):
        periods.append(
            {
                "year": period.year,
                "quarter": period.quarter,
                "period_label": f"{period.year}Q{period.quarter}",
                "start": period.start_time.normalize(),
                "end": min(period.end_time, snapshot_ts),
                "is_current_quarter": period == end_period,
            }
        )
    return periods


def build_universe_for_period(
    movements: pd.DataFrame,
    dimension_df: pd.DataFrame,
    key_cols: list[str],
    end_date: pd.Timestamp,
    include_current_stock: bool,
    stock_filter_col: str,
) -> pd.DataFrame:
    active_keys = movements[
        movements[C.movement_type].isin(["PI", "CR"])
        & (movements["fecha_movimiento"] <= end_date)
    ][key_cols].drop_duplicates()

    if include_current_stock:
        stock_keys = dimension_df[dimension_df[stock_filter_col] > 0][key_cols].drop_duplicates()
        active_keys = pd.concat([active_keys, stock_keys], ignore_index=True).drop_duplicates()

    return active_keys.merge(dimension_df, how="left", on=key_cols)


def build_quarterly_outputs(
    movements: pd.DataFrame,
    owner_dim: pd.DataFrame,
    article_dim: pd.DataFrame,
    snapshot_date: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    owner_rows: list[pd.DataFrame] = []
    article_rows: list[pd.DataFrame] = []

    for period in list_quarter_periods(movements, snapshot_date):
        start = period["start"]
        end = period["end"]
        label = str(period["period_label"])

        quarter_pi_owner = movements[
            (movements[C.movement_type] == "PI")
            & movements["owner_key"].notna()
            & (movements["fecha_movimiento"] >= start)
            & (movements["fecha_movimiento"] <= end)
        ].copy()
        quarter_pi_owner = aggregate_pi_metrics(
            quarter_pi_owner,
            ["owner_key", "article_key"],
            "lineas_pi_trimestre",
            "cantidad_pi_trimestre",
            "ultima_pi_trimestre",
            "descripcion_pi",
        )
        quarter_pi_owner = add_pareto_classification(
            quarter_pi_owner,
            "lineas_pi_trimestre",
            "cantidad_pi_trimestre",
            ["owner_key", "article_key"],
            "rotacion_abc_trimestre",
            "porcentaje_lineas_pi_trimestre",
            "porcentaje_acumulado_pi_trimestre",
        )
        quarter_cr_owner = aggregate_cr_metrics(
            movements,
            ["owner_key", "article_key"],
            end,
            last_col="ultima_cr_hasta_fin_trimestre",
        ).rename(columns={"ultima_cr_hasta_fin_trimestre": "ultima_cr"})
        owner_universe = build_universe_for_period(
            movements,
            owner_dim,
            ["owner_key", "article_key"],
            end,
            bool(period["is_current_quarter"]),
            "stock_actual",
        )
        owner_detail = apply_rotation_logic(
            owner_universe,
            quarter_pi_owner,
            quarter_cr_owner,
            ["owner_key", "article_key"],
            "lineas_pi_trimestre",
            "cantidad_pi_trimestre",
            "porcentaje_lineas_pi_trimestre",
            "porcentaje_acumulado_pi_trimestre",
            "rotacion_abc_trimestre",
            "rotacion_final_trimestre",
            "ultima_pi_trimestre",
            pd.Timestamp(end).normalize(),
            "Sin rotación trimestre, recién llegado",
            "criterio_rotacion_trimestre",
            f"Pareto ABC por líneas PI del trimestre {label}",
            f"Sin líneas PI en {label} y sin entrada CR en los últimos {RECENT_DAYS} días",
        ).rename(columns={"ultima_cr": "ultima_cr_hasta_fin_trimestre"})
        owner_detail["year"] = period["year"]
        owner_detail["quarter"] = period["quarter"]
        owner_detail["period_label"] = label
        owner_rows.append(
            owner_detail[
                [
                    "year",
                    "quarter",
                    "period_label",
                    "propietario",
                    "owner_key",
                    "articulo",
                    "descripcion",
                    "lineas_pi_trimestre",
                    "cantidad_pi_trimestre",
                    "porcentaje_lineas_pi_trimestre",
                    "porcentaje_acumulado_pi_trimestre",
                    "rotacion_abc_trimestre",
                    "rotacion_final_trimestre",
                    "criterio_rotacion_trimestre",
                    "ultima_pi_trimestre",
                    "ultima_cr_hasta_fin_trimestre",
                    "stock_actual",
                    "ubicaciones_con_stock",
                ]
            ]
        )

        quarter_pi_article = movements[
            (movements[C.movement_type] == "PI")
            & (movements["fecha_movimiento"] >= start)
            & (movements["fecha_movimiento"] <= end)
        ].copy()
        quarter_pi_article = aggregate_pi_metrics(
            quarter_pi_article,
            ["article_key"],
            "lineas_pi_trimestre",
            "cantidad_pi_trimestre",
            "ultima_pi_trimestre",
            "descripcion_pi",
        )
        quarter_pi_article = add_pareto_classification(
            quarter_pi_article,
            "lineas_pi_trimestre",
            "cantidad_pi_trimestre",
            ["article_key"],
            "rotacion_abc_trimestre",
            "porcentaje_lineas_pi_trimestre",
            "porcentaje_acumulado_pi_trimestre",
        )
        quarter_cr_article = aggregate_cr_metrics(
            movements,
            ["article_key"],
            end,
            last_col="ultima_cr_hasta_fin_trimestre",
        ).rename(columns={"ultima_cr_hasta_fin_trimestre": "ultima_cr"})
        article_universe = build_universe_for_period(
            movements,
            article_dim,
            ["article_key"],
            end,
            bool(period["is_current_quarter"]),
            "stock_actual_total",
        )
        article_detail = apply_rotation_logic(
            article_universe,
            quarter_pi_article,
            quarter_cr_article,
            ["article_key"],
            "lineas_pi_trimestre",
            "cantidad_pi_trimestre",
            "porcentaje_lineas_pi_trimestre",
            "porcentaje_acumulado_pi_trimestre",
            "rotacion_abc_trimestre",
            "rotacion_final_trimestre",
            "ultima_pi_trimestre",
            pd.Timestamp(end).normalize(),
            "Sin rotación trimestre, recién llegado",
            "criterio_rotacion_trimestre",
            f"Pareto ABC por líneas PI del trimestre {label}",
            f"Sin líneas PI en {label} y sin entrada CR en los últimos {RECENT_DAYS} días",
        ).rename(columns={"ultima_cr": "ultima_cr_hasta_fin_trimestre"})
        article_detail["year"] = period["year"]
        article_detail["quarter"] = period["quarter"]
        article_detail["period_label"] = label
        article_rows.append(
            article_detail[
                [
                    "year",
                    "quarter",
                    "period_label",
                    "articulo",
                    "descripcion",
                    "propietarios_distintos",
                    "lineas_pi_trimestre",
                    "cantidad_pi_trimestre",
                    "porcentaje_lineas_pi_trimestre",
                    "porcentaje_acumulado_pi_trimestre",
                    "rotacion_abc_trimestre",
                    "rotacion_final_trimestre",
                    "criterio_rotacion_trimestre",
                    "ultima_pi_trimestre",
                    "ultima_cr_hasta_fin_trimestre",
                    "stock_actual_total",
                    "ubicaciones_con_stock",
                ]
            ]
        )

    owner_quarterly = concat_frames(owner_rows)
    article_quarterly = concat_frames(article_rows)
    return owner_quarterly, article_quarterly


def class_rank(value: object) -> int:
    text = str(value)
    if text == "A":
        return 4
    if text == "B":
        return 3
    if text == "C":
        return 2
    if "recién llegado" in text.lower():
        return 1
    if text == "D":
        return 1
    return 0


def build_quarterly_change_output(article_quarterly: pd.DataFrame) -> pd.DataFrame:
    if article_quarterly.empty:
        return pd.DataFrame(
            columns=[
                "year",
                "quarter",
                "period_label",
                "articulo",
                "descripcion",
                "rotacion_trimestre_anterior",
                "rotacion_final_trimestre",
                "cambio_abc",
                "sentido_cambio",
            ]
        )

    ordered = article_quarterly.sort_values(["articulo", "year", "quarter"]).copy()
    ordered["rotacion_trimestre_anterior"] = ordered.groupby("articulo")["rotacion_final_trimestre"].shift(1)

    previous = ordered["rotacion_trimestre_anterior"]
    current = ordered["rotacion_final_trimestre"]
    no_movement = ordered["lineas_pi_trimestre"].fillna(0).eq(0)
    ordered["cambio_abc"] = previous.fillna("Nuevo") + " -> " + current.fillna("D")
    ordered.loc[previous.isna(), "cambio_abc"] = "Nuevo"
    ordered.loc[no_movement & previous.notna(), "cambio_abc"] = "Sin movimiento"

    prev_rank = previous.map(class_rank)
    curr_rank = current.map(class_rank)
    ordered["sentido_cambio"] = "Se mantiene"
    ordered.loc[previous.isna(), "sentido_cambio"] = "Nuevo"
    ordered.loc[no_movement & previous.notna(), "sentido_cambio"] = "Sin movimiento"
    ordered.loc[(curr_rank > prev_rank) & previous.notna() & ~no_movement, "sentido_cambio"] = "Mejora"
    ordered.loc[(curr_rank < prev_rank) & previous.notna() & ~no_movement, "sentido_cambio"] = "Empeora"

    return ordered[
        [
            "year",
            "quarter",
            "period_label",
            "articulo",
            "descripcion",
            "rotacion_trimestre_anterior",
            "rotacion_final_trimestre",
            "cambio_abc",
            "sentido_cambio",
            "lineas_pi_trimestre",
            "cantidad_pi_trimestre",
            "stock_actual_total",
            "propietarios_distintos",
        ]
    ]


def build_class_summary(
    frame: pd.DataFrame,
    class_col: str,
    stock_col: str,
    line_col: str,
    qty_col: str,
    label: str | None = None,
    period_label: str | None = None,
) -> pd.DataFrame:
    if frame.empty:
        columns = ["rotacion", "referencias", "stock_total", "lineas_pi", "cantidad_pi", "porcentaje_stock", "porcentaje_referencias"]
        if label is not None:
            columns.insert(0, "analisis")
        if period_label is not None:
            columns.insert(1 if label is not None else 0, "period_label")
        return pd.DataFrame(columns=columns)

    summary = (
        frame.groupby(class_col, dropna=False)
        .agg(
            referencias=("articulo", "count"),
            stock_total=(stock_col, "sum"),
            lineas_pi=(line_col, "sum"),
            cantidad_pi=(qty_col, "sum"),
        )
        .reset_index()
        .rename(columns={class_col: "rotacion"})
    )
    total_stock = summary["stock_total"].sum()
    total_refs = summary["referencias"].sum()
    summary["porcentaje_stock"] = summary["stock_total"] / total_stock if total_stock else 0
    summary["porcentaje_referencias"] = summary["referencias"] / total_refs if total_refs else 0
    summary["orden_rotacion"] = summary["rotacion"].map(
        {
            "A": 1,
            "B": 2,
            "C": 3,
            "Sin rotación, recién llegado": 4,
            "Sin rotación 30d, recién llegado": 4,
            "Sin rotación trimestre, recién llegado": 4,
            "D": 5,
        }
    ).fillna(9)
    summary = summary.sort_values("orden_rotacion").drop(columns=["orden_rotacion"])

    if label is not None:
        summary.insert(0, "analisis", label)
    if period_label is not None:
        summary.insert(1 if label is not None else 0, "period_label", period_label)
    return summary


def build_summaries(
    detail_stock: pd.DataFrame,
    article_ytd: pd.DataFrame,
    owner_30d: pd.DataFrame,
    article_30d: pd.DataFrame,
    owner_quarterly: pd.DataFrame,
    article_quarterly: pd.DataFrame,
    snapshot_date: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    line_col = f"lineas_pi_{snapshot_date.year}"
    summary_current = (
        detail_stock.groupby("rotacion_final", dropna=False)
        .agg(
            referencias=("articulo", "count"),
            stock_total=("stock_actual", "sum"),
            **{line_col: (line_col, "sum")},
        )
        .reset_index()
    )
    summary_current["orden_rotacion"] = summary_current["rotacion_final"].map(
        {"A": 1, "B": 2, "C": 3, "Sin rotación, recién llegado": 4, "D": 5}
    ).fillna(9)
    summary_current = summary_current.sort_values("orden_rotacion").drop(columns=["orden_rotacion"])

    summary_article = build_class_summary(
        article_ytd,
        "rotacion_final_ytd",
        "stock_actual_total",
        "lineas_pi_ytd",
        "cantidad_pi_ytd",
    )

    summary_30d = pd.concat(
        [
            build_class_summary(
                owner_30d,
                "rotacion_final_30d",
                "stock_actual",
                "lineas_pi_30d",
                "cantidad_pi_30d",
                label="owner-articulo",
            ),
            build_class_summary(
                article_30d,
                "rotacion_final_30d",
                "stock_actual_total",
                "lineas_pi_30d",
                "cantidad_pi_30d",
                label="articulo_unico",
            ),
        ],
        ignore_index=True,
    )

    quarterly_summaries: list[pd.DataFrame] = []
    for period_label in owner_quarterly["period_label"].drop_duplicates().tolist():
        quarterly_summaries.append(
            build_class_summary(
                owner_quarterly[owner_quarterly["period_label"] == period_label],
                "rotacion_final_trimestre",
                "stock_actual",
                "lineas_pi_trimestre",
                "cantidad_pi_trimestre",
                label="owner-articulo",
                period_label=period_label,
            )
        )

    for period_label in article_quarterly["period_label"].drop_duplicates().tolist():
        quarterly_summaries.append(
            build_class_summary(
                article_quarterly[article_quarterly["period_label"] == period_label],
                "rotacion_final_trimestre",
                "stock_actual_total",
                "lineas_pi_trimestre",
                "cantidad_pi_trimestre",
                label="articulo_unico",
                period_label=period_label,
            )
        )

    summary_quarterly = pd.concat(quarterly_summaries, ignore_index=True) if quarterly_summaries else pd.DataFrame()
    return summary_current, summary_article, summary_30d, summary_quarterly


def build_criteria_sheet(snapshot_date: pd.Timestamp, stock_file: Path) -> pd.DataFrame:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rolling_start = (snapshot_date - pd.Timedelta(days=ROLLING_DAYS - 1)).strftime("%Y-%m-%d")
    return pd.DataFrame(
        {
            "Parametro": [
                "Fecha foto stock",
                "Fichero stock detectado",
                "Periodo YTD",
                "Periodo rolling 30 días",
                "Regla recién llegado",
                "Regla Pareto",
                "Definición trimestre",
                "Cobertura líneas 30d",
                "Cobertura cantidad 30d",
                "Inactivo 30d",
                "Inactivo 90d",
                "Dispersión stock",
                "Densidad stock",
                "Flag sobrestock",
                "Flag reubicar",
                "Fecha/hora ejecución",
            ],
            "Valor": [
                snapshot_date.strftime("%Y-%m-%d"),
                stock_file.name,
                f"{snapshot_date.year}-01-01 a {snapshot_date.strftime('%Y-%m-%d')}",
                f"{rolling_start} a {snapshot_date.strftime('%Y-%m-%d')}",
                f"Sin PI en el periodo y con última CR en los últimos {RECENT_DAYS} días respecto al fin del periodo",
                "A hasta 80%, B hasta 95%, C resto, D si no rota",
                "Se genera automáticamente cada trimestre disponible hasta la fecha de la foto; el trimestre actual se corta en la fecha de la foto",
                "stock_actual / lineas_pi_30d, con control de división por cero",
                "stock_actual / cantidad_pi_30d, con control de división por cero",
                "Sí cuando la última PI histórica está a más de 30 días o no existe",
                "Sí cuando la última PI histórica está a más de 90 días o no existe",
                "Número de ubicaciones con stock actual",
                "stock_actual / ubicaciones_con_stock",
                "Heurística moderada: stock en cuartil alto, rotación C/D, sin entrada reciente y sin actividad en 90 días",
                "Heurística moderada: rotación A/B con dispersión alta y densidad baja",
                now,
            ],
        }
    )


def build_dashboard_datasets(
    detail_stock: pd.DataFrame,
    article_ytd: pd.DataFrame,
    article_30d: pd.DataFrame,
    owner_quarterly: pd.DataFrame,
    article_quarterly: pd.DataFrame,
    quarterly_changes: pd.DataFrame,
    snapshot_date: pd.Timestamp,
    generated_at: str,
) -> dict[str, pd.DataFrame]:
    line_ytd = f"lineas_pi_{snapshot_date.year}"
    qty_ytd = f"cantidad_pi_{snapshot_date.year}"
    last_ytd = f"ultima_pi_{snapshot_date.year}"
    days_ytd = f"dias_desde_ultima_pi_{snapshot_date.year}"

    owner_current = detail_stock.copy()
    owner_current["article_key"] = owner_current["articulo"]
    owner_current["id_owner_article"] = owner_current["owner_key"].astype(str) + "|" + owner_current["article_key"].astype(str)
    owner_current = owner_current.rename(
        columns={
            line_ytd: "lineas_pi_ytd",
            qty_ytd: "cantidad_pi_ytd",
            "porcentaje_lineas_pi": "porcentaje_lineas_pi_ytd",
            "porcentaje_acumulado_pi": "porcentaje_acumulado_pi_ytd",
            "rotacion_abc": "rotacion_abc_ytd",
            "rotacion_final": "rotacion_final_ytd",
            "criterio_rotacion": "criterio_rotacion_ytd",
            last_ytd: "ultima_pi_ytd",
            days_ytd: "dias_desde_ultima_pi_ytd",
        }
    )
    owner_current = add_metadata_columns(owner_current, snapshot_date, generated_at)
    owner_current = owner_current[
        [
            "snapshot_date",
            "generated_at",
            "id_owner_article",
            "propietario",
            "owner_key",
            "articulo",
            "article_key",
            "descripcion",
            "stock_actual",
            "ubicaciones_con_stock",
            "lineas_pi_ytd",
            "cantidad_pi_ytd",
            "porcentaje_lineas_pi_ytd",
            "porcentaje_acumulado_pi_ytd",
            "rotacion_abc_ytd",
            "rotacion_final_ytd",
            "criterio_rotacion_ytd",
            "ultima_pi_ytd",
            "dias_desde_ultima_pi_ytd",
            "ultima_cr",
            "dias_desde_ultima_cr",
            "lineas_cr_historico",
            "cantidad_cr_historico",
            "lineas_pi_30d",
            "cantidad_pi_30d",
            "porcentaje_lineas_pi_30d",
            "porcentaje_acumulado_pi_30d",
            "rotacion_abc_30d",
            "rotacion_final_30d",
            "ultima_pi_30d",
            "dias_desde_ultima_pi_30d",
            "cobertura_lineas_30d",
            "cobertura_cantidad_30d",
            "inactivo_30d",
            "inactivo_90d",
            "dispersion_stock",
            "densidad_stock",
            "flag_sobrestock",
            "flag_reubicar",
        ]
    ]
    require_columns(owner_current, owner_current.columns.tolist(), "stock_abc_actual_owner_article")

    article_current = article_ytd.copy()
    article_current["article_key"] = article_current["articulo"]
    article_current["id_article"] = article_current["article_key"]
    article_current["ubicaciones_con_stock"] = article_current.get("ubicaciones_con_stock", article_current["dispersion_stock"])
    article_current = article_current.merge(
        article_30d[
            [
                "articulo",
                "lineas_pi_30d",
                "cantidad_pi_30d",
                "porcentaje_lineas_pi_30d",
                "porcentaje_acumulado_pi_30d",
                "rotacion_abc_30d",
                "rotacion_final_30d",
                "ultima_pi_30d",
                "dias_desde_ultima_pi_30d",
            ]
        ],
        how="left",
        on="articulo",
        suffixes=("", "_30d_merge"),
    )
    article_current = add_metadata_columns(article_current, snapshot_date, generated_at)
    article_current = article_current[
        [
            "snapshot_date",
            "generated_at",
            "id_article",
            "articulo",
            "article_key",
            "descripcion",
            "propietarios_distintos",
            "stock_actual_total",
            "ubicaciones_con_stock",
            "lineas_pi_ytd",
            "cantidad_pi_ytd",
            "porcentaje_lineas_pi_ytd",
            "porcentaje_acumulado_pi_ytd",
            "rotacion_abc_ytd",
            "rotacion_final_ytd",
            "criterio_rotacion_ytd",
            "ultima_pi_ytd",
            "dias_desde_ultima_pi_ytd",
            "ultima_cr",
            "dias_desde_ultima_cr",
            "lineas_cr_historico",
            "cantidad_cr_historico",
            "lineas_pi_30d",
            "cantidad_pi_30d",
            "porcentaje_lineas_pi_30d",
            "porcentaje_acumulado_pi_30d",
            "rotacion_abc_30d",
            "rotacion_final_30d",
            "ultima_pi_30d",
            "dias_desde_ultima_pi_30d",
            "cobertura_lineas_30d",
            "cobertura_cantidad_30d",
            "inactivo_30d",
            "inactivo_90d",
            "dispersion_stock",
            "densidad_stock",
            "flag_sobrestock",
            "flag_reubicar",
        ]
    ]

    quarterly_owner = owner_quarterly.copy()
    quarterly_owner["article_key"] = quarterly_owner["articulo"]
    quarterly_owner["id_owner_article"] = quarterly_owner["owner_key"].astype(str) + "|" + quarterly_owner["article_key"].astype(str)
    quarterly_owner = quarterly_owner.rename(
        columns={
            "stock_actual": "stock_actual_foto",
            "ubicaciones_con_stock": "ubicaciones_con_stock_foto",
        }
    )
    quarterly_owner = add_metadata_columns(quarterly_owner, snapshot_date, generated_at)
    quarterly_owner = quarterly_owner[
        [
            "period_label",
            "year",
            "quarter",
            "snapshot_date",
            "generated_at",
            "id_owner_article",
            "propietario",
            "owner_key",
            "articulo",
            "article_key",
            "descripcion",
            "lineas_pi_trimestre",
            "cantidad_pi_trimestre",
            "porcentaje_lineas_pi_trimestre",
            "porcentaje_acumulado_pi_trimestre",
            "rotacion_abc_trimestre",
            "rotacion_final_trimestre",
            "criterio_rotacion_trimestre",
            "ultima_pi_trimestre",
            "ultima_cr_hasta_fin_trimestre",
            "stock_actual_foto",
            "ubicaciones_con_stock_foto",
        ]
    ]

    quarterly_article = article_quarterly.copy()
    quarterly_article["article_key"] = quarterly_article["articulo"]
    quarterly_article["id_article"] = quarterly_article["article_key"]
    quarterly_article = quarterly_article.rename(
        columns={
            "stock_actual_total": "stock_actual_total_foto",
            "ubicaciones_con_stock": "ubicaciones_con_stock_foto",
        }
    )
    quarterly_article = add_metadata_columns(quarterly_article, snapshot_date, generated_at)
    quarterly_article = quarterly_article[
        [
            "period_label",
            "year",
            "quarter",
            "snapshot_date",
            "generated_at",
            "id_article",
            "articulo",
            "article_key",
            "descripcion",
            "propietarios_distintos",
            "lineas_pi_trimestre",
            "cantidad_pi_trimestre",
            "porcentaje_lineas_pi_trimestre",
            "porcentaje_acumulado_pi_trimestre",
            "rotacion_abc_trimestre",
            "rotacion_final_trimestre",
            "criterio_rotacion_trimestre",
            "ultima_pi_trimestre",
            "ultima_cr_hasta_fin_trimestre",
            "stock_actual_total_foto",
            "ubicaciones_con_stock_foto",
        ]
    ]

    quarterly_changes_clean = quarterly_changes.copy()
    quarterly_changes_clean["article_key"] = quarterly_changes_clean["articulo"]
    quarterly_changes_clean["id_article"] = quarterly_changes_clean["article_key"]
    quarterly_changes_clean = quarterly_changes_clean.rename(columns={"stock_actual_total": "stock_actual_total_foto"})
    quarterly_changes_clean = add_metadata_columns(quarterly_changes_clean, snapshot_date, generated_at)
    quarterly_changes_clean = quarterly_changes_clean[
        [
            "period_label",
            "year",
            "quarter",
            "snapshot_date",
            "generated_at",
            "id_article",
            "articulo",
            "article_key",
            "descripcion",
            "rotacion_trimestre_anterior",
            "rotacion_final_trimestre",
            "cambio_abc",
            "sentido_cambio",
            "lineas_pi_trimestre",
            "cantidad_pi_trimestre",
            "propietarios_distintos",
            "stock_actual_total_foto",
        ]
    ]

    return {
        "stock_abc_actual_owner_article": owner_current,
        "stock_abc_actual_article": article_current,
        "stock_abc_historico_trimestral_owner_article": quarterly_owner,
        "stock_abc_historico_trimestral_article": quarterly_article,
        "stock_abc_cambios_trimestrales": quarterly_changes_clean,
    }


def build_kpi_json(
    owner_current: pd.DataFrame,
    article_current: pd.DataFrame,
    quarterly_article: pd.DataFrame,
    snapshot_date: pd.Timestamp,
    generated_at: str,
) -> dict[str, object]:
    def count_class(frame: pd.DataFrame, class_col: str, class_value: str) -> int:
        return int(frame[class_col].eq(class_value).sum())

    def stock_class(frame: pd.DataFrame, class_col: str, stock_col: str, class_value: str) -> float:
        return float(frame.loc[frame[class_col] == class_value, stock_col].sum())

    top_ytd = article_current.sort_values(
        ["lineas_pi_ytd", "cantidad_pi_ytd", "stock_actual_total"],
        ascending=[False, False, False],
    ).head(10)
    top_30d = article_current.sort_values(
        ["lineas_pi_30d", "cantidad_pi_30d", "stock_actual_total"],
        ascending=[False, False, False],
    ).head(10)

    ultimo_trimestre = None
    if not quarterly_article.empty:
        ultimo = quarterly_article.sort_values(["year", "quarter"]).iloc[-1]
        ultimo_trimestre = str(ultimo["period_label"])

    return {
        "snapshot_date": snapshot_date.strftime("%Y-%m-%d"),
        "generated_at": generated_at,
        "total_referencias_owner_article": int(len(owner_current)),
        "total_referencias_articulo": int(len(article_current)),
        "stock_total_owner_article": float(owner_current["stock_actual"].sum()),
        "stock_total_articulo": float(article_current["stock_actual_total"].sum()),
        "referencias_A_ytd": count_class(article_current, "rotacion_final_ytd", "A"),
        "referencias_B_ytd": count_class(article_current, "rotacion_final_ytd", "B"),
        "referencias_C_ytd": count_class(article_current, "rotacion_final_ytd", "C"),
        "referencias_D_ytd": count_class(article_current, "rotacion_final_ytd", "D"),
        "referencias_recien_llegado_ytd": count_class(article_current, "rotacion_final_ytd", "Sin rotación, recién llegado"),
        "stock_A_ytd": stock_class(article_current, "rotacion_final_ytd", "stock_actual_total", "A"),
        "stock_B_ytd": stock_class(article_current, "rotacion_final_ytd", "stock_actual_total", "B"),
        "stock_C_ytd": stock_class(article_current, "rotacion_final_ytd", "stock_actual_total", "C"),
        "stock_D_ytd": stock_class(article_current, "rotacion_final_ytd", "stock_actual_total", "D"),
        "stock_recien_llegado_ytd": stock_class(article_current, "rotacion_final_ytd", "stock_actual_total", "Sin rotación, recién llegado"),
        "referencias_A_30d": count_class(article_current, "rotacion_final_30d", "A"),
        "referencias_B_30d": count_class(article_current, "rotacion_final_30d", "B"),
        "referencias_C_30d": count_class(article_current, "rotacion_final_30d", "C"),
        "referencias_D_30d": count_class(article_current, "rotacion_final_30d", "D"),
        "referencias_recien_llegado_30d": count_class(article_current, "rotacion_final_30d", "Sin rotación 30d, recién llegado"),
        "articulos_flag_sobrestock": int(article_current["flag_sobrestock"].eq("Sí").sum()),
        "articulos_flag_reubicar": int(article_current["flag_reubicar"].eq("Sí").sum()),
        "top_10_articulos_ytd": json.loads(
            top_ytd[
                ["articulo", "descripcion", "lineas_pi_ytd", "cantidad_pi_ytd", "rotacion_final_ytd", "stock_actual_total"]
            ].to_json(orient="records", force_ascii=False)
        ),
        "top_10_articulos_30d": json.loads(
            top_30d[
                ["articulo", "descripcion", "lineas_pi_30d", "cantidad_pi_30d", "rotacion_final_30d", "stock_actual_total"]
            ].to_json(orient="records", force_ascii=False)
        ),
        "ultimo_trimestre_disponible": ultimo_trimestre,
    }


def build_temporality_criteria_sheet(snapshot_date: pd.Timestamp) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Parametro": [
                "Horizonte temporal",
                "Señal de demanda",
                "Unidad principal de análisis",
                "Definición Regular",
                "Definición Estacional",
                "Definición Intermitente",
                "Definición Errático",
                "Definición Dormido",
                "Definición Nuevo / sin histórico suficiente",
                "Definición warning rotación baja pero estacional",
                "Definición warning stock dormido real",
                "Definición mover a almacén secundario",
                "Definición mover solo fuera de temporada",
                "Definición mantener en almacén principal",
                "Definición revisar manualmente",
                "Limitaciones",
            ],
            "Valor": [
                f"Histórico PI hasta {snapshot_date.strftime('%Y-%m-%d')} con perfiles mensuales, trimestrales y rolling implícito de 12 meses",
                "Movimientos PI agregados por artículo único; CR se usa como contexto de entradas en la capa ABC existente",
                "Artículo único (article_key), ignorando propietario en el patrón principal",
                "ADI < 1.32, CV2 < 0.49 y sin señales fuertes de concentración estacional",
                "Concentración alta en meses o trimestres concretos, índice estacional elevado y recurrencia suficiente entre años",
                "ADI >= 1.32 y CV2 < 0.49, con demanda espaciada pero no especialmente volátil",
                "CV2 >= 0.49 o patrón lumpiness, con variabilidad alta entre periodos activos",
                "Sin actividad PI en los últimos 12 meses y con histórico muy débil o claramente agotado",
                "Sin PI histórico o con menos de 3 meses activos / menos de 6 meses observados efectivos",
                "SKU con rotación baja actual pero patrón estacional recurrente y pico futuro identificable",
                "SKU con stock actual, nula actividad reciente y sin evidencia de patrón recurrente aprovechable",
                "Baja rotación YTD y 30d, sobrestock o cobertura alta, inactividad reciente, baja recurrencia y sin pico próximo esperado",
                "Patrón estacional claro, fuera de temporada actual y con necesidad de reposición antes del siguiente pico",
                "Actividad actual alta o pico próximo esperado con riesgo alto de romper servicio si se traslada",
                "Poco histórico, patrón ambiguo o comportamiento errático no robusto para automatizar decisión",
                "Heurística explicable, no modelo predictivo. Conviene revisar junto con contexto comercial, campañas y capacidad real de almacenes.",
            ],
        }
    )


def build_temporality_outputs(
    movements: pd.DataFrame,
    article_current: pd.DataFrame,
    snapshot_date: pd.Timestamp,
    generated_at: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, object], pd.DataFrame]:
    article_base = article_current[
        [
            "id_article",
            "articulo",
            "article_key",
            "descripcion",
            "rotacion_final_ytd",
            "rotacion_final_30d",
            "stock_actual_total",
            "cobertura_lineas_30d",
            "cobertura_cantidad_30d",
            "inactivo_30d",
            "inactivo_90d",
            "propietarios_distintos",
            "flag_sobrestock",
            "flag_reubicar",
        ]
    ].drop_duplicates().copy()

    pi = movements[movements[C.movement_type] == "PI"].copy()
    if pi.empty:
        empty_summary = add_metadata_columns(article_base.copy(), snapshot_date, generated_at)
        return (
            {
                "stock_abc_temporalidad_article": empty_summary,
                "stock_abc_temporalidad_monthly_article": pd.DataFrame(),
                "stock_abc_temporalidad_quarterly_article": pd.DataFrame(),
                "stock_abc_decision_almacen_article": pd.DataFrame(),
            },
            {
                "snapshot_date": snapshot_date.strftime("%Y-%m-%d"),
                "generated_at": generated_at,
                "numero_articulos_regulares": 0,
                "numero_articulos_estacionales": 0,
                "numero_articulos_intermitentes": 0,
                "numero_articulos_dormidos": 0,
                "numero_candidatos_mover": 0,
                "numero_candidatos_mantener": 0,
                "numero_candidatos_mover_fuera_temporada": 0,
                "top_estacionales": [],
                "top_dormidos": [],
                "top_riesgo_mala_decision_si_se_mueven": [],
                "proximos_meses_pico_detectados": [],
                "proximos_trimestres_pico_detectados": [],
            },
            build_temporality_criteria_sheet(snapshot_date),
        )

    pi["month_period"] = pi["fecha_movimiento"].dt.to_period("M")
    pi["quarter_period"] = pi["fecha_movimiento"].dt.to_period("Q")

    monthly_actual = (
        pi.groupby(["article_key", "month_period"], dropna=False)
        .agg(
            lineas_pi_mes=("article_key", "size"),
            cantidad_pi_mes=("cantidad_movimiento", "sum"),
        )
        .reset_index()
    )
    monthly_actual["year"] = monthly_actual["month_period"].dt.year
    monthly_actual["month"] = monthly_actual["month_period"].dt.month
    monthly_actual["period_label"] = monthly_actual["month_period"].astype(str)

    quarterly_actual = (
        pi.groupby(["article_key", "quarter_period"], dropna=False)
        .agg(
            lineas_pi_trimestre=("article_key", "size"),
            cantidad_pi_trimestre=("cantidad_movimiento", "sum"),
        )
        .reset_index()
    )
    quarterly_actual["year"] = quarterly_actual["quarter_period"].dt.year
    quarterly_actual["quarter"] = quarterly_actual["quarter_period"].dt.quarter
    quarterly_actual["period_label"] = quarterly_actual["quarter_period"].astype(str)

    start_month = pi["month_period"].min()
    month_range = pd.period_range(start=start_month, end=snapshot_date.to_period("M"), freq="M")
    month_dim = pd.DataFrame({"month_period": month_range})
    month_dim["year"] = month_dim["month_period"].dt.year
    month_dim["month"] = month_dim["month_period"].dt.month
    month_dim["period_label"] = month_dim["month_period"].astype(str)

    start_quarter = pi["quarter_period"].min()
    quarter_range = pd.period_range(start=start_quarter, end=snapshot_date.to_period("Q"), freq="Q")
    quarter_dim = pd.DataFrame({"quarter_period": quarter_range})
    quarter_dim["year"] = quarter_dim["quarter_period"].dt.year
    quarter_dim["quarter"] = quarter_dim["quarter_period"].dt.quarter
    quarter_dim["period_label"] = quarter_dim["quarter_period"].astype(str)

    monthly_full = article_base[["id_article", "articulo", "article_key", "descripcion"]].merge(month_dim, how="cross")
    monthly_full = monthly_full.merge(
        monthly_actual[["article_key", "month_period", "lineas_pi_mes", "cantidad_pi_mes"]],
        how="left",
        on=["article_key", "month_period"],
    )
    monthly_full["lineas_pi_mes"] = pd.to_numeric(monthly_full["lineas_pi_mes"], errors="coerce").fillna(0)
    monthly_full["cantidad_pi_mes"] = pd.to_numeric(monthly_full["cantidad_pi_mes"], errors="coerce").fillna(0)

    quarterly_full = article_base[["id_article", "articulo", "article_key", "descripcion"]].merge(quarter_dim, how="cross")
    quarterly_full = quarterly_full.merge(
        quarterly_actual[["article_key", "quarter_period", "lineas_pi_trimestre", "cantidad_pi_trimestre"]],
        how="left",
        on=["article_key", "quarter_period"],
    )
    quarterly_full["lineas_pi_trimestre"] = pd.to_numeric(quarterly_full["lineas_pi_trimestre"], errors="coerce").fillna(0)
    quarterly_full["cantidad_pi_trimestre"] = pd.to_numeric(quarterly_full["cantidad_pi_trimestre"], errors="coerce").fillna(0)

    hist = (
        pi.groupby("article_key", dropna=False)
        .agg(
            first_pi_date=("fecha_movimiento", "min"),
            last_pi_date=("fecha_movimiento", "max"),
            years_with_activity=("year", "nunique"),
            total_lineas_pi_historico=("article_key", "size"),
            total_cantidad_pi_historico=("cantidad_movimiento", "sum"),
        )
        .reset_index()
    )

    monthly_activity = (
        monthly_actual.groupby("article_key", dropna=False)
        .agg(total_months_with_pi=("month_period", "nunique"))
        .reset_index()
    )
    quarterly_activity = (
        quarterly_actual.groupby("article_key", dropna=False)
        .agg(total_quarters_with_pi=("quarter_period", "nunique"))
        .reset_index()
    )

    monthly_agg = (
        monthly_full.groupby("article_key", dropna=False)
        .agg(
            lineas_media_mensual=("lineas_pi_mes", "mean"),
            cantidad_media_mensual=("cantidad_pi_mes", "mean"),
            lineas_max_mes=("lineas_pi_mes", "max"),
            cantidad_max_mes=("cantidad_pi_mes", "max"),
        )
        .reset_index()
    )

    month_profile = (
        monthly_full.groupby(["article_key", "month"], dropna=False)
        .agg(
            lineas_mes_media=("lineas_pi_mes", "mean"),
            cantidad_mes_media=("cantidad_pi_mes", "mean"),
            lineas_mes_total=("lineas_pi_mes", "sum"),
            cantidad_mes_total=("cantidad_pi_mes", "sum"),
            years_activo_mes=("lineas_pi_mes", lambda s: int((s > 0).sum())),
        )
        .reset_index()
    )
    quarter_profile = (
        quarterly_full.groupby(["article_key", "quarter"], dropna=False)
        .agg(
            lineas_trimestre_media=("lineas_pi_trimestre", "mean"),
            cantidad_trimestre_media=("cantidad_pi_trimestre", "mean"),
            lineas_trimestre_total=("lineas_pi_trimestre", "sum"),
            cantidad_trimestre_total=("cantidad_pi_trimestre", "sum"),
            years_activo_trimestre=("lineas_pi_trimestre", lambda s: int((s > 0).sum())),
        )
        .reset_index()
    )

    lineas_media_lookup = monthly_agg.set_index("article_key")["lineas_media_mensual"]
    monthly_full["indice_vs_media"] = safe_divide(
        monthly_full["lineas_pi_mes"],
        monthly_full["article_key"].map(lineas_media_lookup),
    ).fillna(0)
    lineas_media_quarter_lookup = (
        quarterly_full.groupby("article_key", dropna=False)["lineas_pi_trimestre"].mean()
    )
    quarterly_full["indice_vs_media"] = safe_divide(
        quarterly_full["lineas_pi_trimestre"],
        quarterly_full["article_key"].map(lineas_media_quarter_lookup),
    ).fillna(0)

    summary = article_base.merge(hist, how="left", on="article_key")
    summary = summary.merge(monthly_activity, how="left", on="article_key")
    summary = summary.merge(quarterly_activity, how="left", on="article_key")
    summary = summary.merge(monthly_agg, how="left", on="article_key")

    quarter_agg = (
        quarterly_full.groupby("article_key", dropna=False)
        .agg(
            lineas_pi_trimestre_media=("lineas_pi_trimestre", "mean"),
            cantidad_pi_trimestre_media=("cantidad_pi_trimestre", "mean"),
        )
        .reset_index()
    )
    summary = summary.merge(quarter_agg, how="left", on="article_key")

    summary["total_months_with_pi"] = pd.to_numeric(summary["total_months_with_pi"], errors="coerce").fillna(0).astype(int)
    summary["total_quarters_with_pi"] = pd.to_numeric(summary["total_quarters_with_pi"], errors="coerce").fillna(0).astype(int)
    summary["total_lineas_pi_historico"] = pd.to_numeric(summary["total_lineas_pi_historico"], errors="coerce").fillna(0)
    summary["total_cantidad_pi_historico"] = pd.to_numeric(summary["total_cantidad_pi_historico"], errors="coerce").fillna(0)
    summary["years_with_activity"] = pd.to_numeric(summary["years_with_activity"], errors="coerce").fillna(0).astype(int)
    summary["lineas_media_mensual"] = pd.to_numeric(summary["lineas_media_mensual"], errors="coerce").fillna(0)
    summary["cantidad_media_mensual"] = pd.to_numeric(summary["cantidad_media_mensual"], errors="coerce").fillna(0)
    summary["lineas_max_mes"] = pd.to_numeric(summary["lineas_max_mes"], errors="coerce").fillna(0)
    summary["cantidad_max_mes"] = pd.to_numeric(summary["cantidad_max_mes"], errors="coerce").fillna(0)
    summary["lineas_pi_trimestre_media"] = pd.to_numeric(summary["lineas_pi_trimestre_media"], errors="coerce").fillna(0)
    summary["cantidad_pi_trimestre_media"] = pd.to_numeric(summary["cantidad_pi_trimestre_media"], errors="coerce").fillna(0)

    month_peak = month_profile.sort_values(["article_key", "lineas_mes_media", "cantidad_mes_media"], ascending=[True, False, False])
    month_peak = month_peak.groupby("article_key", dropna=False).head(1)[["article_key", "month"]].rename(columns={"month": "mes_pico_lineas"})
    month_peak_qty = month_profile.sort_values(["article_key", "cantidad_mes_media", "lineas_mes_media"], ascending=[True, False, False])
    month_peak_qty = month_peak_qty.groupby("article_key", dropna=False).head(1)[["article_key", "month"]].rename(columns={"month": "mes_pico_cantidad"})
    quarter_peak = quarter_profile.sort_values(["article_key", "lineas_trimestre_media"], ascending=[True, False])
    quarter_peak = quarter_peak.groupby("article_key", dropna=False).head(1)[["article_key", "quarter"]].rename(columns={"quarter": "quarter_peak"})
    summary = summary.merge(month_peak, how="left", on="article_key").merge(month_peak_qty, how="left", on="article_key").merge(quarter_peak, how="left", on="article_key")

    top_month_concentration = (
        month_profile.sort_values(["article_key", "lineas_mes_total"], ascending=[True, False])
        .groupby("article_key", dropna=False)["lineas_mes_total"]
        .agg(
            top1=lambda s: float(s.head(1).sum()),
            top2=lambda s: float(s.head(2).sum()),
        )
        .reset_index()
    )
    top_quarter_concentration = (
        quarter_profile.sort_values(["article_key", "lineas_trimestre_total"], ascending=[True, False])
        .groupby("article_key", dropna=False)["lineas_trimestre_total"]
        .agg(
            top1=lambda s: float(s.head(1).sum()),
            top2=lambda s: float(s.head(2).sum()),
        )
        .reset_index()
    )
    summary = summary.merge(top_month_concentration, how="left", on="article_key").merge(
        top_quarter_concentration, how="left", on="article_key", suffixes=("_mes", "_trimestre")
    )
    summary["porcentaje_concentracion_top_1_mes"] = safe_divide(summary["top1_mes"], summary["total_lineas_pi_historico"]).fillna(0)
    summary["porcentaje_concentracion_top_2_meses"] = safe_divide(summary["top2_mes"], summary["total_lineas_pi_historico"]).fillna(0)
    summary["porcentaje_concentracion_top_1_trimestre"] = safe_divide(summary["top1_trimestre"], summary["total_lineas_pi_historico"]).fillna(0)
    summary["porcentaje_concentracion_top_2_trimestres"] = safe_divide(summary["top2_trimestre"], summary["total_lineas_pi_historico"]).fillna(0)
    summary["porcentaje_concentracion_top_trimestre"] = summary["porcentaje_concentracion_top_1_trimestre"]
    peak_month_mean = (
        month_profile.groupby("article_key", dropna=False)["lineas_mes_media"]
        .max()
        .reset_index(name="peak_month_mean")
    )
    peak_quarter_mean = (
        quarter_profile.groupby("article_key", dropna=False)["lineas_trimestre_media"]
        .max()
        .reset_index(name="peak_quarter_mean")
    )
    summary = summary.merge(peak_month_mean, how="left", on="article_key").merge(
        peak_quarter_mean, how="left", on="article_key"
    )
    summary["seasonality_index_monthly"] = safe_divide(summary["peak_month_mean"], summary["lineas_media_mensual"]).fillna(0)
    summary["seasonality_index_quarterly"] = safe_divide(
        summary["peak_quarter_mean"],
        summary["lineas_pi_trimestre_media"],
    ).fillna(0)

    summary["mes_pico_lineas"] = summary["mes_pico_lineas"].map(lambda value: MONTH_NAMES_ES.get(int(value)) if pd.notna(value) else None)
    summary["mes_pico_cantidad"] = summary["mes_pico_cantidad"].map(lambda value: MONTH_NAMES_ES.get(int(value)) if pd.notna(value) else None)
    summary["quarter_peak"] = summary["quarter_peak"].map(lambda value: quarter_label(int(value)) if pd.notna(value) else None)

    peak_month_candidates = month_profile.merge(
        summary[["article_key", "years_with_activity", "lineas_media_mensual"]],
        how="left",
        on="article_key",
    )
    peak_month_candidates["recurrence_ratio"] = safe_divide(
        peak_month_candidates["years_activo_mes"],
        peak_month_candidates["years_with_activity"],
    ).fillna(0)
    peak_month_candidates["es_mes_pico"] = (
        (peak_month_candidates["lineas_mes_media"] >= peak_month_candidates["lineas_media_mensual"] * 1.25)
        & (peak_month_candidates["recurrence_ratio"] >= 0.5)
        & (peak_month_candidates["lineas_mes_total"] > 0)
    )
    month_peaks = (
        peak_month_candidates[peak_month_candidates["es_mes_pico"]]
        .groupby("article_key", dropna=False)
        .agg(
            peak_months=("month", lambda s: sorted({int(value) for value in s})),
            monthly_peak_recurrence=("recurrence_ratio", "mean"),
        )
        .reset_index()
    )

    peak_quarter_candidates = quarter_profile.merge(
        summary[["article_key", "years_with_activity", "lineas_pi_trimestre_media"]],
        how="left",
        on="article_key",
    )
    peak_quarter_candidates["recurrence_ratio"] = safe_divide(
        peak_quarter_candidates["years_activo_trimestre"],
        peak_quarter_candidates["years_with_activity"],
    ).fillna(0)
    peak_quarter_candidates["es_trimestre_pico"] = (
        (peak_quarter_candidates["lineas_trimestre_media"] >= peak_quarter_candidates["lineas_pi_trimestre_media"] * 1.20)
        & (peak_quarter_candidates["recurrence_ratio"] >= 0.5)
        & (peak_quarter_candidates["lineas_trimestre_total"] > 0)
    )
    quarter_peaks = (
        peak_quarter_candidates[peak_quarter_candidates["es_trimestre_pico"]]
        .groupby("article_key", dropna=False)
        .agg(
            peak_quarters=("quarter", lambda s: sorted({int(value) for value in s})),
            quarterly_peak_recurrence=("recurrence_ratio", "mean"),
        )
        .reset_index()
    )

    summary = summary.merge(month_peaks, how="left", on="article_key").merge(quarter_peaks, how="left", on="article_key")
    summary["peak_months"] = summary["peak_months"].apply(lambda value: value if isinstance(value, list) else [])
    summary["peak_quarters"] = summary["peak_quarters"].apply(lambda value: value if isinstance(value, list) else [])
    summary["monthly_peak_recurrence"] = pd.to_numeric(summary["monthly_peak_recurrence"], errors="coerce").fillna(0)
    summary["quarterly_peak_recurrence"] = pd.to_numeric(summary["quarterly_peak_recurrence"], errors="coerce").fillna(0)
    summary["recurrencia_estacional"] = summary[["monthly_peak_recurrence", "quarterly_peak_recurrence"]].max(axis=1)
    summary["meses_pico_recurrentes"] = summary["peak_months"].apply(month_name_list)
    summary["trimestres_pico_recurrentes"] = summary["peak_quarters"].apply(quarter_name_list)

    monthly_full = monthly_full.merge(
        summary[["article_key", "peak_months", "lineas_media_mensual"]],
        how="left",
        on="article_key",
    )
    monthly_full["es_mes_pico"] = monthly_full.apply(
        lambda row: int(row["month"]) in (row["peak_months"] if isinstance(row["peak_months"], list) else []),
        axis=1,
    )
    monthly_full = add_metadata_columns(monthly_full.drop(columns=["peak_months", "month_period"]), snapshot_date, generated_at)
    monthly_full = monthly_full[
        [
            "snapshot_date",
            "generated_at",
            "id_article",
            "articulo",
            "article_key",
            "descripcion",
            "year",
            "month",
            "period_label",
            "lineas_pi_mes",
            "cantidad_pi_mes",
            "indice_vs_media",
            "es_mes_pico",
        ]
    ]

    quarterly_full = quarterly_full.merge(
        summary[["article_key", "peak_quarters", "lineas_pi_trimestre_media"]],
        how="left",
        on="article_key",
    )
    quarterly_full["es_trimestre_pico"] = quarterly_full.apply(
        lambda row: int(row["quarter"]) in (row["peak_quarters"] if isinstance(row["peak_quarters"], list) else []),
        axis=1,
    )
    quarterly_full = add_metadata_columns(quarterly_full.drop(columns=["peak_quarters", "quarter_period"]), snapshot_date, generated_at)
    quarterly_full = quarterly_full[
        [
            "snapshot_date",
            "generated_at",
            "id_article",
            "articulo",
            "article_key",
            "descripcion",
            "year",
            "quarter",
            "period_label",
            "lineas_pi_trimestre",
            "cantidad_pi_trimestre",
            "indice_vs_media",
            "es_trimestre_pico",
        ]
    ]

    first_period = summary["first_pi_date"].dt.to_period("M")
    summary["months_observed"] = first_period.map(
        lambda start: ((snapshot_date.year - start.year) * 12 + snapshot_date.month - start.month + 1) if pd.notna(start) else 0
    )
    summary["months_observed"] = pd.to_numeric(summary["months_observed"], errors="coerce").fillna(0)
    summary["ADI"] = safe_divide(summary["months_observed"], summary["total_months_with_pi"]).fillna(0)

    non_zero_monthly = monthly_full[monthly_full["lineas_pi_mes"] > 0].copy()
    cv2 = (
        non_zero_monthly.groupby("article_key", dropna=False)["lineas_pi_mes"]
        .agg(lambda s: float((s.std(ddof=0) / s.mean()) ** 2) if len(s) > 1 and s.mean() else 0.0)
        .reset_index(name="CV2")
    )
    summary = summary.merge(cv2, how="left", on="article_key")
    summary["CV2"] = pd.to_numeric(summary["CV2"], errors="coerce").fillna(0)

    summary["peak_period_upcoming"], summary["months_to_next_peak"] = zip(
        *summary["peak_months"].apply(lambda peaks: compute_next_peak_month(snapshot_date.month, peaks))
    )
    summary["next_peak_quarter"], summary["quarters_to_next_peak"] = zip(
        *summary["peak_quarters"].apply(lambda peaks: compute_next_peak_quarter(snapshot_date.quarter, peaks))
    )

    insufficient_history = (summary["total_lineas_pi_historico"] == 0) | (summary["months_observed"] < 6) | (summary["total_months_with_pi"] < 3)
    dormant_signal = (
        summary["last_pi_date"].isna()
        | (days_since(snapshot_date, summary["last_pi_date"]).fillna(9999) > 365)
    ) & (summary["total_months_with_pi"] <= 6)
    estacional_signal = (
        (summary["years_with_activity"] >= 2)
        & (
            (
                (summary["seasonality_index_monthly"] >= 1.6)
                & (summary["porcentaje_concentracion_top_2_meses"] >= 0.55)
                & (summary["monthly_peak_recurrence"] >= 0.5)
            )
            | (
                (summary["seasonality_index_quarterly"] >= 1.4)
                & (summary["porcentaje_concentracion_top_1_trimestre"] >= 0.45)
                & (summary["quarterly_peak_recurrence"] >= 0.5)
            )
        )
        & ~dormant_signal
    )

    summary["temporalidad_clase"] = "Regular"
    summary.loc[insufficient_history, "temporalidad_clase"] = "Nuevo / sin histórico suficiente"
    summary.loc[~insufficient_history & dormant_signal, "temporalidad_clase"] = "Dormido"
    summary.loc[~insufficient_history & ~dormant_signal & estacional_signal, "temporalidad_clase"] = "Estacional"
    summary.loc[
        ~insufficient_history & ~dormant_signal & ~estacional_signal & (summary["ADI"] >= 1.32) & (summary["CV2"] < 0.49),
        "temporalidad_clase",
    ] = "Intermitente"
    summary.loc[
        ~insufficient_history & ~dormant_signal & ~estacional_signal & (summary["CV2"] >= 0.49),
        "temporalidad_clase",
    ] = "Errático"

    summary["es_estacional"] = summary["temporalidad_clase"].eq("Estacional")
    summary["es_dormido"] = summary["temporalidad_clase"].eq("Dormido")
    summary["es_intermitente"] = summary["temporalidad_clase"].eq("Intermitente")
    summary["es_erratico"] = summary["temporalidad_clase"].eq("Errático")
    summary["warning_rotacion_baja_pero_estacional"] = (
        summary["es_estacional"]
        & summary["rotacion_final_ytd"].isin(["C", "D", "Sin rotación, recién llegado"])
    )
    summary["warning_stock_dormido_real"] = summary["es_dormido"] & (summary["stock_actual_total"] > 0) & summary["inactivo_90d"].eq("Sí")

    low_rotation_ytd = summary["rotacion_final_ytd"].isin(["C", "D", "Sin rotación, recién llegado"])
    low_rotation_30d = summary["rotacion_final_30d"].isin(["C", "D", "Sin rotación 30d, recién llegado"])
    no_recent = summary["inactivo_90d"].eq("Sí")
    overstock = summary["flag_sobrestock"].eq("Sí") | (summary["cobertura_lineas_30d"].fillna(0) >= 12)
    no_peak_soon = summary["months_to_next_peak"].fillna(99) > 2
    high_risk_move = summary["es_estacional"] | summary["warning_rotacion_baja_pero_estacional"] | summary["rotacion_final_30d"].isin(["A", "B"])

    summary["riesgo_mover_almacen"] = "Medio"
    summary.loc[summary["es_dormido"] & no_peak_soon, "riesgo_mover_almacen"] = "Bajo"
    summary.loc[high_risk_move | summary["temporalidad_clase"].eq("Regular"), "riesgo_mover_almacen"] = "Alto"

    summary["accion_recomendada"] = "Revisar manualmente"
    summary.loc[
        summary["rotacion_final_30d"].isin(["A", "B"]) | summary["rotacion_final_ytd"].isin(["A", "B"]) | (summary["months_to_next_peak"].fillna(99) <= 2),
        "accion_recomendada",
    ] = "Mantener en almacén principal"
    summary.loc[
        summary["es_estacional"] & low_rotation_ytd & low_rotation_30d & no_peak_soon,
        "accion_recomendada",
    ] = "Mover solo fuera de temporada"
    summary.loc[
        ~summary["es_estacional"] & low_rotation_ytd & low_rotation_30d & overstock & no_recent & (summary["recurrencia_estacional"] < 0.4),
        "accion_recomendada",
    ] = "Mover a almacén secundario"
    summary.loc[
        summary["temporalidad_clase"].isin(["Nuevo / sin histórico suficiente", "Errático"]),
        "accion_recomendada",
    ] = "Revisar manualmente"

    summary["motivo_recomendacion"] = "Patrón ambiguo o poco robusto; conviene contraste manual."
    summary.loc[
        summary["accion_recomendada"].eq("Mantener en almacén principal"),
        "motivo_recomendacion",
    ] = "Actividad actual relevante o pico próximo esperado; moverlo ahora aumenta riesgo operativo."
    summary.loc[
        summary["accion_recomendada"].eq("Mover solo fuera de temporada"),
        "motivo_recomendacion",
    ] = "SKU estacional con rotación baja fuera de temporada y recurrencia suficiente para planificar su retorno antes del pico."
    summary.loc[
        summary["accion_recomendada"].eq("Mover a almacén secundario"),
        "motivo_recomendacion",
    ] = "Baja rotación actual, sin actividad reciente ni patrón estacional fuerte, con señales de sobrestock o cobertura alta."

    summary["ventana_reubicacion_recomendada"] = "Revisión manual en próximo ciclo mensual"
    summary.loc[
        summary["accion_recomendada"].eq("Mantener en almacén principal"),
        "ventana_reubicacion_recomendada",
    ] = "Sin traslado recomendado"
    summary.loc[
        summary["accion_recomendada"].eq("Mover a almacén secundario"),
        "ventana_reubicacion_recomendada",
    ] = "Traslado viable de inmediato"
    summary.loc[
        summary["accion_recomendada"].eq("Mover solo fuera de temporada"),
        "ventana_reubicacion_recomendada",
    ] = summary["peak_period_upcoming"].fillna(summary["next_peak_quarter"]).map(
        lambda value: f"Fuera de temporada; reponer 1-2 meses antes de {value}" if pd.notna(value) else "Fuera de temporada; revisar antes del siguiente pico"
    )

    summary["prioridad_revision"] = "Media"
    summary.loc[summary["riesgo_mover_almacen"].eq("Alto"), "prioridad_revision"] = "Alta"
    summary.loc[summary["riesgo_mover_almacen"].eq("Bajo"), "prioridad_revision"] = "Baja"

    summary["es_candidato_mover"] = summary["accion_recomendada"].eq("Mover a almacén secundario")
    summary["es_candidato_mover_fuera_temporada"] = summary["accion_recomendada"].eq("Mover solo fuera de temporada")
    summary["es_candidato_mantener"] = summary["accion_recomendada"].eq("Mantener en almacén principal")

    temporality_article = add_metadata_columns(summary.copy(), snapshot_date, generated_at)
    temporality_article = temporality_article[
        [
            "snapshot_date",
            "generated_at",
            "id_article",
            "articulo",
            "article_key",
            "descripcion",
            "first_pi_date",
            "last_pi_date",
            "years_with_activity",
            "total_months_with_pi",
            "total_quarters_with_pi",
            "total_lineas_pi_historico",
            "total_cantidad_pi_historico",
            "lineas_media_mensual",
            "cantidad_media_mensual",
            "lineas_max_mes",
            "cantidad_max_mes",
            "mes_pico_lineas",
            "mes_pico_cantidad",
            "lineas_pi_trimestre_media",
            "cantidad_pi_trimestre_media",
            "quarter_peak",
            "porcentaje_concentracion_top_trimestre",
            "porcentaje_concentracion_top_1_mes",
            "porcentaje_concentracion_top_2_meses",
            "porcentaje_concentracion_top_1_trimestre",
            "porcentaje_concentracion_top_2_trimestres",
            "seasonality_index_monthly",
            "seasonality_index_quarterly",
            "recurrencia_estacional",
            "meses_pico_recurrentes",
            "trimestres_pico_recurrentes",
            "ADI",
            "CV2",
            "temporalidad_clase",
            "es_estacional",
            "es_dormido",
            "es_intermitente",
            "es_erratico",
            "peak_period_upcoming",
            "months_to_next_peak",
            "warning_rotacion_baja_pero_estacional",
            "warning_stock_dormido_real",
        ]
    ]

    decision_article = add_metadata_columns(summary.copy(), snapshot_date, generated_at)
    decision_article = decision_article[
        [
            "snapshot_date",
            "generated_at",
            "id_article",
            "articulo",
            "article_key",
            "descripcion",
            "rotacion_final_ytd",
            "rotacion_final_30d",
            "stock_actual_total",
            "cobertura_lineas_30d",
            "cobertura_cantidad_30d",
            "inactivo_30d",
            "inactivo_90d",
            "total_lineas_pi_historico",
            "years_with_activity",
            "temporalidad_clase",
            "seasonality_index_quarterly",
            "recurrencia_estacional",
            "riesgo_mover_almacen",
            "accion_recomendada",
            "motivo_recomendacion",
            "ventana_reubicacion_recomendada",
            "prioridad_revision",
            "es_estacional",
            "es_dormido",
            "es_intermitente",
            "es_erratico",
            "es_candidato_mover",
            "es_candidato_mover_fuera_temporada",
            "es_candidato_mantener",
            "peak_period_upcoming",
            "months_to_next_peak",
            "warning_rotacion_baja_pero_estacional",
            "warning_stock_dormido_real",
        ]
    ].rename(columns={"total_lineas_pi_historico": "lineas_pi_historico"})

    top_estacionales = temporality_article[temporality_article["temporalidad_clase"] == "Estacional"].sort_values(
        ["recurrencia_estacional", "seasonality_index_quarterly"],
        ascending=[False, False],
    ).head(10)
    top_dormidos = decision_article[decision_article["warning_stock_dormido_real"]].sort_values(
        ["stock_actual_total", "lineas_pi_historico"],
        ascending=[False, True],
    ).head(10)
    top_riesgo = decision_article[decision_article["riesgo_mover_almacen"] == "Alto"].sort_values(
        ["months_to_next_peak", "stock_actual_total"],
        ascending=[True, False],
    ).head(10)

    temporality_json = {
        "snapshot_date": snapshot_date.strftime("%Y-%m-%d"),
        "generated_at": generated_at,
        "numero_articulos_regulares": int((temporality_article["temporalidad_clase"] == "Regular").sum()),
        "numero_articulos_estacionales": int((temporality_article["temporalidad_clase"] == "Estacional").sum()),
        "numero_articulos_intermitentes": int((temporality_article["temporalidad_clase"] == "Intermitente").sum()),
        "numero_articulos_dormidos": int((temporality_article["temporalidad_clase"] == "Dormido").sum()),
        "numero_candidatos_mover": int(decision_article["es_candidato_mover"].sum()),
        "numero_candidatos_mantener": int(decision_article["es_candidato_mantener"].sum()),
        "numero_candidatos_mover_fuera_temporada": int(decision_article["es_candidato_mover_fuera_temporada"].sum()),
        "top_estacionales": json.loads(
            top_estacionales[
                ["articulo", "descripcion", "recurrencia_estacional", "seasonality_index_quarterly", "peak_period_upcoming"]
            ].to_json(orient="records", force_ascii=False)
        ),
        "top_dormidos": json.loads(
            top_dormidos[
                ["articulo", "descripcion", "stock_actual_total", "accion_recomendada", "warning_stock_dormido_real"]
            ].to_json(orient="records", force_ascii=False)
        ),
        "top_riesgo_mala_decision_si_se_mueven": json.loads(
            top_riesgo[
                ["articulo", "descripcion", "riesgo_mover_almacen", "accion_recomendada", "peak_period_upcoming", "months_to_next_peak"]
            ].to_json(orient="records", force_ascii=False)
        ),
        "proximos_meses_pico_detectados": sorted(
            {value for value in temporality_article["peak_period_upcoming"].dropna().tolist() if value}
        ),
        "proximos_trimestres_pico_detectados": sorted(
            {value for value in summary["next_peak_quarter"].dropna().tolist() if value}
        ),
    }

    temporality_datasets = {
        "stock_abc_temporalidad_article": temporality_article,
        "stock_abc_temporalidad_monthly_article": monthly_full,
        "stock_abc_temporalidad_quarterly_article": quarterly_full,
        "stock_abc_decision_almacen_article": decision_article,
    }
    return temporality_datasets, temporality_json, build_temporality_criteria_sheet(snapshot_date)


def build_audit_excel(
    detail_stock: pd.DataFrame,
    summary_current: pd.DataFrame,
    criteria: pd.DataFrame,
    article_ytd: pd.DataFrame,
    owner_30d: pd.DataFrame,
    article_30d: pd.DataFrame,
    owner_quarterly: pd.DataFrame,
    article_quarterly: pd.DataFrame,
    quarterly_changes: pd.DataFrame,
    summary_article: pd.DataFrame,
    summary_30d: pd.DataFrame,
    summary_quarterly: pd.DataFrame,
    temporality_article: pd.DataFrame,
    temporality_monthly: pd.DataFrame,
    temporality_quarterly: pd.DataFrame,
    decision_article: pd.DataFrame,
    temporalidad_criteria: pd.DataFrame,
) -> None:
    owner_quarterly_audit = owner_quarterly.rename(
        columns={
            "stock_actual": "stock_actual_foto",
            "ubicaciones_con_stock": "ubicaciones_con_stock_foto",
        }
    )
    article_quarterly_audit = article_quarterly.rename(
        columns={
            "stock_actual_total": "stock_actual_total_foto",
            "ubicaciones_con_stock": "ubicaciones_con_stock_foto",
        }
    )
    quarterly_changes_audit = quarterly_changes.rename(columns={"stock_actual_total": "stock_actual_total_foto"})

    with pd.ExcelWriter(AUDIT_FILE, engine="openpyxl") as writer:
        detail_stock.to_excel(writer, sheet_name="Detalle stock", index=False)
        summary_current.to_excel(writer, sheet_name="Resumen", index=False)
        criteria.to_excel(writer, sheet_name="Criterios", index=False)
        article_ytd.to_excel(writer, sheet_name="ABC articulo unico YTD", index=False)
        owner_30d.to_excel(writer, sheet_name="ABC 30d owner-articulo", index=False)
        article_30d.to_excel(writer, sheet_name="ABC articulo unico 30d", index=False)
        owner_quarterly_audit.to_excel(writer, sheet_name="ABC trimestral owner-articulo", index=False)
        article_quarterly_audit.to_excel(writer, sheet_name="ABC trimestral articulo", index=False)
        quarterly_changes_audit.to_excel(writer, sheet_name="Cambios ABC trimestral", index=False)
        summary_article.to_excel(writer, sheet_name="Resumen articulo unico", index=False)
        summary_30d.to_excel(writer, sheet_name="Resumen 30d", index=False)
        summary_quarterly.to_excel(writer, sheet_name="Resumen trimestral", index=False)
        temporality_article.to_excel(writer, sheet_name="Temporalidad articulo", index=False)
        temporality_monthly.to_excel(writer, sheet_name="Temporalidad mensual articulo", index=False)
        temporality_quarterly.to_excel(writer, sheet_name="Temporalidad trim articulo", index=False)
        decision_article.to_excel(writer, sheet_name="Decision almacen articulo", index=False)
        temporalidad_criteria.to_excel(writer, sheet_name="Criterios temporalidad", index=False)


def save_outputs(
    datasets: dict[str, pd.DataFrame],
    kpis: dict[str, object],
    temporality_datasets: dict[str, pd.DataFrame],
    temporality_kpis: dict[str, object],
    detail_stock: pd.DataFrame,
    summary_current: pd.DataFrame,
    criteria: pd.DataFrame,
    article_ytd: pd.DataFrame,
    owner_30d: pd.DataFrame,
    article_30d: pd.DataFrame,
    owner_quarterly: pd.DataFrame,
    article_quarterly: pd.DataFrame,
    quarterly_changes: pd.DataFrame,
    summary_article: pd.DataFrame,
    summary_30d: pd.DataFrame,
    summary_quarterly: pd.DataFrame,
    temporalidad_criteria: pd.DataFrame,
) -> None:
    ensure_output_dirs()

    for dataset_name, frame in datasets.items():
        frame.to_parquet(PARQUET_DIR / f"{dataset_name}.parquet", index=False)
    for dataset_name, frame in temporality_datasets.items():
        frame.to_parquet(PARQUET_DIR / f"{dataset_name}.parquet", index=False)

    with (JSON_DIR / "stock_abc_resumen_kpis.json").open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(kpis, handle, ensure_ascii=False, indent=2)
    with (JSON_DIR / "stock_abc_resumen_temporalidad.json").open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(temporality_kpis, handle, ensure_ascii=False, indent=2)

    build_audit_excel(
        detail_stock,
        summary_current,
        criteria,
        article_ytd,
        owner_30d,
        article_30d,
        owner_quarterly,
        article_quarterly,
        quarterly_changes,
        summary_article,
        summary_30d,
        summary_quarterly,
        temporality_datasets["stock_abc_temporalidad_article"],
        temporality_datasets["stock_abc_temporalidad_monthly_article"],
        temporality_datasets["stock_abc_temporalidad_quarterly_article"],
        temporality_datasets["stock_abc_decision_almacen_article"],
        temporalidad_criteria,
    )


def main() -> None:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stock_file = detect_stock_file(Path.cwd())
    snapshot_date = parse_snapshot_date(stock_file)
    if snapshot_date is None:
        raise ValueError(f"No se ha podido obtener la fecha de foto desde {stock_file.name}")

    movements = prepare_movements(snapshot_date)
    owner_article_stock, article_only_stock = prepare_stock(stock_file)
    owner_dim, article_dim = build_dimensions(movements, owner_article_stock, article_only_stock)

    detail_stock = build_owner_article_ytd(movements, owner_article_stock, snapshot_date)
    article_ytd = build_article_only_ytd(movements, article_only_stock, snapshot_date)
    owner_30d = build_owner_article_30d(movements, owner_article_stock, snapshot_date)
    article_30d = build_article_only_30d(movements, article_only_stock, snapshot_date)
    owner_quarterly, article_quarterly = build_quarterly_outputs(
        movements,
        owner_dim,
        article_dim,
        snapshot_date,
    )
    quarterly_changes = build_quarterly_change_output(article_quarterly)
    summary_current, summary_article, summary_30d, summary_quarterly = build_summaries(
        detail_stock,
        article_ytd,
        owner_30d,
        article_30d,
        owner_quarterly,
        article_quarterly,
        snapshot_date,
    )
    criteria = build_criteria_sheet(snapshot_date, stock_file)

    datasets = build_dashboard_datasets(
        detail_stock,
        article_ytd,
        article_30d,
        owner_quarterly,
        article_quarterly,
        quarterly_changes,
        snapshot_date,
        generated_at,
    )
    kpis = build_kpi_json(
        datasets["stock_abc_actual_owner_article"],
        datasets["stock_abc_actual_article"],
        datasets["stock_abc_historico_trimestral_article"],
        snapshot_date,
        generated_at,
    )
    temporality_datasets, temporality_kpis, temporalidad_criteria = build_temporality_outputs(
        movements,
        datasets["stock_abc_actual_article"],
        snapshot_date,
        generated_at,
    )

    save_outputs(
        datasets,
        kpis,
        temporality_datasets,
        temporality_kpis,
        detail_stock,
        summary_current,
        criteria,
        article_ytd,
        owner_30d,
        article_30d,
        owner_quarterly,
        article_quarterly,
        quarterly_changes,
        summary_article,
        summary_30d,
        summary_quarterly,
        temporalidad_criteria,
    )

    print(f"Datasets Parquet generados en: {PARQUET_DIR}")
    print(f"JSON KPI generado en: {JSON_DIR / 'stock_abc_resumen_kpis.json'}")
    print(f"JSON temporalidad generado en: {JSON_DIR / 'stock_abc_resumen_temporalidad.json'}")
    print(f"Excel de auditoría generado en: {AUDIT_FILE}")
    print(f"Fichero de stock usado: {stock_file.name}")
    print(f"Fecha foto de stock: {snapshot_date.date()}")


if __name__ == "__main__":
    main()
