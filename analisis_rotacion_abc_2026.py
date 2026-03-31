from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re

import pandas as pd


MOVIMIENTOS_FILE = Path("movimientos.xlsx")
OUTPUT_FILE = Path("analisis_rotacion_abc_2026.xlsx")
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


def detect_stock_file(workdir: Path) -> Path:
    candidates: list[tuple[pd.Timestamp, Path]] = []
    for path in workdir.glob("*.xlsx"):
        if path.name.lower() in {MOVIMIENTOS_FILE.name.lower(), OUTPUT_FILE.name.lower()}:
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


def save_excel(
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
) -> None:
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        detail_stock.to_excel(writer, sheet_name="Detalle stock", index=False)
        summary_current.to_excel(writer, sheet_name="Resumen", index=False)
        criteria.to_excel(writer, sheet_name="Criterios", index=False)
        article_ytd.to_excel(writer, sheet_name="ABC articulo unico YTD", index=False)
        owner_30d.to_excel(writer, sheet_name="ABC 30d owner-articulo", index=False)
        article_30d.to_excel(writer, sheet_name="ABC articulo unico 30d", index=False)
        owner_quarterly.to_excel(writer, sheet_name="ABC trimestral owner-articulo", index=False)
        article_quarterly.to_excel(writer, sheet_name="ABC trimestral articulo", index=False)
        quarterly_changes.to_excel(writer, sheet_name="Cambios ABC trimestral", index=False)
        summary_article.to_excel(writer, sheet_name="Resumen articulo unico", index=False)
        summary_30d.to_excel(writer, sheet_name="Resumen 30d", index=False)
        summary_quarterly.to_excel(writer, sheet_name="Resumen trimestral", index=False)


def main() -> None:
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

    save_excel(
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
    )

    print(f"Archivo generado: {OUTPUT_FILE}")
    print(f"Fichero de stock usado: {stock_file.name}")
    print(f"Fecha foto de stock: {snapshot_date.date()}")


if __name__ == "__main__":
    main()
