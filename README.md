# ABC - Analisis ABC, Temporalidad y Decision Logistica

## Que hace este repositorio

Este repositorio genera una capa analitica completa para:

- clasificar SKU por rotacion ABC
- detectar productos recien llegados
- analizar comportamiento rolling 30 dias
- construir historico trimestral
- medir cambios de clase ABC entre trimestres
- detectar temporalidad / estacionalidad por SKU
- recomendar decisiones logisticas de almacen
- exportar datasets listos para dashboard
- exportar un Excel de auditoria para revision manual

La logica principal usa:

- `PI` como senal de salida / rotacion
- `CR` como senal de entrada
- la fecha de la foto de stock como referencia temporal del analisis

## Estructura del repo

```text
abc/
|-- analisis_rotacion_abc_2026.py
|-- movimientos.xlsx
|-- 31-03-2026.xlsx
|-- output/
|   |-- parquet/
|   |-- json/
|   `-- auditoria/
|-- .gitattributes
|-- .gitignore
`-- README.md
```

## Que es cada archivo

### Codigo

- [`analisis_rotacion_abc_2026.py`](/C:/Users/rdiezl/Desktop/abc/analisis_rotacion_abc_2026.py)
  Script principal. Lee movimientos y stock, calcula ABC, temporalidad, decision logistica y genera todos los outputs.

### Entradas

- [`movimientos.xlsx`](/C:/Users/rdiezl/Desktop/abc/movimientos.xlsx)
  Historico de movimientos. Es obligatorio y debe llamarse exactamente asi.

- [`31-03-2026.xlsx`](/C:/Users/rdiezl/Desktop/abc/31-03-2026.xlsx)
  Foto de stock actual. El script detecta automaticamente cualquier fichero con formato `dd-mm-yyyy.xlsx` y usa el mas reciente.

### Configuracion Git

- [`.gitattributes`](/C:/Users/rdiezl/Desktop/abc/.gitattributes)
  Fija finales de linea y tipos binarios para que el repo funcione bien entre Windows y macOS.

- [`.gitignore`](/C:/Users/rdiezl/Desktop/abc/.gitignore)
  Excluye artefactos locales que no deben versionarse, como `__pycache__`.

### Salidas

- [`output/parquet/`](/C:/Users/rdiezl/Desktop/abc/output/parquet)
  Datasets principales para backend / app web / dashboard.

- [`output/json/`](/C:/Users/rdiezl/Desktop/abc/output/json)
  JSON de KPIs y resumenes rapidos para tarjetas y widgets.

- [`output/auditoria/`](/C:/Users/rdiezl/Desktop/abc/output/auditoria)
  Excel de auditoria para validacion manual.

## Requisitos

### Python

Recomendado:

- Python 3.11 o 3.12

### Librerias necesarias

Instalar como minimo:

- `pandas`
- `openpyxl`
- `pyarrow`

Comando:

```bash
pip install pandas openpyxl pyarrow
```

## Como ejecutarlo en Windows

### 1. Crear entorno virtual

```powershell
python -m venv .venv
```

### 2. Activarlo

```powershell
.\.venv\Scripts\Activate.ps1
```

### 3. Instalar dependencias

```powershell
pip install pandas openpyxl pyarrow
```

### 4. Ejecutar desde la raiz del repo

```powershell
python .\analisis_rotacion_abc_2026.py
```

## Como ejecutarlo en macOS

### 1. Crear entorno virtual

```bash
python3 -m venv .venv
```

### 2. Activarlo

```bash
source .venv/bin/activate
```

### 3. Instalar dependencias

```bash
pip install pandas openpyxl pyarrow
```

### 4. Ejecutar desde la raiz del repo

```bash
python3 analisis_rotacion_abc_2026.py
```

## Regla importante para no perder informacion

El script regenera siempre los mismos ficheros dentro de `output/`.

Eso significa:

- si vuelves a ejecutar con una foto nueva, se sobreescriben los outputs anteriores
- los datos fuente no se borran
- el historico que se usa en el analisis sale de `movimientos.xlsx`

Para no perder trazabilidad:

1. conserva cada foto de stock con su nombre de fecha
2. versiona la carpeta `output/` si quieres mover exactamente las salidas entre Windows y macOS
3. haz commit despues de cada ejecucion relevante
4. si quieres guardar varias corridas en paralelo, copia `output/` a una carpeta con fecha antes de volver a ejecutar

Ejemplo:

```text
output_2026-03-31/
output_2026-04-30/
```

## Como detecta la foto de stock

El script busca en la raiz del repo un fichero Excel con nombre:

```text
dd-mm-yyyy.xlsx
```

Ejemplos validos:

- `31-03-2026.xlsx`
- `30-04-2026.xlsx`

Reglas:

- ignora `movimientos.xlsx`
- ignora el Excel de auditoria
- usa la foto con fecha mas reciente

## Inputs minimos obligatorios

### 1. Movimientos

Debe existir:

- `movimientos.xlsx`

Campos esperados mas importantes:

- `Tipo movimiento`
- `Fecha inicio`
- `Articulo`
- `Denominacion articulo`
- `Cantidad`
- `Propietario`

### 2. Stock

Debe existir al menos una foto de stock con fecha en el nombre.

Campos esperados mas importantes:

- `Denominacion propietario`
- `Propie.`
- `Art._y`
- `Denominacion`
- `Stock pal.`
- `Ubicacion`
- `Ocupacion`

## Salidas principales para dashboard

### 1. `stock_abc_actual_owner_article.parquet`

Nivel:

- propietario + articulo

Uso:

- tabla operativa principal actual
- filtros por propietario
- filtros por clase ABC
- filtros por flags
- ranking por owner + sku

Campos clave:

- `id_owner_article`
  Id tecnico estable = `owner_key|article_key`
- `rotacion_final_ytd`
  Clase ABC final YTD
- `rotacion_final_30d`
  Clase ABC final ultimos 30 dias
- `flag_sobrestock`
  Senal de exceso de stock segun heuristica actual
- `flag_reubicar`
  Senal de SKU rapido pero disperso, candidato a mejora operativa

### 2. `stock_abc_actual_article.parquet`

Nivel:

- articulo unico

Uso:

- tabla principal para dashboard de SKU
- base para temporalidad
- analisis transversal ignorando propietario

Campos clave:

- `id_article`
  Id tecnico estable = `article_key`
- `propietarios_distintos`
  Cuantos propietarios distintos tienen ese SKU

### 3. `stock_abc_historico_trimestral_owner_article.parquet`

Nivel:

- propietario + articulo

Uso:

- evolucion trimestral por owner + sku

Importante:

- `stock_actual_foto` y `ubicaciones_con_stock_foto` son datos informativos de la foto actual
- no representan stock historico del trimestre

### 4. `stock_abc_historico_trimestral_article.parquet`

Nivel:

- articulo unico

Uso:

- evolucion historica por SKU
- base para graficas temporales y comparativas por trimestre

### 5. `stock_abc_cambios_trimestrales.parquet`

Uso:

- comparar clase del trimestre actual con la del trimestre anterior
- detectar mejora, deterioro o estabilidad

### 6. `stock_abc_temporalidad_article.parquet`

Uso:

- clasificacion de temporalidad por SKU
- soporte a decision logistica
- detectar productos aparentemente lentos pero realmente estacionales

Campos clave:

- `temporalidad_clase`
  Clase de patron historico del SKU
- `seasonality_index_monthly`
  Intensidad del pico mensual frente a su media
- `seasonality_index_quarterly`
  Intensidad del pico trimestral frente a su media
- `recurrencia_estacional`
  Cuanto se repiten los periodos pico entre anos
- `warning_rotacion_baja_pero_estacional`
  El SKU hoy parece lento, pero historicamente tiene picos recurrentes
- `warning_stock_dormido_real`
  El SKU tiene stock y senales de dormido real

### 7. `stock_abc_temporalidad_monthly_article.parquet`

Uso:

- heatmap mensual por SKU
- deteccion de meses pico
- visuales de patron mensual

### 8. `stock_abc_temporalidad_quarterly_article.parquet`

Uso:

- heatmap trimestral
- patron agregado de demanda
- visuales mas estables que el perfil mensual

### 9. `stock_abc_decision_almacen_article.parquet`

Uso:

- tabla final de recomendacion logistica
- soporte a decision de mover / mantener / revisar

Campos clave:

- `accion_recomendada`
  Recomendacion principal
- `riesgo_mover_almacen`
  Riesgo de tomar una mala decision logistica
- `motivo_recomendacion`
  Explicacion corta y interpretable
- `ventana_reubicacion_recomendada`
  Cuando conviene mover o devolver el producto
- `prioridad_revision`
  Prioridad manual sugerida

## JSON generados

### `stock_abc_resumen_kpis.json`

Uso:

- home del dashboard ABC
- tarjetas KPI
- top articulos por YTD y 30d

### `stock_abc_resumen_temporalidad.json`

Uso:

- home del dashboard de temporalidad
- resumen de articulos regulares, estacionales, dormidos, etc.
- top estacionales
- top dormidos
- top riesgo de mala decision si se mueven

## Excel de auditoria

Fichero:

- [`analisis_rotacion_abc_auditoria.xlsx`](/C:/Users/rdiezl/Desktop/abc/output/auditoria/analisis_rotacion_abc_auditoria.xlsx)

Hojas:

- `Detalle stock`
- `Resumen`
- `Criterios`
- `ABC articulo unico YTD`
- `ABC 30d owner-articulo`
- `ABC articulo unico 30d`
- `ABC trimestral owner-articulo`
- `ABC trimestral articulo`
- `Cambios ABC trimestral`
- `Resumen articulo unico`
- `Resumen 30d`
- `Resumen trimestral`
- `Temporalidad articulo`
- `Temporalidad mensual articulo`
- `Temporalidad trim articulo`
- `Decision almacen articulo`
- `Criterios temporalidad`

Uso:

- revisar manualmente calculos y clasificaciones
- validar resultados antes de llevarlos a produccion
- explicar reglas de negocio a usuarios no tecnicos

## Reglas de negocio actuales

### Regla Pareto ABC

- `A`: hasta el 80% acumulado
- `B`: hasta el 95% acumulado
- `C`: resto
- `D`: sin rotacion en el periodo

### Regla recien llegado

Se marca como recien llegado si:

- no tiene `PI` en el periodo analizado
- pero si tiene `CR` en los ultimos 14 dias respecto al cierre de ese periodo

### Periodos principales

- `YTD`: desde el 1 de enero del ano de la foto hasta `snapshot_date`
- `30d`: rolling de los ultimos 30 dias hasta `snapshot_date`
- `trimestre`: cada trimestre historico detectado automaticamente

## Diccionario visual de parametros

### Identificacion

- `snapshot_date`
  Fecha de la foto de stock usada en la corrida
- `generated_at`
  Fecha y hora de generacion del output
- `owner_key`
  Clave normalizada de propietario
- `article_key`
  Clave normalizada del SKU
- `id_owner_article`
  Identificador tecnico `owner_key|article_key`
- `id_article`
  Identificador tecnico `article_key`

### Rotacion ABC

- `lineas_pi_ytd`
  Numero de lineas `PI` del ano en curso
- `cantidad_pi_ytd`
  Cantidad total movida por `PI` en YTD
- `porcentaje_lineas_pi_ytd`
  Peso del SKU dentro del total de lineas `PI` YTD
- `porcentaje_acumulado_pi_ytd`
  Acumulado Pareto para asignar clase
- `rotacion_abc_ytd`
  Clase A/B/C segun Pareto puro
- `rotacion_final_ytd`
  Clase final considerando tambien `D` y `recien llegado`

- `lineas_pi_30d`
  Numero de lineas `PI` en los ultimos 30 dias
- `cantidad_pi_30d`
  Cantidad total movida en los ultimos 30 dias
- `rotacion_final_30d`
  Clase final rolling 30 dias

### Entradas y actividad

- `ultima_pi_ytd`
  Fecha de la ultima salida PI en YTD
- `ultima_pi_30d`
  Fecha de la ultima salida PI en ventana 30d
- `ultima_cr`
  Fecha de la ultima entrada CR historica hasta la foto
- `dias_desde_ultima_pi_ytd`
  Dias desde la ultima PI YTD
- `dias_desde_ultima_pi_30d`
  Dias desde la ultima PI en 30d
- `dias_desde_ultima_cr`
  Dias desde la ultima entrada CR

### Operativa de stock

- `stock_actual`
  Stock actual por owner + articulo
- `stock_actual_total`
  Stock actual agregado por articulo unico
- `ubicaciones_con_stock`
  Numero de ubicaciones con stock
- `dispersion_stock`
  Equivale a cuantas ubicaciones ocupa el SKU
- `densidad_stock`
  Relacion `stock / ubicaciones`
- `cobertura_lineas_30d`
  Stock actual dividido por lineas 30d
- `cobertura_cantidad_30d`
  Stock actual dividido por cantidad 30d

### Flags operativos

- `inactivo_30d`
  Sin actividad reciente relevante en 30 dias
- `inactivo_90d`
  Sin actividad reciente relevante en 90 dias
- `flag_sobrestock`
  Senal heuristica de exceso de stock
- `flag_reubicar`
  Senal heuristica de SKU rapido pero mal distribuido

### Temporalidad

- `first_pi_date`
  Primera salida PI historica detectada
- `last_pi_date`
  Ultima salida PI historica detectada
- `years_with_activity`
  Anos distintos con actividad
- `total_months_with_pi`
  Numero de meses con actividad PI
- `total_quarters_with_pi`
  Numero de trimestres con actividad PI
- `total_lineas_pi_historico`
  Lineas PI acumuladas historicas
- `total_cantidad_pi_historico`
  Cantidad PI acumulada historica

- `lineas_media_mensual`
  Media mensual historica
- `lineas_max_mes`
  Maximo de lineas en un solo mes
- `mes_pico_lineas`
  Mes historicamente mas fuerte por lineas
- `quarter_peak`
  Trimestre historicamente mas fuerte

- `porcentaje_concentracion_top_1_mes`
  Cuanto pesa el mejor mes sobre el total historico
- `porcentaje_concentracion_top_2_meses`
  Cuanto pesan los dos mejores meses
- `porcentaje_concentracion_top_1_trimestre`
  Cuanto pesa el mejor trimestre
- `seasonality_index_monthly`
  Pico mensual frente a su media
- `seasonality_index_quarterly`
  Pico trimestral frente a su media
- `recurrencia_estacional`
  Repeticion de los picos entre anos
- `meses_pico_recurrentes`
  Lista de meses que se repiten como pico
- `trimestres_pico_recurrentes`
  Lista de trimestres pico recurrentes

### Intermitencia

- `ADI`
  Average Demand Interval. Aproxima cada cuantos meses hay demanda
- `CV2`
  Variabilidad cuadratica de la demanda no nula

Interpretacion orientativa:

- ADI alto = demanda espaciada
- CV2 alto = demanda muy variable

### Clases de temporalidad

- `Regular`
  Demanda mas estable y sin concentracion temporal fuerte
- `Estacional`
  Picos repetitivos y concentrados en ciertos meses / trimestres
- `Intermitente`
  Demanda espaciada, pero no especialmente volatil
- `Erratico`
  Demanda variable y dificil de anticipar
- `Dormido`
  Producto con senales de inactividad real
- `Nuevo / sin historico suficiente`
  No hay suficiente base historica para clasificar con confianza

### Decision logistica

- `accion_recomendada`
  Una de estas:
  - `Mantener en almacen principal`
  - `Mover a almacen secundario`
  - `Mover solo fuera de temporada`
  - `Revisar manualmente`

- `riesgo_mover_almacen`
  Riesgo de que trasladarlo provoque una mala decision operativa

- `motivo_recomendacion`
  Explicacion corta de la decision

- `ventana_reubicacion_recomendada`
  Momento sugerido para mover o devolver stock

- `warning_rotacion_baja_pero_estacional`
  SKU que hoy parece lento pero tiene picos recurrentes

- `warning_stock_dormido_real`
  SKU con stock actual y senales fuertes de dormido real

## Flujo de trabajo recomendado

### Para una corrida nueva

1. copia la nueva foto de stock en la raiz con formato `dd-mm-yyyy.xlsx`
2. actualiza `movimientos.xlsx` si procede
3. ejecuta el script
4. revisa primero:
   - `output/json/stock_abc_resumen_kpis.json`
   - `output/json/stock_abc_resumen_temporalidad.json`
   - `output/auditoria/analisis_rotacion_abc_auditoria.xlsx`
5. si la salida es correcta, haz commit del `output/`

### Para usarlo en dashboard

Consumir prioritariamente:

- Parquet para tablas grandes
- JSON para tarjetas KPI
- Excel solo para auditoria y explicacion de negocio

## Limitaciones actuales

- la logica de temporalidad es heuristica e interpretable, no predictiva
- no usa previsiones externas, calendario comercial ni campañas
- la recomendacion logistica debe validarse con contexto de operacion real
- el script sobreescribe `output/` en cada corrida
- si quieres historico de ejecuciones, conserva snapshots y versiona salidas

## Comandos utiles

### Ejecutar

```bash
python analisis_rotacion_abc_2026.py
```

### Ver estado Git

```bash
git status
```

### Añadir cambios

```bash
git add .
```

### Commit sugerido

```bash
git commit -m "Actualiza outputs ABC y temporalidad"
```

## Resumen rapido

Si solo necesitas acordarte de lo minimo:

1. deja `movimientos.xlsx` actualizado
2. pon la nueva foto como `dd-mm-yyyy.xlsx`
3. ejecuta `python analisis_rotacion_abc_2026.py`
4. usa `output/parquet` y `output/json` para la app
5. usa `output/auditoria/analisis_rotacion_abc_auditoria.xlsx` para revisar manualmente
