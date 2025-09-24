# 🚖 NYC TLC Trip Data Pipeline (2015–2025) con Mage + dbt + Snowflake

Proyecto de **Data Mining** — Universidad San Francisco de Quito.  
Implementa un pipeline completo de ingesta, transformación y modelado dimensional sobre el dataset **NYC TLC Trip Record Data (Yellow/Green 2015–2025)**.

---

## 📌 Resumen

- **Ingesta**: archivos Parquet 2015–2025 de Yellow y Green, cargados a Snowflake (bronze).  
- **Transformaciones**: arquitectura de medallas (`bronze → silver → gold`) con **dbt**.  
- **Orquestación**: **Mage** en Docker ejecuta pipelines de backfill y transformaciones.  
- **Modelo final (Gold)**: tabla de hechos `fct_trips` y dimensiones conformadas (`dim_zone`, `dim_payment_type`, `dim_ratecode`).  
- **Clustering**: aplicado sobre `fct_trips` en Snowflake (por `pickup_datetime`, `pu_zone_sk`).  
- **Calidad**: validaciones dbt (`not_null`, `unique`, `accepted_values`, `relationships`).  
- **Documentación**: diccionario de datos, auditoría de cargas, tests y notebook de análisis con SQL.  

---

## 🎯 Objetivos de aprendizaje

1. Ingerir datos históricos masivos (2015–2025).  
2. Aplicar arquitectura de medallas (bronze, silver, gold).  
3. Evaluar impacto de **clustering** en Snowflake (Query Profile).  
4. Operar secretos y roles con privilegios mínimos en Mage.  
5. Garantizar calidad con **tests dbt**, auditorías y documentación.  

---

## 🏗️ Arquitectura

```mermaid
flowchart TD
    subgraph Mage["Orquestación Mage"]
        A[generate_months (PY)] --> B[fetch_and_stage_parquet (PY)]
        B --> C[snowflake_connection (PY)]
        C --> D[copy_into_bronze (PY)]
        C --> E[load_taxi_zones (PY)]
        D --> Bronze[BRONZE.*]
        E --> Bronze
    end

    subgraph Snowflake["Snowflake Layers"]
        Bronze[BRONZE schema<br/>green_raw, yellow_raw, taxi_zones]
        Lookups[LOOKUPS schema<br/>payment_type_lookup, ratecode_lookup]
        Silver[SILVER schema<br/>silver_trips (VIEW)]
        Gold[GOLD schema<br/>dim_zone, dim_payment_type, dim_ratecode, fct_trips]
    end

    Bronze --> Staging[stg_yellow / stg_green (dbt)]
    Lookups --> Silver
    Bronze --> Silver
    Silver --> Gold
    Lookups --> Gold

    subgraph Audit["AUDIT Layer"]
        M[build_coverage_matrix (PY)] --> N[sync_coverage_to_audit_py (PY)]
        M --> O[update_coverage (PY)]
    end

    Gold --> M
```



- **Bronze (raw)**: datos tal cual del Parquet + metadatos de ingesta (`run_id`, `ingest_ts`).  
- **Silver**: estandarización, limpieza, enriquecimiento con Taxi Zones.  
- **Gold**: modelo en estrella con `fct_trips` y dimensiones conformadas.  
- **Orquestación**: pipelines Mage para ingesta mensual y transformaciones dbt.

### Diagrama orquestacion
```mermaid
flowchart TD
    A[generate_months (PY)] --> B[fetch_and_stage_parquet (PY)]
    B --> C[snowflake_connection (PY)]
    C --> D[copy_into_bronze (PY)]
    C --> E[load_taxi_zones (PY)]

    D --> F[stg_green (DBT)]
    D --> G[stg_yellow (DBT)]

    F --> J[silver_trips (DBT, VIEW)]
    G --> J

    H[payment_type_lookup (DBT)] --> J
    I[ratecode_lookup (DBT)] --> J
    E --> J

    J --> K[dim_zone (DBT)]
    J --> L[dim_payment_type (DBT)]
    J --> M[dim_ratecode (DBT)]

    K --> N[fct_trips (DBT)]
    L --> N
    M --> N

    N --> O[build_coverage_matrix (PY)]
    O --> P[sync_coverage_to_audit_py (PY)]
    O --> Q[update_coverage (PY)]

    Q --> R[dbt_setup (YAML - tests)]

```

---

## 📂 Cobertura (2015–2025)

Matriz de cobertura por año/mes y servicio (Yellow/Green).  
Se documenta si un mes carece de archivo Parquet oficial.

Revisar en docs coverage_matrix.csv

---

## 🔑 Gestión de secretos y roles

### Secrets en Mage
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`

### Roles (mínimos privilegios)
| Rol          | Privilegios mínimos |
|--------------|----------------------|
| svc_ingest   | USAGE en warehouse + database, INSERT en bronze |
| svc_dbt      | USAGE en warehouse, SELECT en bronze/silver, CREATE/INSERT en silver/gold |

📸 Evidencia: capturas de Mage Secrets y Snowflake Roles (sin exponer valores).  

---

## ⚙️ Transformaciones (dbt)

- **Silver**  
  - Limpieza: distancias <0 → null, montos < -50 → null.  
  - Enriquecimiento: unión Yellow+Green, join con Taxi Zones.  
  - Variables de control: `service_type`, `year`, `month`.

- **Gold**  
  - Dimensiones:  
    - `dim_zone` (zonas TLC).  
    - `dim_payment_type` (métodos de pago).  
    - `dim_ratecode` (tipos de tarifa).  
  - Hecho: `fct_trips` (1 fila = 1 viaje).  
  - Relaciones entre SKs y dimensiones.  
  - Deduplicación con `row_number()` (última ingesta prevalece).  

---

## 🧪 Pruebas de calidad (dbt)

- `not_null` y `unique` en SKs (`trip_sk`, `zone_sk`, etc.).  
- `accepted_values` en `payment_type`, `ratecode_id`, `service_type`.  
- `relationships` para validar joins entre hecho y dimensiones.  
- Resultado: **6 PASS / 1 FAIL inicial (trip_sk duplicado)** → corregido con deduplicación en `fct_trips.sql`.  

---

## 📖 Diccionario de datos (Gold)

| Columna          | Descripción | Origen |
|------------------|-------------|--------|
| trip_sk          | Surrogate key estable | Generado en `fct_trips.sql` |
| pu_zone_sk       | Zona de recogida (SK) | `dim_zone` (pu_location_id) |
| do_zone_sk       | Zona de destino (SK) | `dim_zone` (do_location_id) |
| payment_type_sk  | Tipo de pago (SK) | `dim_payment_type` |
| ratecode_sk      | Código de tarifa (SK) | `dim_ratecode` |
| vendor_id        | ID del proveedor | silver_trips |
| pickup_datetime  | Fecha/hora inicio | silver_trips |
| dropoff_datetime | Fecha/hora fin | silver_trips |
| passenger_count  | Número de pasajeros | silver_trips |
| trip_distance    | Distancia (millas) | silver_trips |
| total_amount     | Monto total (USD) | silver_trips |
| tip_amount       | Propina (USD) | silver_trips |
| trip_minutes     | Duración en minutos | calculado en silver |
| service_type     | Yellow/Green | silver_trips |
| year, month      | Año/mes del viaje | silver_trips |

---

## 📊 Auditoría de cargas

Conteos por mes y servicio (`green/yellow`), + % de filas descartadas por reglas de calidad (ej. distancias <0, montos < -50).

Ejemplo (2019):

| Año | Mes | Servicio | N_viajes | % descartados |
|-----|-----|----------|-----------|---------------|
| 2019 | 01 | Yellow | 7,696,617 | 0.4% |
| 2019 | 01 | Green  |   672,105 | 0.3% |

---

## 🔎 Clustering en Snowflake

- **Tabla**: `gold.fct_trips`  
- **Cluster keys**: `(pickup_datetime, pu_zone_sk)`  
- **Antes**: scans completos, sin pruning.  
- **Después**: reducción de micro-partitions (~30% pruning).  
- **Conclusión**: clustering por `pickup_datetime` y `pu_zone_sk` optimiza consultas analíticas frecuentes (ej. demanda por mes y zona).  

📸 Evidencias: Query Profiles antes/después incluidas en `docs/`.  

---

## 📒 Notebook de análisis (`data_analysis.ipynb`)

Consultas SQL (Snowflake Notebook):

1. **Demanda por zona y mes** → top 10 zonas por `pu_zone` y `do_zone`.  
2. **Ingresos y propinas** → ingresos totales + % tip por borough y mes.  
3. **Velocidad y congestión** → mph promedio por franja horaria (día/noche).  
4. **Duración del viaje** → percentiles (p50/p90) de `trip_minutes` por pickup zone.  
5. **Elasticidad temporal** → distribución de viajes por día de semana y hora (picos).  

---

## 📝 Checklist de aceptación

- [x] Datos 2015–2025 (Parquet Yellow/Green) cargados en Bronze.  
- [x] Pipelines Mage orquestan ingesta mensual (idempotencia garantizada).  
- [x] Bronze refleja origen, Silver limpia/unifica, Gold en estrella.  
- [x] Clustering en `fct_trips` con métricas antes/después.  
- [x] Secrets y cuenta de servicio con mínimos privilegios.  
- [x] Tests dbt ejecutados y documentados.  
- [x] Diccionario de datos y auditoría de cargas.  
- [x] Notebook con 5 análisis de negocio desde Gold.  

---

## ⚠️ Troubleshooting

- **Mes faltante**: registrar en la matriz de cobertura (README).  
- **Duplicados en fct_trips**: corregido con deduplicación (`row_number()`).  
- **Errores de permisos Snowflake**: revisar rol de servicio (`USAGE`, `CREATE`, `INSERT`).  
- **Clustering sin efecto**: verificar Query Profile y llaves de cluster.  

---

## 📜 Licencia

MIT © 2025




