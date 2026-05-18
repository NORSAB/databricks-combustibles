# Combustibles en Honduras — Databricks Medallion + Tesis UNAH

Pipeline completo de análisis de precios de combustibles (Super, Regular, Diésel, Kerosene) en Databricks Free Edition.
Incluye la implementación reproducible de la tesis de maestría UNAH (cadenas de Markov, NNLS, SSRC).

## Estructura del Repositorio

```
Combustibles/
├── local/                              ← Notebooks editados localmente
│   ├── 01_bronze_ingesta.py            ← Ingesta CSV → Bronze
│   ├── 02_silver_limpieza.py           ← Limpieza → Silver
│   ├── 03_gold_modelo.py              ← Agregaciones → Gold (operativo)
│   ├── tesis_01_cap2_variantes_tcra.py ← Cap 2: Operador TCRA (αₜ)
│   ├── tesis_02_cap3_markov_kmeans.py  ← Cap 3: K-Medias + Markov
│   ├── tesis_03_cap4_nnls_hibrido.py   ← Cap 4: NNLS híbrido
│   ├── tesis_04_cap5_deteccion_pronostico.py ← Cap 5: Grid search + pronóstico
│   └── tesis_05_cap6_reservorio_ssrc.py     ← Cap 6: Reservorio SSRC
│
├── web/                                ← Respaldo descargado de Databricks
└── README.md
```

## Pipeline Operativo (Medallion)

| Capa | Notebook | Tabla de Salida | Propósito |
|------|----------|-----------------|-----------|
| **Bronze** | `01_bronze_ingesta.py` | `combustibles_hn.bronze.*` | Ingesta cruda del CSV histórico |
| **Silver** | `02_silver_limpieza.py` | `combustibles_hn.silver.silver_precios_semanales` | Limpieza, tipado, validación |
| **Gold** | `03_gold_modelo.py` | `combustibles_hn.gold.*` | Agregaciones de negocio |

## Pipeline Tesis (Capítulos 2–6)

### Orden de Ejecución

```
tesis_01 → tesis_02 → tesis_03 → tesis_04 → tesis_05
```

> **Prerequisito:** El pipeline operativo (Bronze → Silver) debe ejecutarse primero.

### Notebooks y Tablas Gold

#### `tesis_01` — Cap 2: Operador TCRA (1 tabla)

Calcula la tasa de cambio relativa αₜ para los 4 combustibles.

| Tabla Gold | Descripción |
|------------|-------------|
| `tesis_alphas_combustibles` | Series temporales + αₜ por combustible |

#### `tesis_02` — Cap 3: Cadenas de Markov (6 tablas)

Discretiza αₜ en estados (K-Medias + Cuantiles) y estima matrices de transición P.

| Tabla Gold | Descripción |
|------------|-------------|
| `tesis_alphas_con_estados` | αₜ + estados K-Medias + estados Cuantiles |
| `tesis_cap3_centroides` | Centroides por combustible (K* variable: 3,4,3,5) |
| `tesis_cap3_matrices_transicion` | Matrices P por combustible |
| `tesis_cap3_propiedades_espectrales` | Autovalores, gap espectral, π estacionaria, varianza explicada |
| `tesis_cap5_cuantiles_boundaries` | Umbrales de cuantiles (benchmark) |
| `tesis_cap5_cuantiles_matrices` | Matrices P de cuantiles (benchmark) |

#### `tesis_03` — Cap 4: Modelo Híbrido NNLS (5 tablas)

Validación predictiva NNLS + comparación P̂ vs Ap con umbrales fijos.

| Tabla Gold | Descripción |
|------------|-------------|
| `tesis_cap4_predicciones_nnls` | Probabilidades NNLS por estado |
| `tesis_cap4_rmse_markov_base` | RMSE Markov base (benchmark para Cap 6) |
| `tesis_cap4_predicciones_precio_semanal` | Precio real vs predicho por semana |
| `tesis_cap4_tabla_4_3` | Rendimiento P̂ vs Ap (Max/Avg accuracy) |
| `tesis_cap4_detalle_particiones` | Accuracy por partición (60/40 a 95/5) |

#### `tesis_04` — Cap 5: Detección y Pronóstico (5 tablas)

Grid search (W, λ, K), quiebres estructurales, y pronóstico de la siguiente semana.

| Tabla Gold | Descripción |
|------------|-------------|
| `tesis_cap5_grid_completo` | Resultados completos del grid search |
| `tesis_cap5_best_hyperparams` | Mejores (W*, λ*, K*) por combustible |
| `tesis_cap5_quiebres_estructurales` | Puntos de quiebre detectados |
| `tesis_cap5_significancia_estadistica` | T-test K-Medias vs Cuantiles |
| `tesis_cap5_pronostico_siguiente` | Pronóstico siguiente semana |

#### `tesis_05` — Cap 6: Reservorio SSRC (10 tablas)

Reservoir computing: grid search (D, ρ, a), 30 realizaciones, test de Diebold-Mariano.

| Tabla Gold | Descripción |
|------------|-------------|
| `tesis_cap6_grid_completo` | Grid completo SSRC (D × ρ × a) |
| `tesis_cap6_best_ssrc` | Mejores configs + RMSE Ridge (benchmark) |
| `tesis_cap6_predicciones_semanales` | Real vs Markov vs SSRC por semana |
| `tesis_cap6_comparacion_final` | RMSE + DM test + significancia estadística |
| `tesis_cap6_realizaciones` | 30 realizaciones para boxplot |
| `tesis_cap6_washout_convergencia` | Convergencia del washout |
| `tesis_cap6_verificaciones_teoricas` | ESP, rango W_res, inclusión |
| `tesis_cap6_perturbacion` | Cotas de perturbación |
| `tesis_cap6_autovalores` | Autovalores del reservorio (plano complejo) |
| `tesis_cap6_reservorio_pca` | PCA de estados (PC1, PC2, régimen) |

## Parámetros Globales

| Parámetro | Valor | Uso |
|-----------|-------|-----|
| `SEED` | 42 | Reproducibilidad en todos los notebooks |
| `K*` | {Super: 4, Regular: 3, Diesel: 3, Kerosene: 5} | Regímenes óptimos por combustible |
| `W*` | ~50–52 | Ventana óptima del grid search |
| `λ*` | 0.97–0.99 | Factor de decaimiento óptimo |
| `TRAIN_RATIO` | 0.80 | Partición train/test del reservorio |
| `N_REALIZATIONS` | 30 | Realizaciones para boxplot SSRC |
| `RESERVOIR_WASHOUT` | 50 | Periodos de washout del reservorio |

## Catálogo Databricks

```
combustibles_hn
├── bronze/
├── silver/
│   └── silver_precios_semanales
└── gold/
    ├── tesis_alphas_combustibles          (NB01)
    ├── tesis_alphas_con_estados           (NB02)
    ├── tesis_cap3_centroides              (NB02)
    ├── tesis_cap3_matrices_transicion     (NB02)
    ├── tesis_cap3_propiedades_espectrales (NB02)
    ├── tesis_cap5_cuantiles_boundaries    (NB02)
    ├── tesis_cap5_cuantiles_matrices      (NB02)
    ├── tesis_cap4_predicciones_nnls       (NB03)
    ├── tesis_cap4_rmse_markov_base        (NB03)
    ├── tesis_cap4_predicciones_precio_semanal (NB03)
    ├── tesis_cap4_tabla_4_3               (NB03)
    ├── tesis_cap4_detalle_particiones     (NB03)
    ├── tesis_cap5_grid_completo           (NB04)
    ├── tesis_cap5_best_hyperparams        (NB04)
    ├── tesis_cap5_quiebres_estructurales  (NB04)
    ├── tesis_cap5_significancia_estadistica(NB04)
    ├── tesis_cap5_pronostico_siguiente    (NB04)
    ├── tesis_cap6_grid_completo           (NB05)
    ├── tesis_cap6_best_ssrc               (NB05)
    ├── tesis_cap6_predicciones_semanales  (NB05)
    ├── tesis_cap6_comparacion_final       (NB05)
    ├── tesis_cap6_realizaciones           (NB05)
    ├── tesis_cap6_washout_convergencia    (NB05)
    ├── tesis_cap6_verificaciones_teoricas (NB05)
    ├── tesis_cap6_perturbacion            (NB05)
    ├── tesis_cap6_autovalores             (NB05)
    └── tesis_cap6_reservorio_pca          (NB05)
```

## Flujo de Trabajo

```
┌─────────────────┐     download      ┌─────────────────┐
│   Databricks    │ ──────────────►   │    web/          │
│   Free Edition  │                   │  (respaldo)      │
│   (workspace)   │                   └─────────────────┘
│                 │
│                 │     upload         ┌─────────────────┐
│                 │ ◄──────────────   │    local/        │
└─────────────────┘                   │  (edición)       │
                                      └─────────────────┘
                                            ▲
                                            │ edits
                                      ┌─────────────────┐
                                      │  VS Code /       │
                                      │  Antigravity     │
                                      └─────────────────┘
```

**Última actualización:** Mayo 2026
