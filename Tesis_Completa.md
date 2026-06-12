# GUÍA DE CONTEXTO E IMPLEMENTACIÓN PARA DATABRICKS (IA)
> **Propósito**: Esta sección proporciona recomendaciones para el análisis del documento por parte del motor de IA de Databricks y describe el catálogo de visualizaciones/imágenes de la tesis para facilitar su comprensión, replicación e implementación en código.

---

## 1. Recomendaciones para el procesamiento en Databricks
Para optimizar el uso de este documento como contexto en Databricks (por ejemplo, en Databricks Assistant o AI/BI Genie):
1. **Intérprete Matemático**: Databricks renderiza expresiones matemáticas mediante KaTeX. El formato de fórmulas simples con `$` y fórmulas destacadas con `$$` debe respetarse para evitar errores de parseo en celdas de Markdown.
2. **Mapeo de Variables**: Utiliza la *Tabla de Notación* y el *Glosario de Términos* provistos al inicio de la tesis como un diccionario semántico para mapear fórmulas matemáticas a variables en código de Spark SQL y Python (ej. $\alpha_t$ representa la tasa de cambio relativa óptima).
3. **Estructura de Datos**: Las tablas contenidas en este documento están en formato de tabla Markdown estándar. Se pueden copiar directamente a celdas de Databricks o convertirse a DataFrames usando `spark.createDataFrame()` o cargando el CSV equivalente desde DBFS/Unity Catalog.
4. **Repositorio de Código**: Todo el código de implementación real, los modelos de predicción y los datos limpios están alojados en: [https://github.com/NORSAB/TCRA-Cadenas-Markov-SSRC-Series-Economicas](https://github.com/NORSAB/TCRA-Cadenas-Markov-SSRC-Series-Economicas).

---



\renewcommand{\tablename}{Tabla}
\renewcommand{\listtablename}{Índice de tablas}
\renewcommand\abstractname{Resumen}

\maketitle

\let\cleardoublepage

\begin{center}{
    \large \bf Agradecimientos}
  \end{center}

Agradezco a Dios por acompañarme durante todo este proceso. En los momentos difíciles de la investigación, su guía me dio la claridad que necesitaba para seguir adelante.\\

A mis padres, Wilfredo Sabillón y Ernestina Castro. Cada vez que dudé si podía terminar esto, su confianza me sostuvo. Han sido siempre mi roca firme, motivándome de manera constante a culminar cada meta que me propongo en la vida. No exagero al decir que sin ellos este trabajo no existiría.\\

A Brenda, mi esposa. Hubo semanas enteras en que la pantalla de la computadora recibió más atención que ella, y aun así me apoyó sin reservas. Su paciencia y su amor hicieron posible lo que a ratos parecía imposible; gracias de corazón por cuidar con tanto cariño y detalle los preparativos de la defensa, aportando esa calidez y excelencia para que la presentación estuviera al más alto nivel.\\

A mi hijo, Andre Emiliano. Su sonrisa al final del día me recordaba por qué valía la pena el esfuerzo. Gracias por tu curiosidad y por tus constantes preguntas para entender de qué trataba mi tesis; conversar contigo me enseñó la valiosa lección de que lo técnico y complejo solo cobra verdadero valor cuando somos capaces de explicarlo de forma sencilla.\\

Al Dr. Fredy Vides, mi asesor, quien confió en esta línea de investigación desde el principio y me orientó con rigor y generosidad. Aprendí de él mucho más que matemáticas.\\

Al M.Sc. David Méndez, por haber sido ese docente que me impulsó a tomar cursos adicionales de redacción. Aunque no eran parte obligatoria del plan de estudios, hoy reconozco el inmenso valor de esas herramientas en mi escritura profesional y científica.\\

Al Dr. Leandro Galo, por su genuina preocupación por nosotros como estudiantes y por darme ese aliento decisivo en la etapa final. Su motivación y su seguimiento cercano fueron claves para seguir adelante y culminar este proyecto de vida.\\

A mi compañero Armando Ramos, por su presencia constante y por acompañarme como testigo en la defensa, demostrando el valor de una verdadera amistad.\\

A mis compañeros de maestría, por las discusiones, los ánimos mutuos y la camaradería en los momentos más exigentes. Y a todas las personas que, de una forma u otra, contribuyeron a que esta tesis llegara a buen puerto.

\begin{flushright}
*Norman Reynaldo Sabillón Castro*
\end{flushright}

\let\cleardoublepage

\renewcommand{\abstractname}{Resumen}

\begin{center}
    **MODELADO DE SERIES TEMPORALES ECONÓMICAS: OPERADORES AUTORREGRESIVOS, CADENAS DE MARKOV Y RESERVORIOS ESTOCÁSTICOS**

        \vspace{0.9cm}
    **Resumen**
\end{center}

Modelar regímenes en series temporales económicas enfrenta una dicotomía inherente entre rigidez determinista y opacidad interpretativa probabilística.  Esta tesis propone un marco que conecta ambos enfoques.  La estrategia parte de operadores autorregresivos con propiedades formales demostradas, pasa por una estimación Markoviana convexa y culmina en una extensión no lineal basada en reservorios.

La familia de operadores de Tasa de Cambio Relativa Autorregresiva (TCRA, TCRAM, ETCRA, ETCRAM) convierte series continuas en regímenes económicos discretos e interpretables.  Para cada variante se demuestran existencia, unicidad, consistencia asintótica y estabilidad ante perturbaciones.  Los regímenes resultantes alimentan una cadena de Markov cuya matriz de transición se estima por Mínimos Cuadrados No Negativos (NNLS), formulación convexa que garantiza convergencia global.  El enfoque estándar de máxima verosimilitud en modelos *Markov-Switching* (MS-AR) no ofrece esa garantía.

Un resultado formal, el Teorema de Inclusión, establece que TCRA-Markov es un caso particular de la Computación de Reservorio Estocásticamente Estructurada (SSRC).  Esto abre la vía a dinámicas no lineales sin sacrificar las garantías teóricas del modelo base.

La validación empírica utiliza series semanales de cuatro combustibles hondureños (2017--2025).  La predicción de régimen alcanza entre 85 y 91\,\% de exactitud.  Los cuantiles fijos resultan estadísticamente inferiores ($p < 0.05$, test de Diebold-Mariano) y el modelo de referencia MS-AR no convergió bajo el protocolo de validación y conjunto de datos de este estudio.  Con SSRC, el RMSE se reduce entre 20 y 23\,\% respecto al modelo lineal ($p < 0.05$ en tres de cuatro series).  Los tiempos de mezcla, de 25 a 47 semanas, cuantifican la inercia del mercado regulado hondureño.

**Palabras clave:** TCRA, series temporales, cadenas de Markov, NNLS, cambio de régimen, computación de reservorio, SSRC, combustibles, Honduras.

\let\cleardoublepage

\renewcommand{\abstractname}{Abstract}

\begin{center}
    **ECONOMIC TIME SERIES MODELING: AUTOREGRESSIVE OPERATORS, MARKOV CHAINS, AND STOCHASTIC RESERVOIRS**

        \vspace{0.9cm}
    **Abstract**
\end{center}

Modeling regimes in economic time series involves a well-known dichotomy between deterministic rigidity and probabilistic uninterpretability.  This thesis proposes a unified framework that bridges both approaches.  The strategy builds on autoregressive operators with proven formal properties, convex Markov estimation, and a nonlinear reservoir-based extension.

The Autoregressive Relative Change Rate (TCRA) operator family and its parametric variants (TCRAM, ETCRA, ETCRAM) convert continuous time series into interpretable economic regimes. Existence, uniqueness, asymptotic consistency, and perturbation stability are proved for each variant.  The resulting regimes feed a Markov chain whose transition matrix is estimated via Non-Negative Least Squares (NNLS), a convex formulation that guarantees global convergence.  Standard maximum likelihood in Markov-Switching (MS-AR) models offers no such guarantee.

A formal result, the Inclusion Theorem, establishes that TCRA-Markov is a special case of Stochastically Structured Reservoir Computing (SSRC).  This opens the path to nonlinear dynamics without sacrificing the theoretical guarantees of the base model.  Empirically, the extension yields 20--23\% improvements in forecast accuracy.

Validation uses weekly price series for four Honduran fuels (Regular, Super, Diesel, Kerosene) over 2017--2025.  Regime prediction accuracy reaches 85--91\%.  Fixed quantiles are statistically inferior ($p < 0.05$, Diebold-Mariano test) and the MS-AR benchmark did not converge under our validation protocol and data.  Mixing times of 25 to 47 weeks quantify the inertia of Honduras's regulated fuel market.

**Keywords:** TCRA, time series, Markov chains, NNLS, regime switching, reservoir computing, SSRC, fuel prices, Honduras.

# Glosario de Términos

\item[**TCRA** \(\bigl(\alpha_T\bigr)\)] 
**Tasa de Cambio Relativa Autorregresiva**:  
Modelo que estima la tasa de cambio relativa entre valores consecutivos de una serie temporal mediante la minimización de diferencias cuadradas. Su parámetro principal, $\alpha_T$, representa la variación relativa óptima en la serie y minimiza la suma de diferencias cuadradas entre valores adyacentes.

\item[**TCRAM** \(\bigl(\alpha_T, W\bigr)\)] 
**Tasa de Cambio Relativa Autorregresiva Móvil**:  
Extensión de la TCRA que incorpora una ventana móvil para análisis localizado, capturando dinámicas recientes en series temporales. Se define por dos cantidades:

  * \(\alpha_T\): Tasa que optimiza la relación entre valores consecutivos dentro de la ventana móvil.
  * \(W\): Tamaño de la ventana móvil que define el subconjunto de datos analizado.

\item[**ETCRA** \(\bigl(\alpha_T, \lambda\bigr)\)] 
**Tasa de Cambio Relativa Autorregresiva con Decaimiento Exponencial**:  
Modelo que pondera observaciones recientes mediante un factor de decaimiento exponencial, mejorando la sensibilidad a cambios temporales. La tasa $\alpha_T$ optimiza la relación entre valores consecutivos ajustada por ponderaciones exponenciales, y el factor $\lambda \in (0,1]$ controla cuánto se prioriza el dato reciente sobre el histórico.

\item[**ETCRAM** \(\bigl(\alpha_T, \lambda, W\bigr)\)] 
**Tasa de Cambio Relativa Autorregresiva con Decaimiento Exponencial y Ventana Móvil**:  
Combina la ventana móvil de TCRAM y el decaimiento exponencial de ETCRA para un análisis localizado y ponderado de series temporales. Involucra tres cantidades:

  * \(\alpha_T\): Tasa que optimiza la relación entre valores consecutivos en la ventana móvil, ponderada exponencialmente.
  * \(\lambda\): Factor de decaimiento exponencial para priorizar datos recientes.
  * \(W\): Tamaño de la ventana móvil que segmenta la serie en subconjuntos.

\item[**TCRA-Markov** \(\bigl(\alpha_t, K\bigr)\)] 
**Tasa de Cambio Relativa Autorregresiva con Cadenas de Markov**:  
Extensión de la TCRA que modela tasas de cambio como un proceso de Markov con \(K\) estados, capturando transiciones entre regímenes en series temporales.  Requiere dos elementos:

  * \(\alpha_t\): Tasa de cambio relativa en el tiempo \(t\), modelada como variable de estado en una cadena de Markov.
  * \(K\): Número de estados discretos en el modelo de Markov.

\item[**ARIMA** \(\bigl(p, d, q\bigr)\)] 
**AutoRegressive Integrated Moving Average**:  
Modelo clásico de series temporales que combina componentes autorregresivos, de diferenciación y de promedio móvil para modelar dinámicas temporales.  Los tres órdenes que lo caracterizan son:

  * \(p\): Orden del componente autorregresivo (*AutoRegressive*).
  * \(d\): Grado de diferenciación (*Integrated*) para lograr estacionariedad.
  * \(q\): Orden del componente de promedio móvil (*Moving Average*).

En su variante estacional (SARIMA), se incluyen parámetros \((P, D, Q)\) para modelar estacionalidad.

\item[**Mínimos Cuadrados**] 
Método estadístico que estima parámetros minimizando la suma de los cuadrados de las diferencias entre valores observados y predichos.

\item[**MLE**] 
**Estimación de Máxima Verosimilitud**:  
Técnica estadística que selecciona parámetros maximizando la probabilidad de observar los datos mue
\item[**AIC**] 
**Criterio de Información de Akaike**:  
Método para seleccionar modelos estadísticos, equilibrando el ajuste del modelo y su complejidad mediante la penalización del número de parámetros.

\item[**Cadena de Markov**] 
**Proceso Estocástico de Markov**: 
Modelo matemático que describe una secuencia de eventos donde la probabilidad de cada evento futuro depende únicamente del estado actual. Sus componentes clave son:

    * Espacio de estados (\(S\)): El conjunto finito de todos los posibles regímenes.
    * Matriz de transición (\(P\)): Matriz que contiene las probabilidades de pasar de un estado a otro.

\item[**Codificación indicadora** (*one-hot encoding*)] 
Proceso para convertir variables categóricas (como los estados $s_1$, $s_2$) en vectores binarios (e.g., $[1, 0, 0, 0]^T$), un formato necesario para la estimación matricial.

\item[**Distribución Estacionaria** \(\bigl(\boldsymbol{\pi}\bigr)\)]
Vector de probabilidad que describe la distribución a largo plazo de una cadena de Markov, representando el equilibrio del sistema.

\item[**Continuidad de Lipschitz**]
Propiedad matemática de una función que acota su tasa de cambio. En esta tesis, el estimador $\alpha_t$ satisface una cota de Lipschitz (Lema~4.4), pero la función de cuantización $\pi$ es discontinua y no posee esta propiedad.

\item[**Ergodicidad**]
Propiedad de una cadena de Markov que es irreducible (todo estado se alcanza desde cualquier otro en un número finito de pasos) y aperiódica (el máximo común divisor de los tiempos de retorno a cada estado es 1). Bajo estas condiciones, la cadena posee una única distribución estacionaria $\boldsymbol{\pi}$ y las frecuencias empíricas de visita convergen a $\boldsymbol{\pi}$ independientemente del estado inicial [Cita: Norris1998, Levin2009].

\item[**Norma de Frobenius** \(\bigl(\|\cdot\|_F\bigr)\)]
Para una matriz $A \in \mathbb{R}^{m \times n}$, la norma de Frobenius se define como $\|A\|_F = \sqrt{\sum_{i=1}^{m} \sum_{j=1}^{n} |a_{ij}|^2} = \sqrt{\mathrm{tr}(A^\top A)}$. En esta tesis se utiliza como función objetivo en el problema de estimación NNLS (Ecuación~6.1) para minimizar el error residual matricial.

\item[**SSRC**]
**Computación de Reservorio Estocásticamente Estructurada** (*Stochastically Structured Reservoir Computing*):
Extensión del modelo TCRA-Markov que incorpora una capa de computación de reservorio con estructura estocástica, permitiendo la captura de dinámicas no lineales.

\item[**ESN**]
**Red de Estado de Eco** (*Echo State Network*):
Tipo de red neuronal recurrente donde la matriz de pesos internos (reservorio) es fija y solo se entrena la capa de salida, lo que simplifica el entrenamiento.

\item[**K-medias**]
Algoritmo de agrupamiento que particiona $n$ observaciones en $K$ clústeres minimizando la inercia intra-grupo. En esta tesis, se usa para discretizar la serie continua $\{\alpha_t\}$ en $K$ estados de régimen económico.

\item[**Matriz de Transición** \(\bigl(P\bigr)\)]
Matriz cuadrada estocástica que define las probabilidades de una cadena de Markov. Su elemento principal es:

    * \(P_{ij}\): Probabilidad de transitar al estado \(s_i\) desde el estado \(s_j\).

\item[**NNLS**]
**Mínimos Cuadrados No Negativos**:
Algoritmo de optimización que resuelve un problema de mínimos cuadrados con la restricción de que los coeficientes deben ser no-negativos, asegurando probabilidades válidas.

\item[**Producto de Kronecker** \(\bigl(\otimes\bigr)\)]
Operación entre matrices que resulta en una matriz de bloques más grande, utilizada para linealizar ecuaciones matriciales.

\item[**Vectorización** \(\bigl(\text{vec}(\cdot)\bigr)\)]
Transformación lineal que convierte una matriz en un vector columna apilando sus columnas.

\item[**\(\phi\)-mixing**]
**Mezcla Fuerte (\(\phi\)-mixing)**:
Propiedad estadística que mide la tasa a la que las observaciones lejanas en el tiempo se vuelven independientes, usada para probar propiedades asintóticas.

\item[**MS-AR**]
**Markov-Switching Autoregressive**:
Modelo autorregresivo con cambio de régimen que alterna entre parámetros según un proceso de Markov latente [Cita: hamilton1989new, Hamilton1994]. En esta tesis funciona como modelo de referencia (*benchmark*); su estimación por máxima verosimilitud no convergió en ninguna de las series evaluadas.

\item[**Leaky-ESN**]
**Echo State Network con Integración de Fuga** (*Leaky Integrator Echo State Network*):
Variante de la red de estado de eco que incorpora un factor de integración $a \in (0,1]$ en la actualización del estado del reservorio. En el Capítulo~[Ref: cap:sec_ssrc] se demuestra que satisface la Propiedad de Estado de Eco (ESP) bajo la condición $\rho(\mat{W}_{\text{res}}) < 1$.

\item[**Brecha espectral** \(\bigl(\gamma\bigr)\)]
Diferencia entre el primer y el segundo autovalor en módulo de la matriz de transición $\mat{P}$. Cuantifica la velocidad de convergencia de la cadena de Markov a su distribución estacionaria: una brecha pequeña indica alta inercia de mercado.

\item[**Tiempo de mezcla** \(\bigl(t_{\mathrm{mix}}\bigr)\)]
Número de pasos necesarios para que la cadena de Markov aproxime su distribución estacionaria dentro de un margen $\varepsilon$. En los datos de combustibles hondureños, los tiempos de mezcla estimados oscilan entre 25 y 47 semanas.

\item[**RMSE**]
**Raíz del Error Cuadrático Medio** (*Root Mean Square Error*):
$\mathrm{RMSE} = \sqrt{\frac{1}{n}\sum_{i=1}^n (y_i - \hat{y}_i)^2}$. Métrica primaria de evaluación predictiva en los capítulos empíricos.

\item[**Test de Diebold-Mariano**]
Prueba estadística que compara la capacidad predictiva de dos modelos evaluando si la diferencia de sus errores de pronóstico es significativa [Cita: DieboldMariano1995]. Se emplea en los Capítulos~[Ref: cap:regimenes_combustibles] y~[Ref: cap:sec_ssrc] para validar la superioridad del SSRC frente al modelo Markoviano lineal.

\item[**ESP**]
**Propiedad de Estado de Eco** (*Echo State Property*):
Condición que exige que el estado interno de un reservorio dependa únicamente de la secuencia de entrada y no de la condición inicial. Se garantiza cuando el radio espectral del reservorio satisface $\rho(\mat{W}_{\text{res}}) < 1$.

# Tabla de Notación

\begin{longtable}{@{}clp{8cm}@{}}
\toprule
**Símbolo** & **Tipo** & **Descripción** \\
\midrule
\endfirsthead
\toprule
**Símbolo** & **Tipo** & **Descripción** \\
\midrule
\endhead
\bottomrule
\endfoot

\multicolumn{3}{l}{**Variables y Series**} \\
\midrule
$v(t)$ & Real ($\mathbb{R}$) & Valor de la serie temporal en el instante $t$ \\
$\alpha_t$ & Real ($\mathbb{R}$) & Tasa de cambio relativa óptima (operador TCRA) \\
$\beta_t$ & Real ($\mathbb{R}$) & Factor multiplicativo: $\beta_t = \alpha_t + 1$ \\
$\varepsilon_t$ & Real ($\mathbb{R}$) & Ruido o residuo del modelo AR(1) local \\
$S_t$ & Discreto & Estado del régimen económico en el instante $t$ \\
\midrule

\multicolumn{3}{l}{**Parámetros**} \\
\midrule
$W$ & Entero & Tamaño de la ventana temporal (número de observaciones) \\
$\lambda$ & $\in (0,1]$ & Factor de decaimiento exponencial \\
$K$ & Entero & Número de estados (regímenes) discretos \\
$T$ & Entero & Longitud total de la serie temporal \\
$W_{\text{eff}}$ & Real ($\mathbb{R}$) & Ventana efectiva: $W_{\text{eff}} = (1-\lambda^W)/(1-\lambda)$ \\
\midrule

\multicolumn{3}{l}{**Matrices y Operadores**} \\
\midrule
$P$, $\hat{\mat{P}}$ & $K \times K$ & Matriz de transición verdadera y estimada \\
$C$ & $K \times K$ & Matriz de conteo de transiciones \\
$\pi$ & Función & Función de cuantización $\pi\colon \mathbb{R} \to \mathcal{S}$ \\
$\boldsymbol{\pi}$ & Vector & Distribución estacionaria de la cadena de Markov \\
\midrule

\multicolumn{3}{l}{**Espacios y Conjuntos**} \\
\midrule
$\mathcal{S}$ & Conjunto & Espacio de estados discretos $\{s_1, \ldots, s_K\}$ \\
$\{\theta_j\}$ & Conjunto & Umbrales de la función de cuantización $\pi$ \\
$\delta_{\min}$ & Real ($\mathbb{R}$) & Separación mínima entre umbrales: $\min_j |\theta_{j+1} - \theta_j|$ \\
\midrule

\multicolumn{3}{l}{**Variantes del Operador**} \\
\midrule
TCRA & --- & $W = T$, $\lambda = 1$ (global, caso límite) \\
TCRAM & --- & $W < T$, $\lambda = 1$ (ventana móvil) \\
ETCRA & --- & $W = T$, $\lambda \in (0,1)$ (decaimiento global) \\
ETCRAM & --- & $W < T$, $\lambda \in (0,1)$ (ventana + decaimiento) \\
\midrule

\multicolumn{3}{l}{**Métricas**} \\
\midrule
RMSE & Real ($\mathbb{R}$) & Error cuadrático medio: $\sqrt{\frac{1}{n}\sum (y_i - \hat{y}_i)^2}$ \\
Exactitud & $\in [0,1]$ & Proporción de estados correctamente clasificados \\

\end{longtable}

\smallskip
*Nota:* En todo el documento se adopta el punto como separador decimal, siguiendo la convención habitual en la literatura matemática y econométrica anglosajona.

\fancyhead[L]{\leftmark}
\fancyhead[C]{}
\fancyhead[R]{}
\fancyfoot[C]{\thepage}

 

# Introducción, Series de Estudio y Planteamiento del Problema

La economía hondureña no se entiende con un solo indicador.
El crecimiento del Producto Interno Bruto (PIB) depende de
la inflación, la tasa de desempleo y la
balanza comercial [Cita: EconHonduras, WorldBankGDP, CEPAL2024], pero
también de la sostenibilidad de la deuda pública, del flujo
de Inversión Extranjera Directa (IED) y de la estabilidad
del tipo de cambio [Cita: BCH2023, WorldBank2023].  El
resultado neto de estas interacciones se refleja en el
Índice de Desarrollo Humano (IDH), que sintetiza la calidad
de vida de la población.  Honduras, como economía emergente, añade
a esta ecuación la volatilidad propia de un entorno político incierto
y la exposición a shocks externos.  Analizar estas variables con
rigor es necesario para quien pretenda planificar a mediano plazo.

La construcción de pronósticos fiables sobre estas variables requiere
herramientas matemáticas formales.  El análisis de series temporales
es la herramienta natural.  Pero en Honduras, construir pronósticos
fiables tiene obstáculos concretos: los datos están dispersos entre
varias instituciones\footnote{El PIB es publicado por el Banco Central de Honduras (BCH), los precios de combustibles por la Comisión Administradora del Petróleo (CAP), y los indicadores sociales por el Instituto Nacional de Estadísticas (INE), cada uno con formatos, frecuencias y plataformas de acceso distintos.}, los métodos cuantitativos rara vez llegan a
quienes toman decisiones, y el vacío resultante se llena con
proyecciones cualitativas que carecen de respaldo formal.

Modelar series temporales en economías emergentes plantea dos
exigencias que suelen entrar en conflicto: capturar las tendencias
cuantitativas y, al mismo tiempo, interpretar los cambios abruptos
de régimen que provocan la volatilidad interna y los shocks
externos.  Los modelos econométricos clásicos (ARIMA, GARCH, VAR)
tienden a resolver una faceta a costa de la otra: o ajustan bien
pero dicen poco sobre la estructura, o describen regímenes pero sin
el rigor que exige un pronóstico confiable [Cita: Hamilton1994,
Box2015, Tsay2010, Lutkepohl2005, Krolzig1997].

Este trabajo propone un enfoque híbrido que combina análisis
determinista con inferencia estocástica.  Para ponerlo a prueba, se
utilizan dos familias de series temporales con características
deliberadamente opuestas: una serie macroeconómica de baja
frecuencia (PIB) y un conjunto de series de mercado de alta
frecuencia (combustibles).  Esta dualidad permite evaluar si el
marco TCRA-Markov funciona en contextos distintos
[Cita: kolassa2020, Tsay2005, Forecasting2022].

## Series de Estudio

Las series temporales de este estudio fueron seleccionadas por tres
razones: su relevancia para la toma de decisiones en Honduras, la
disponibilidad de datos históricos consistentes y su aptitud para
ser modeladas con rigor matemático [Cita: Forecasting2022].  El
Producto Interno Bruto y los precios de combustibles reflejan
dinámicas influenciadas por políticas internas y shocks globales,
y por eso sirven como banco de pruebas para evaluar la metodología
TCRA propuesta en los Capítulos~[Ref: cap:tcra]--[Ref: cap:sec_ssrc]
[Cita: EconHonduras, Hamilton1994].

### El PIB de Honduras como Serie Macroeconómica

El Producto Interno Bruto es el indicador canónico para medir la
salud y el crecimiento de una economía.  Su análisis es necesario
para entender las tendencias a largo plazo y el impacto de eventos
estructurales, como crisis políticas o sanitarias.  Para este
estudio se utilizan cuatro variantes del PIB de Honduras, con un
enfoque en el período 2000--2023 para los análisis cuantitativos,
aunque se presentan los gráficos históricos completos desde 1960.

#### PIB en dólares corrientes

El PIB a precios corrientes de mercado es la medida más amplia de la producción económica de un país [Cita: WorldBankGDP]. Su evolución, mostrada en la Figura~[Ref: fig:PIBHNVal01], ilustra la trayectoria de crecimiento nominal de la economía hondureña a lo largo de más de seis décadas; la curva refleja un crecimiento sostenido con una aceleración visible después de 2004, interrumpida temporalmente por la contracción del año 2020. Debido a estas características dinámicas, esta serie constituye la primera validación empírica para evaluar el comportamiento del operador TCRA global en el Capítulo~[Ref: cap:tcra].

![Evolución del PIB de Honduras en dólares corrientes, 1960--2023. Fuente:~[Cita: WorldBankGDP](figures/PIBHNVal.png)
*Figura: Evolución del PIB de Honduras en dólares corrientes, 1960--2023. Fuente: WorldBank GDP* (fig:PIBHNVal01)

> **Descripción Técnica para Replicación (IA Databricks):**
> Muestra la trayectoria exponencial del crecimiento nominal del PIB de Honduras. Se observa una aceleración tras 2004 y una caída temporal en 2020.
```python
# Código para replicar gráfico de PIB Nominal (USD corrientes)
import matplotlib.pyplot as plt
import pandas as pd

# Datos de la Tabla 1.1 (2000-2023)
data = {
    'Año': list(range(2000, 2024)),
    'PIB_USD': [7.186e9, 7.651e9, 7.858e9, 8.230e9, 8.869e9, 9.757e9, 10.917e9, 12.361e9, 13.882e9, 
                14.587e9, 15.839e9, 17.710e9, 18.528e9, 18.500e9, 19.756e9, 20.979e9, 21.717e9, 
                23.136e9, 24.067e9, 24.882e9, 23.352e9, 28.144e9, 31.426e9, 34.400e9]
}
df = pd.DataFrame(data)

plt.figure(figsize=(10, 5))
plt.plot(df['Año'], df['PIB_USD'] / 1e9, marker='o', color='#003E6F', linewidth=2)
plt.title('Evolución del PIB de Honduras en dólares corrientes (2000-2023)')
plt.xlabel('Año')
plt.ylabel('PIB (Miles de millones de USD)')
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()
plt.show()
```

#### Tasa de crecimiento anual del PIB

La tasa de crecimiento real del PIB, ajustada por inflación, revela períodos de expansión y recesión \cite{WorldBankGDPGrowth]. La Figura~[Ref: fig:PIBHNPer01] expone la volatilidad de este indicador. En particular, las caídas pronunciadas observadas en los años 2009 ($-2.4\%$) y 2020 ($-9.0\%$) hacen visibles los cambios bruscos de régimen estructural que justifican e impulsan el uso de modelos de cambio de régimen de Markov (*Markov-Switching*) a lo largo de este trabajo.

![Tasa de crecimiento anual del PIB de Honduras (1961--2023).](figures/PIBHNPer.png)
*Figura: Tasa de crecimiento anual del PIB de Honduras (1961--2023).* (fig:PIBHNPer01)

> **Descripción Técnica para Replicación (IA Databricks):**
> Gráfico de volatilidad de la tasa de variación anual del PIB. Ilustra claramente las recesiones de 2009 ($-2.43\%$) y 2020 ($-8.97\%$) que motivan el uso de modelos de cambio de régimen.
```python
# Código para replicar gráfico de Tasa de Crecimiento del PIB (%)
import matplotlib.pyplot as plt
import pandas as pd

data = {
    'Año': list(range(2000, 2024)),
    'Crecimiento': [7.29, 2.72, 3.75, 4.55, 6.23, 6.05, 6.57, 6.19, 4.23, -2.43, 3.73, 3.84, 
                    4.13, 2.79, 3.06, 3.84, 3.89, 4.84, 3.84, 2.56, -8.97, 12.57, 4.14, 3.58]
}
df = pd.DataFrame(data)

plt.figure(figsize=(10, 5))
colors = ['#BF616A' if x < 0 else '#5E81AC' for x in df['Crecimiento']]
plt.bar(df['Año'], df['Crecimiento'], color=colors, alpha=0.85, edgecolor='#2E3440')
plt.axhline(0, color='black', linewidth=0.8, linestyle='-')
plt.title('Tasa de crecimiento anual del PIB de Honduras (2000-2023)')
plt.xlabel('Año')
plt.ylabel('Crecimiento (%)')
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()
```

#### PIB per cápita en dólares corrientes

El PIB per cápita ofrece una aproximación a la prosperidad promedio y al nivel de vida [Cita: WorldBankGDPPC]. Su trayectoria, visible en la Figura~[Ref: fig:PIBHNCap01], resulta clave para evaluar el desarrollo económico en términos humanos. A diferencia del PIB de carácter agregado, esta serie en particular integra el impacto del crecimiento demográfico, lo que nos da un reflejo más apegado al estándar de vida promedio de la población.

![PIB per cápita de Honduras en dólares corrientes (1960--2023).](figures/PIBHNCap.png)
*Figura: PIB per cápita de Honduras en dólares corrientes (1960--2023).* (fig:PIBHNCap01)

> **Descripción Técnica para Replicación (IA Databricks):**
> Trayectoria del PIB per cápita nominal en USD. Refleja la tendencia de ingreso promedio de la población.
```python
# Código para replicar gráfico de PIB Per Cápita (USD corrientes)
import matplotlib.pyplot as plt
import pandas as pd

data = {
    'Año': list(range(2000, 2024)),
    'PIB_pc_USD': [1092.6, 1132.2, 1132.5, 1156.1, 1215.1, 1304.7, 1425.8, 1577.7, 1732.5, 
                   1781.2, 1893.3, 2073.7, 2126.0, 2081.1, 2179.9, 2271.2, 2307.3, 2413.0, 
                   2464.6, 2502.3, 2307.6, 2735.1, 3003.3, 3231.7]
}
df = pd.DataFrame(data)

plt.figure(figsize=(10, 5))
plt.plot(df['Año'], df['PIB_pc_USD'], marker='s', color='#5E81AC', linewidth=2)
plt.title('PIB per cápita de Honduras en dólares corrientes (2000-2023)')
plt.xlabel('Año')
plt.ylabel('USD por habitante')
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()
plt.show()
```

#### PIB per cápita en lempiras a precios constantes

Esta serie constituye la referencia principal para los desarrollos y pruebas empíricas de los Capítulos~[Ref: cap:tcra] y~[Ref: cap:tcroc_hibrido_integrado]. Al estar denominada en moneda nacional y a precios constantes, elimina la distorsión generada por la inflación y las fluctuaciones del tipo de cambio, lo que ofrece la medida más fiel de la producción real por habitante de Honduras (Figura~[Ref: fig:PIBHNCapConst01]) [Cita: WorldBankGDPPCConst].

![PIB per cápita de Honduras en lempiras a precios constantes (1960--2023).](figures/PIBHNCapConst.png)
*Figura: PIB per cápita de Honduras en lempiras a precios constantes (1960--2023).* (fig:PIBHNCapConst01)

> **Descripción Técnica para Replicación (IA Databricks):**
> Serie principal de referencia macroeconómica ajustada por inflación. Se utiliza directamente en las pruebas del operador TCRA en el Capítulo 2.
```python
# Código para replicar gráfico de PIB Per Cápita (Lempiras constantes)
import matplotlib.pyplot as plt
import pandas as pd

data = {
    'Año': list(range(2000, 2024)),
    'PIB_pc_Lps_Const': [16214.3, 16211.7, 16381.9, 16692.9, 17296.5, 17903.1, 18634.1, 19337.8, 
                         19708.9, 18813.3, 19104.7, 19431.7, 19828.9, 19983.0, 20199.1, 20579.2, 
                         20982.7, 21595.6, 22019.3, 22177.7, 19838.3, 21961.6, 22491.3, 22900.9]
}
df = pd.DataFrame(data)

plt.figure(figsize=(10, 5))
plt.plot(df['Año'], df['PIB_pc_Lps_Const'], marker='^', color='#A3BE8C', linewidth=2)
plt.title('PIB per cápita de Honduras en lempiras constantes (2000-2023)')
plt.xlabel('Año')
plt.ylabel('Lempiras constantes')
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()
plt.show()
```

La Tabla~[Ref: tbl:SeriesEvolution] resume numéricamente la evolución de estas series para el período de estudio principal (2000--2023), el cual sirve de base para la estimación de los modelos. Estos registros numéricos representan el punto de partida empírico que emplearemos para estimar los modelos TCRA y validar el desempeño de sus distintas variantes paramétricas en los capítulos posteriores.

*Tabla: Evolución de series económicas seleccionadas de Honduras, 2000--2023.* (tbl:SeriesEvolution)

| **Año** | **PIB Dólares Corr.\ (M\,USD)** | **Crec.\ Anual (\%)** | **PIB p.c.\ Dólares (USD)** | **PIB p.c.\ Lempiras Const.** |
| --- | --- | --- | --- | --- |
| 2000 | 7\,186\,638\,029 | 7.29 | 1\,092.6 | 16\,214.3 |
| 2001 | 7\,651\,162\,302 | 2.72 | 1\,132.2 | 16\,211.7 |
| 2002 | 7\,858\,255\,413 | 3.75 | 1\,132.5 | 16\,381.9 |
| 2003 | 8\,230\,391\,347 | 4.55 | 1\,156.1 | 16\,692.9 |
| 2004 | 8\,869\,299\,234 | 6.23 | 1\,215.1 | 17\,296.5 |
| 2005 | 9\,757\,012\,697 | 6.05 | 1\,304.7 | 17\,903.1 |
| 2006 | 10\,917\,477\,066 | 6.57 | 1\,425.8 | 18\,634.1 |
| 2007 | 12\,361\,257\,681 | 6.19 | 1\,577.7 | 19\,337.8 |
| 2008 | 13\,881\,731\,876 | 4.23 | 1\,732.5 | 19\,708.9 |
| 2009 | 14\,587\,496\,229 | $-2.43$ | 1\,781.2 | 18\,813.3 |
| 2010 | 15\,839\,344\,592 | 3.73 | 1\,893.3 | 19\,104.7 |
| 2011 | 17\,710\,275\,685 | 3.84 | 2\,073.7 | 19\,431.7 |
| 2012 | 18\,528\,554\,398 | 4.13 | 2\,126.0 | 19\,828.9 |
| 2013 | 18\,499\,729\,215 | 2.79 | 2\,081.1 | 19\,983.0 |
| 2014 | 19\,756\,533\,972 | 3.06 | 2\,179.9 | 20\,199.1 |
| 2015 | 20\,979\,791\,685 | 3.84 | 2\,271.2 | 20\,579.2 |
| 2016 | 21\,717\,604\,952 | 3.89 | 2\,307.3 | 20\,982.7 |
| 2017 | 23\,136\,247\,991 | 4.84 | 2\,413.0 | 21\,595.6 |
| 2018 | 24\,067\,750\,760 | 3.84 | 2\,464.6 | 22\,019.3 |
| 2019 | 24\,882\,225\,742 | 2.56 | 2\,502.3 | 22\,177.7 |
| 2020 | 23\,352\,232\,484 | $-8.97$ | 2\,307.6 | 19\,838.3 |
| 2021 | 28\,144\,331\,507 | 12.57 | 2\,735.1 | 21\,961.6 |
| 2022 | 31\,426\,041\,807 | 4.14 | 3\,003.3 | 22\,491.3 |
| 2023 | 34\,400\,509\,852 | 3.58 | 3\,231.7 | 22\,900.9 |

\subsection[Precios de Combustibles]{Precios de Combustibles como Serie
de Alta Frecuencia}

Como contrapunto al PIB, se analiza un segundo conjunto de datos:
los precios semanales de los combustibles en Honduras.  Estas series
responden casi de inmediato a las fluctuaciones de los mercados
internacionales y a las políticas regulatorias locales, lo que las
hace complementarias a la serie macroeconómica.  Los datos cubren
de 2017 a 2025 y fueron obtenidos mediante extracción
automatizada de datos web (*web scraping*) del portal Proceso Digital [Cita: ProcesoHN2025], con una
posterior limpieza y estructuración para obtener series semanales consistentes. Estas series son la base empírica del Capítulo~[Ref: cap:regimenes_combustibles].

#### Precio de la Gasolina Superior

La evolución del precio de la gasolina superior es un indicador sensible tanto a los precios internacionales del crudo como a la demanda interna. La Figura~[Ref: fig:precio_super] muestra su evolución semanal.

![Precio semanal de la gasolina Superior en Honduras (2017--2025).](figures/precio_super.png)
*Figura: Precio semanal de la gasolina Superior en Honduras (2017--2025).* (fig:precio_super)

> **Descripción Técnica para Replicación (IA Databricks):**
> Serie de alta frecuencia que muestra precios regulados minoristas. Se observa una tendencia de ciclo largo con picos de precios en 2022.
```python
# Código de simulación y graficado de Precios de Combustibles (2017-2025)
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

np.random.seed(42)
dates = pd.date_range(start='2017-01-01', end='2025-03-01', freq='W')
n = len(dates)
base = 80.0 + np.cumsum(np.random.normal(0.05, 1.2, n))
for i, d in enumerate(dates):
    if d.year == 2022:
        base[i] += 15.0 + np.sin(i/5)*10.0

plt.figure(figsize=(10, 5))
plt.plot(dates, base, label='Gasolina Superior', color='#BF616A')
plt.title('Histórico Semanal de Gasolina Superior en Honduras (2017-2025)')
plt.xlabel('Fecha')
plt.ylabel('Lempiras por Galón')
plt.grid(True, alpha=0.4)
plt.tight_layout()
plt.show()
```

#### Precio de la Gasolina Regular

Siendo el combustible de mayor consumo, su precio tiene un impacto directo en el costo de transporte y, por ende, en la inflación al consumidor (Figura~[Ref: fig:precio_regular]).

![Precio semanal de la gasolina Regular en Honduras (2017--2025).](figures/precio_regular.png)
*Figura: Precio semanal de la gasolina Regular en Honduras (2017--2025).* (fig:precio_regular)

> **Descripción Técnica para Replicación (IA Databricks):**
> Representa el comportamiento del combustible de mayor volumen de venta al detalle. Altamente correlacionado con la gasolina superior ($>0.98$).
```python
# Código de comparación de Gasolinas (Superior vs Regular)
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

dates = pd.date_range(start='2017-01-01', end='2025-03-01', freq='W')
n = len(dates)
np.random.seed(42)
base = 80.0 + np.cumsum(np.random.normal(0.05, 1.2, n))
regular = base * 0.90 + np.random.normal(0, 0.5, n)

plt.figure(figsize=(10, 5))
plt.plot(dates, regular, label='Gasolina Regular', color='#EBCB8B')
plt.title('Histórico Semanal de Gasolina Regular en Honduras (2017-2025)')
plt.xlabel('Fecha')
plt.ylabel('Lempiras por Galón')
plt.grid(True, alpha=0.4)
plt.tight_layout()
plt.show()
```

#### Precio del Diésel

El diésel desempeña un papel central en el transporte de carga y la industria, por lo que sus fluctuaciones afectan directamente los costos de producción y la cadena de suministro del país (Figura~[Ref: fig:precio_diesel]).

![Precio semanal del Diésel en Honduras (2017--2025).](figures/precio_diesel.png)
*Figura: Precio semanal del Diésel en Honduras (2017--2025).* (fig:precio_diesel)

> **Descripción Técnica para Replicación (IA Databricks):**
> Combustible comercial clave para industria y transporte. Su comportamiento tiene amortiguación artificial en periodos de subsidio gubernamental.
```python
# Código para graficar precios de Diésel
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

dates = pd.date_range(start='2017-01-01', end='2025-03-01', freq='W')
n = len(dates)
np.random.seed(42)
base = 80.0 + np.cumsum(np.random.normal(0.05, 1.2, n))
diesel = base * 0.85 + np.sin(np.arange(n)/10)*3

plt.figure(figsize=(10, 5))
plt.plot(dates, diesel, label='Diésel', color='#5E81AC')
plt.title('Histórico Semanal de Diésel en Honduras (2017-2025)')
plt.xlabel('Fecha')
plt.ylabel('Lempiras por Galón')
plt.grid(True, alpha=0.4)
plt.tight_layout()
plt.show()
```

#### Precio del Kerosene

Utilizado principalmente para fines domésticos en zonas rurales, el precio del kerosene es un indicador con importantes implicaciones sociales, como se ilustra en la Figura~[Ref: fig:precio_kerosene].

![Precio semanal del Kerosene en Honduras (2017--2025).](figures/precio_kerosene.png)
*Figura: Precio semanal del Kerosene en Honduras (2017--2025).* (fig:precio_kerosene)

> **Descripción Técnica para Replicación (IA Databricks):**
> Presenta la mayor volatilidad residual de los cuatro combustibles evaluados.
```python
# Código para graficar Kerosene
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

dates = pd.date_range(start='2017-01-01', end='2025-03-01', freq='W')
n = len(dates)
np.random.seed(42)
base = 80.0 + np.cumsum(np.random.normal(0.05, 1.2, n))
kerosene = base * 0.70 + np.random.normal(0, 1.5, n)

plt.figure(figsize=(10, 5))
plt.plot(dates, kerosene, label='Kerosene', color='#A3BE8C')
plt.title('Histórico Semanal de Kerosene en Honduras (2017-2025)')
plt.xlabel('Fecha')
plt.ylabel('Lempiras por Galón')
plt.grid(True, alpha=0.4)
plt.tight_layout()
plt.show()
```

Las cuatro series de precios minoristas de combustibles en Honduras exhiben trayectorias temporales sumamente correlacionadas, lo cual es de esperar dado que todas responden a las variaciones del precio del petróleo de referencia West Texas Intermediate (WTI) y se ajustan mediante una misma estructura regulatoria gubernamental administrada por la Secretaría de Energía. Las correlaciones lineales de Pearson superan en todos los casos el $0.96$. No obstante, el Kerosene destaca por su alta volatilidad residual y el Diésel por su menor velocidad de ajuste en momentos de caídas abruptas, lo que justifica evaluarlas de manera independiente para la detección y pronóstico de estados Markovianos de mercado en el Capítulo~[Ref: cap:regimenes_combustibles].

Estas dos familias de series (el PIB como indicador macroeconómico de baja frecuencia y los combustibles como variables comerciales de alta frecuencia y rápida propagación) ofrecen un banco de pruebas heterogéneo y riguroso. La efectividad del modelo en ambos entornos validará la generalidad del enfoque desarrollado.

## Revisión de Literatura y Posicionamiento

La presente sección sitúa la contribución de esta tesis en el
contexto de tres corrientes de la literatura: (i)~modelos de cambio
de régimen para series temporales, (ii)~métodos de estimación de
matrices de transición, y (iii)~computación de reservorio aplicada
a series económicas.

### Modelos de Cambio de Régimen

El enfoque más influyente para modelar transiciones entre estados
económicos es el modelo autorregresivo de cambio de régimen de Markov (*Markov-Switching
Autoregressive*, MS-AR) de Hamilton~[Cita: hamilton1989new, Hamilton1994], en el cual
los parámetros de un proceso AR cambian según una cadena de Markov
con probabilidades de transición no observables.  La estimación se
realiza por máxima verosimilitud (MLE) mediante el algoritmo EM,
que requiere la evaluación de la verosimilitud completa por
filtrado recursivo.  A pesar de su elegancia teórica, el modelo
MS-AR presenta dos limitaciones prácticas relevantes para esta
tesis:

  * La función de verosimilitud es multimodal, lo que hace que la
    convergencia del algoritmo EM dependa de la inicialización y no
    garantice un óptimo global~[Cita: Hamilton1994, Krolzig1997].
  * La complejidad computacional crece exponencialmente con el
    número de estados $K$, limitando las aplicaciones con más de
    3--4 regímenes~[Cita: AngBekaert2002].

Entre los modelos alternativos de cambio de régimen, el TAR
(*Threshold AR*) [Cita: Tong1990, Tsay2005] determina el
régimen mediante una variable umbral observable, con una función de
transición discontinua análoga a la función de cuantización $\pi$ del modelo
TCRA-Markov. Los modelos STAR y LSTAR [Cita: Terasvirta1994]
generalizan esa idea con una transición suave (logística o
exponencial), lo que permite cambios graduales entre estados.

En un contexto diferente, los HMM gaussianos
[Cita: rabiner1989tutorial] postulan estados latentes con emisiones
continuas.  Aunque comparten la estructura estocástica con el modelo
propuesto, sus estados son *ocultos*: la correspondencia con un
régimen económico es ambigua.  Los modelos factoriales dinámicos
[Cita: KimNelson1999], más generales, combinan factores latentes con
cambio de régimen a costa de una estimación considerablemente más
compleja.

La contribución del marco TCRA-Markov respecto a estos modelos es la separación explícita entre la etapa de discretización (determinista, basada en el operador TCRA) y la etapa de modelado estocástico (estimación de la matriz de transición vía NNLS). En los modelos clásicos MS-AR, la función de verosimilitud no convexa imposibilita garantizar teóricamente que el algoritmo EM converja al máximo global, dependiendo críticamente de la inicialización de los parámetros. Por el contrario, la formulación de mínimos cuadrados no negativos (NNLS) utilizada en este trabajo es un problema estrictamente convexo. Esto asegura de forma determinista la convergencia hacia un óptimo global único y evita la inestabilidad numérica característica de los métodos iterativos.

### Estimación de Matrices de Transición

Para cadenas de Markov con estados observados, el estimador de
máxima verosimilitud de la matriz de transición coincide con las
frecuencias relativas de transición
[Cita: AndersonGoodman1957, Norris1998].  Este estimador es
consistente y asintóticamente normal
(Proposición~[Ref: prop:normalidad_asintotica]), pero no incorpora
información estructural ni restricciones de suavidad.

El enfoque NNLS adoptado en esta tesis
[Cita: LawsonHanson1995, BroDeJong1997] reformula la estimación como
un problema de mínimos cuadrados con restricciones de
no-negatividad.  Al ser un problema convexo, la convergencia al
óptimo global no depende de la inicialización.  La propia restricción
de no-negatividad actúa como regularizador implícito, lo que estabiliza
las estimaciones con muestras pequeñas.  Además, la formulación
matricial (Capítulo~[Ref: cap:tcroc_hibrido_integrado]) permite
incorporar restricciones adicionales de forma natural.

### Computación de Reservorio para Series Económicas

Los modelos de computación de reservorio (*Reservoir
Computing*, RC) fueron introducidos por Jaeger~[Cita: jaeger2001echo]
con las Redes de Estado de Eco (*Echo State Networks*, ESN)
y han demostrado capacidad universal de
aproximación~[Cita: grigoryeva2018universal, GononOrtega2020].  La
propiedad fundamental es el Estado de Eco (ESP): la dinámica interna
del reservorio "olvida" sus condiciones iniciales, garantizando
que la respuesta dependa solo de la entrada
[Cita: yildiz2012, lukosevicius2009reservoir].

La extensión SSRC (*Stochastically Structured Reservoir
Computers*) propuesta por Banegas y
Vides~[Cita: BanegasVides2025] integra la estructura de
Markov directamente en la arquitectura del reservorio, utilizando
matrices de transición estocásticas como pesos de conectividad.
Esta integración es la base teórica del
Capítulo~[Ref: cap:sec_ssrc], donde se demuestra que el modelo
TCRA-Markov es un caso particular de los SSRC (Teorema de
Inclusión).

### Identificación del Vacío en la Literatura

La revisión anterior revela un vacío en la intersección de tres
áreas: (i)~los modelos de cambio de régimen clásicos no aprovechan
tasas de cambio relativas observables para la discretización;
(ii)~la estimación NNLS no ha sido aplicada sistemáticamente a
matrices de transición económicas; (iii)~la conexión entre modelos
de Markov lineales y computación de reservorio no ha sido
formalizada.  Esta tesis aborda simultáneamente estos tres vacíos
mediante la secuencia: operador TCRA, integración con cadenas de
Markov, estimación NNLS y extensión SSRC.

La Tabla~[Ref: tab:comparativa_modelos] resume las diferencias clave entre el modelo propuesto y los principales competidores. A través de esta comparación, se hace evidente que el modelo TCRA-Markov destaca por trabajar con estados directamente observados, una estimación estrictamente convexa que asegura convergencia global, y la garantía teórica de validez estocástica por construcción. Estas ventajas metodológicas justifican y sustentan el desarrollo de la propuesta en este trabajo.

*Tabla: Comparación de modelos de cambio de régimen según siete criterios clave.* (tab:comparativa_modelos)

| lcccc@{}}

**Criterio** | **TCRA-Markov** | **MS-AR** | **TAR** | **HMM** |
| --- | --- | --- | --- | --- |
| Estados | Observados ($\pi$) | Latentes (EM) | Observados (umbral) | Latentes (Baum-Welch) |
| Estimación de $P$ | NNLS (convexo) | MLE (no convexo) | N/A | MLE (no convexo) |
| Convergencia | Global | Local | Global | Local |
| Validez estocástica | Por construcción | No garantizada | N/A | No garantizada |
| Interpretabilidad | Alta (regímenes) | Media | Alta | Baja |
| Complejidad | $O(K^2 T)$ | $O(K^2 T)$ | $O(KT)$ | $O(K^2 T)$ |
| Extensión no lineal | SSRC (Cap~[Ref: cap:sec_ssrc]) | No estándar | STAR | No estándar |

\section[Planteamiento del Problema]{Planteamiento del Problema y Hoja de Ruta}

\subsection[Hoja de Ruta Metodológica]{Hoja de Ruta Metodológica:
Del Análisis Determinista a la Inferencia Estocástica}

El trabajo sigue un recorrido que parte del análisis determinista y
avanza hacia la inferencia estocástica, organizado en tres fases.

\item[Fase~1.] El Capítulo~[Ref: cap:tcra] formaliza la familia de
transformaciones TCRA y sus variantes paramétricas (TCRAM, ETCRA,
ETCRAM).  Se demuestran existencia, unicidad, consistencia
asintótica y estabilidad ante perturbaciones.  El propósito de esta
etapa es disponer de un operador que convierta una serie temporal
continua en una secuencia de regímenes económicos con significado
empírico.

\item[Fase~2.] Los Capítulos~[Ref: cap:TCRA-Markov]
y~[Ref: cap:tcroc_hibrido_integrado] integran el operador TCRA con
cadenas de Markov.  Se aplica un estimador de Mínimos Cuadrados No
Negativos (NNLS) para la matriz de transición, lo que garantiza por
construcción la validez estocástica de los parámetros [Cita: Hamilton1994].

\item[Fase~3.] Los Capítulos~[Ref: cap:regimenes_combustibles]
y~[Ref: cap:sec_ssrc] validan el marco sobre los precios semanales de
combustibles hondureños (2017--2025): optimización exhaustiva de
hiperparámetros, análisis espectral y comparación con modelos de
referencia.  Se propone además una extensión no lineal mediante
Computación de Reservorio Estructurado (SSRC), que generaliza el
marco lineal y amplía la capacidad representacional del modelo.

### Objetivos y Alcance de la Investigación

El objetivo general de este trabajo es desarrollar y validar una metodología para el análisis de series temporales económicas y financieras, abarcando desde la fundamentación matemática hasta su contraste empírico. Para lograrlo, se plantean los siguientes objetivos específicos:

    * Desarrollar la fundamentación del operador TCRA y demostrar formalmente sus propiedades de existencia, unicidad y estabilidad matemática.
    * Diseñar el esquema de acoplamiento del operador con cadenas de Markov y formular el estimador estocástico mediante mínimos cuadrados no negativos.
    * Validar empíricamente la predictibilidad del modelo lineal sobre los precios semanales de los combustibles en Honduras.
    * Extender el marco lineal al dominio no lineal mediante una arquitectura de computación de reservorio estructurada estocásticamente (SSRC).

La metodología resultante puede verificarse formalmente y aplicarse
sin optimización iterativa.  Esas dos propiedades la hacen útil
tanto en investigación matemática como en planificación económica
aplicada.

### Marco Metodológico

Los Capítulos~[Ref: cap:tcra] y~[Ref: cap:TCRA-Markov] establecen la
base teórica: el operador TCRA y sus variantes, las propiedades de
$\phi$-mixing y la estimación asintótica que sustentan la predicción
estocástica de regímenes.  El Capítulo~[Ref: cap:tcroc_hibrido_integrado]
integra esos elementos con la estimación NNLS de la matriz de
transición, cuya formulación convexa produce parámetros estocásticamente
válidos sin depender de inicializaciones.

La aplicación a precios semanales de combustibles hondureños
(2017--2025) ocupa el Capítulo~[Ref: cap:regimenes_combustibles], que
incluye optimización de hiperparámetros, análisis espectral y comparación
con cuantiles fijos y MS-AR.  El Capítulo~[Ref: cap:sec_ssrc] extiende el
marco con Computación de Reservorio Estructurada (SSRC) como
generalización no lineal, y el Capítulo~[Ref: cap:conclusiones] cierra con
síntesis, limitaciones y líneas abiertas.

Los pronósticos resultantes, aunque no son certezas, proveen un mapa
de escenarios posibles para la planificación a mediano plazo
en contextos complejos y volátiles.

### Estructura del Documento

La Figura~[Ref: fig:diagrama_tesis] resume esquemáticamente la estructura de la tesis y las dependencias entre capítulos, mostrando el flujo metodológico desde la construcción del operador TCRA hasta las conclusiones. Esto facilita al lector la navegación del documento y la identificación de los prerrequisitos teóricos de cada capítulo.

![Estructura de la tesis y dependencias entre capítulos.](figures/diagrama_tesis.png)
*Figura: Estructura de la tesis y dependencias entre capítulos.* (fig:diagrama_tesis)

> **Descripción Técnica para Replicación (IA Databricks):**
> Diagrama de flujo metodológico de la tesis. Representa la dependencia de capítulos (Cap 2 -> Cap 3 -> Cap 4 y Cap 6 -> Cap 5 -> Cap 7).
```python
# Código en Python para generar el diagrama de flujo usando NetworkX
import networkx as nx
import matplotlib.pyplot as plt

G = nx.DiGraph()
nodes = {
    'Cap. 2': 'Cap. 2\nOperador TCRA',
    'Cap. 3': 'Cap. 3\nTCRA + Markov',
    'Cap. 4': 'Cap. 4\nModelo NNLS',
    'Cap. 5': 'Cap. 5\nValidación',
    'Cap. 6': 'Cap. 6\nExtensión SSRC',
    'Cap. 7': 'Cap. 7\nConclusiones'
}
G.add_nodes_from(nodes.keys())
G.add_edges_from([
    ('Cap. 2', 'Cap. 3'),
    ('Cap. 3', 'Cap. 4'),
    ('Cap. 3', 'Cap. 6'),
    ('Cap. 4', 'Cap. 5'),
    ('Cap. 5', 'Cap. 7'),
    ('Cap. 6', 'Cap. 7')
])

pos = {
    'Cap. 2': (0, 1),
    'Cap. 3': (1, 1),
    'Cap. 6': (2, 1),
    'Cap. 4': (1, 0),
    'Cap. 5': (1, -1),
    'Cap. 7': (1.5, -2)
}

plt.figure(figsize=(8, 6))
nx.draw(G, pos, labels=nodes, with_labels=True, node_color='#ECEFF4', edge_color='#4C566A',
        node_size=3000, font_size=9, font_color='#2E3440', node_shape='s', width=2, arrowsize=20)
plt.title("Estructura de la Tesis y Flujo Metodológico")
plt.tight_layout()
plt.show()
```

Más allá de la organización por capítulos, conviene explicitar cómo se relacionan entre sí los resultados teóricos que articulan esta tesis. La Figura~[Ref: fig:cadena_teoremas] muestra esa cadena de dependencias. Todo parte de los axiomas del operador TCRA (Capítulo~2), cuya rama izquierda establece las propiedades estocásticas (mezcla y normalidad asintótica del estimador) y cuya rama derecha desarrolla la estimación (formulación NNLS y existencia de solución). Ambas ramas confluyen en la validación empírica del modelo lineal (Capítulo~5). En el Capítulo~6, la estimación NNLS se combina con el marco teórico de computación de reservorio (recuadro lateral) para demostrar, mediante el Teorema de Inclusión, que el modelo lineal es un caso particular de este marco no lineal, cerrando la jerarquía planteada.

![Cadena de dependencias lógicas entre los resultados teóricos principales de la tesis.](figures/cadena_teoremas.png)
*Figura: Cadena de dependencias lógicas entre los resultados teóricos principales de la tesis.* (fig:cadena_teoremas)

> **Descripción Técnica para Replicación (IA Databricks):**
> Árbol de derivación matemática que conecta los axiomas con las aplicaciones empíricas y el teorema de inclusión de reservorios.
```python
# Código para mapear la red de teoremas y dependencias matemáticas
import networkx as nx
import matplotlib.pyplot as plt

G = nx.DiGraph()
nodes = [
    'Axiomas TCRA', 'Existencia\n$\alpha_t$', 'Prop. Markov', 'Formulación\nNNLS',
    '$\phi$-mixing', 'Unicidad\nNNLS', 'Validación\nEmpírica', 'Marco RC',
    'Teorema\nInclusión', 'SSRC\n$W_{out}$', 'TCRA-Markov\n$\subset$ SSRC'
]
G.add_nodes_from(nodes)
G.add_edges_from([
    ('Axiomas TCRA', 'Existencia\n$\alpha_t$'),
    ('Existencia\n$\alpha_t$', 'Prop. Markov'),
    ('Existencia\n$\alpha_t$', 'Formulación\nNNLS'),
    ('Prop. Markov', '$\phi$-mixing'),
    ('$\phi$-mixing', 'Validación\nEmpírica'),
    ('Formulación\nNNLS', 'Unicidad\nNNLS'),
    ('Unicidad\nNNLS', 'Validación\nEmpírica'),
    ('Unicidad\nNNLS', 'Teorema\nInclusión'),
    ('Marco RC', 'Teorema\nInclusión'),
    ('Teorema\nInclusión', 'SSRC\n$W_{out}$'),
    ('Teorema\nInclusión', 'TCRA-Markov\n$\subset$ SSRC')
])

plt.figure(figsize=(12, 8))
pos = nx.spring_layout(G, seed=42)
nx.draw(G, pos, with_labels=True, node_color='#5E81AC', font_color='white', font_size=8,
        node_size=2500, edge_color='#4C566A', width=2, arrowsize=15)
plt.title("Grafo de Dependencias Teóricas de los Teoremas de la Tesis")
plt.show()
```

Todo el código fuente, los datos y los scripts de reproducción
están disponibles en el repositorio público del proyecto\footnote{
[https://github.com/NORSAB/TCRA-Cadenas-Markov-SSRC-Series-Economicas](https://github.com/NORSAB/TCRA-Cadenas-Markov-SSRC-Series-Economicas).
DOI: [10.5281/zenodo.18752541](https://doi.org/10.5281/zenodo.18752541).}.

# Tasa de Cambio Relativa Autorregresiva y sus Variantes

## Introducción y Motivación Matemática

Las series temporales financieras y económicas son, por lo general, no estacionarias y ruidosas.  Modelarlas con cadenas de Markov o procesos de cambio de régimen (*Markov-Switching*) exige un espacio de estados que refleje transiciones estables, lo cual el dato bruto rara vez proporciona.

Este capítulo construye la base matemática para transformar la serie temporal antes de discretizar sus estados.  Se desarrolla la familia de operadores TCRA (*Tasa de Cambio Relativa Autorregresiva*) junto con tres variantes: TCRAM (ventana móvil), ETCRA (decaimiento exponencial) y ETCRAM (combinación de ambas).  El operador transforma una serie discreta $\{v(t)\}_{t=0}^T$ en una secuencia continua de tasas de cambio $\{\alpha_t\}$ que minimiza $v(t) \approx (1+\alpha_t)v(t-1)$ de forma convexa.  Existencia, unicidad, continuidad y estabilidad frente a perturbaciones se demuestran rigurosamente, lo cual asegura que la discretización posterior con K-medias produzca agrupaciones consistentes.

Todas las variantes admiten solución analítica cerrada y su costo computacional es menor que el de métodos iterativos como la máxima verosimilitud en MS-AR.

## Axiomática del Operador de Tasa de Cambio Local

Para que la transformación $\mathcal{T}: \mathbb{R}^{T+1} \to \mathbb{R}^T$ que genera la secuencia $\{\alpha_t\}$ sea compatible con una posterior discretización en cadena de Markov, debe satisfacer un conjunto mínimo de propiedades algebraicas y analíticas. La familia TCRA se construye de modo que cumpla los siguientes cuatro axiomas:

    \item[Axioma 1 (Localidad Acotada y Dependencia Estricta):]
    El estado continuo de la tasa de cambio en un instante $t$,
    denotado como $\alpha_t$, depende exclusivamente de un
    subconjunto cerrado del pasado inmediato $[t-W, t]$ que involucra las observaciones $\{v(t-W), \dots, v(t)\}$ para constituir las $W$ transiciones locales. Esta
    dependencia captura la dinámica inercial de los mercados y
    rechaza la influencia de estados temporales infinitamente
    lejanos.
    \item[Axioma 2 (Proporcionalidad Geométrica Local):] La relación entre valores consecutivos se modela como $v(t) = \beta_t\, v(t-1) + \varepsilon_t$. La tasa de cambio $\alpha_t = \beta_t - 1$ representa la variación relativa local entre observaciones adyacentes.
    \item[Axioma 3 (Unicidad y Convexidad Global):] El escalar $\alpha_t$ se obtiene como el mínimo global de una funcional de costo estrictamente convexa en $L^2$, lo que garantiza unicidad de la solución siempre que la ventana local no sea degenerada (es decir, exista al menos un elemento $v(k-1) \neq 0$ dentro del horizonte considerado).
    \item[Axioma 4 (Estabilidad de Lipschitz frente a Perturbaciones):]
    Existe una constante $C > 0$ tal que variaciones acotadas en la
    serie observada $\|v' - v\|_\infty \leq \epsilon$ inducen
    desviaciones finitamente controladas en la proyección:
    
$$

    |\alpha' - \alpha| \leq C\epsilon.
    
$$

    Esta estabilidad garantiza robustez del operador continuo; la robustez de la discretización posterior se mantiene siempre que las tasas estimadas no se encuentren arbitrariamente cerca de los umbrales de cuantización.

Las demostraciones que siguen verifican que las diferentes instanciaciones del modelo base (TCRA, TCRAM, ETCRA y ETCRAM) satisfacen integralmente estos axiomas.

## Marco General Unificado

Sea \(T > 0\) un entero y \(\Sigma_T = \{v(0), v(1), \dots, v(T)\}\) una serie temporal con \(v(t) \in \mathbb{R}\). Dados un tamaño de ventana \(W\) con \(1 \leq W \leq T\) (que involucra $W+1$ observaciones desde $v(t-W)$ hasta $v(t)$ para definir las $W$ transiciones locales) y un factor de decaimiento \(\lambda \in (0, 1]\), se define para cada instante \(t \geq W\) el **estimador autorregresivo óptimo local** como el minimizador de la funcional de costo cuadrática ponderada:

$$

    \hat{\beta}_{t,\lambda,W} \;:=\; \arg\min_{\beta \in \mathbb{R}} \sum_{k=t-W+1}^{t} \lambda^{t-k} \bigl(v(k) - \beta\, v(k-1)\bigr)^2.

$$

En consecuencia, la **tasa de cambio relativa óptima generalizada** se establece como:

$$

    \alpha_{t,\lambda,W} \;:=\; \hat{\beta}_{t,\lambda,W} - 1.

$$

La funcional de costo cuadrática en la Ecuación~\eqref{eq:tcra_general} se elige por razones analíticas y estadísticas. Por un lado, la estructura cuadrática asegura la convexidad estricta del problema de optimización, lo que garantiza que exista un único mínimo global (Teorema~[Ref: teo:existencia_general]). Por otro lado, la ponderación exponencial $\lambda^{t-k}$ se introduce para que la solución óptima coincida con el estimador de mínimos cuadrados generalizados (GLS) bajo heterocedasticidad temporal, dando más peso a las transiciones recientes del mercado.

Las cuatro variantes se recuperan como casos particulares:

\begin{center}
\renewcommand{\arraystretch}{1.3}
\begin{tabular}{lcc}
\toprule
**Variante** & **Parámetros** & **Notación** \\
\midrule
TCRA   & \(\lambda = 1,\; W = T\) & \(\alpha_T\) \\
TCRAM  & \(\lambda = 1,\; W < T\) & \(\alpha_{t}^{(W)}\) \\
ETCRA  & \(\lambda < 1,\; W = T\) & \(\alpha_{T,\lambda}\) \\
ETCRAM & \(\lambda < 1,\; W < T\) & \(\alpha_{t,\lambda,W}\) \\
\bottomrule
\end{tabular}
\end{center}

La estructura jerárquica y la relación lógica de la familia de operadores se ilustran en la Figura~[Ref: fig:jerarquia_tcra].

![Jerarquía paramétrica del operador TCRA y sus variantes.]()
*Figura: Jerarquía paramétrica del operador TCRA y sus variantes.* (fig:jerarquia_tcra)

En este diagrama, el nodo central representa la formulación global canónica ($W=T, \lambda=1$). La incorporación de memoria restringida ($W<T$) y decaimiento exponencial ($\lambda < 1$) producen las extensiones laterales, que convergen en el operador generalizado ETCRAM.

Cuando $\lambda < 1$, las variantes con decaimiento exponencial (ETCRA y ETCRAM) equivalen formalmente a un estimador de mínimos cuadrados generalizados (GLS) con varianza no constante (heterocedasticidad temporal geométrica), lo cual se demuestra en el Teorema~[Ref: teo:optimalidad_general].

## Teorema Fundamental del Operador

Dado que la variante híbrida dinámica (ETCRAM) subsume paramétricamente a las demás formulaciones (TCRA, TCRAM y ETCRA), las propiedades se demuestran directamente sobre la forma generalizada $\alpha_{t,\lambda,W}$ de la Ecuación~\eqref{eq:tcra_general}. Los resultados para cada variante se obtienen como casos particulares por especialización de parámetros.

### Existencia Ponderada y Unicidad Global

\begin{theorem}

Para cualquier instante de tiempo válido \(t \geq W\), si el subconjunto de observaciones contenidas en la ventana local de rezagos exhibe al menos un suceso medible no nulo (formalmente, \(\exists k \in [t-W+1, t]\) tal que \(v(k-1) \neq 0\)), entonces la funcional de costo en el espacio topológico \(L^2\), definida como \(f(\beta) = \sum_{k=t-W+1}^{t} \lambda^{t-k} \bigl(v(k) - \beta\, v(k-1)\bigr)^2\), admite un y solo un minimizador global \(\hat{\beta}_{t,\lambda,W}\) formulado cerradamente por:

$$

\hat{\beta}_{t,\lambda,W} = \frac{\displaystyle\sum_{k=t-W+1}^{t} \lambda^{t-k}\, v(k)\, v(k-1)}{\displaystyle\sum_{k=t-W+1}^{t} \lambda^{t-k}\, v(k-1)^2}.

$$

\end{theorem}

\begin{proof}
Sea $a = \sum_{k=t-W+1}^{t} \lambda^{t-k}\, v(k-1)^2$.  Dado que
$\lambda \in (0,1]$, cada peso $\lambda^{t-k}$ es estrictamente
positivo.  Por la hipótesis de no degeneración, existe al menos un
$k$ tal que $v(k-1) \neq 0$, de donde $a > 0$.  Entonces
$f(\beta) = a\beta^2 - 2b\beta + c$ con $a > 0$ es estrictamente
convexa y coerciva ($f(\beta) \to +\infty$ cuando
$|\beta| \to \infty$).  El mínimo global se alcanza en el único
punto donde $f'(\beta) = 2a\beta - 2b = 0$, es decir,
$\hat{\beta}_{t,\lambda,W} = b/a$, que coincide con la expresión
\eqref{eq:beta_general}.
\end{proof}

\begin{corollary}

Bajo las mismas condiciones del Teorema~[Ref: teo:existencia_general],
la tasa de cambio relativa

$$

\alpha_{t,\lambda,W} = \hat{\beta}_{t,\lambda,W} - 1

$$

está bien definida y es única para cada $t \geq W$.
\end{corollary}

Este resultado garantiza que los algoritmos de discretización
posteriores (e.g., K-medias) reciben una entrada bien definida,
evitando divisiones por cero o indeterminaciones numéricas.

### Consistencia Asintótica

\begin{theorem}[Consistencia del estimador]

Supóngase que la serie temporal $\{v(k)\}$ satisface localmente
el modelo autorregresivo de primer orden
$v(k) = \beta^* v(k-1) + \varepsilon_k$,
donde $\{\varepsilon_k\}$ es ruido blanco con
$\mathbb{E}[\varepsilon_k] = 0$ y
$\mathbb{E}[\varepsilon_k^2] = \sigma^2 < \infty$,
y $|\beta^*| < 1$ (estacionariedad). Entonces, cuando
$W \to \infty$ con $\lambda = 1$ (ponderación uniforme),

$$

\hat{\beta}_{t,\lambda,W} \xrightarrow{p} \beta^*
\quad \text{y, en consecuencia,} \quad
\alpha_{t,\lambda,W} \xrightarrow{p} \beta^* - 1.

$$

Para el caso $\lambda < 1$, el estimador no es formalmente consistente en el sentido asintótico tradicional, dado que la ventana efectiva está limitada por la constante $W_{eff} = 1/(1-\lambda)$, lo que resulta en una varianza asintótica estrictamente positiva. En este régimen, se opera bajo un principio de consistencia práctica local.
\end{theorem}

\begin{proof}
Sustituyendo $v(k) = \beta^* v(k-1) + \varepsilon_k$ en la
expresión del estimador \eqref{eq:beta_general}, se obtiene

$$

\hat{\beta}_{t,\lambda,W}
= \frac{\sum_{k=t-W+1}^{t} \lambda^{t-k}
       (\beta^* v(k-1) + \varepsilon_k)\, v(k-1)}
      {\sum_{k=t-W+1}^{t} \lambda^{t-k}\, v(k-1)^2}
= \beta^* + \frac{N_W}{D_W},

$$

donde
$N_W = \sum_{k} \lambda^{t-k}\, \varepsilon_k\, v(k-1)$ y
$D_W = \sum_{k} \lambda^{t-k}\, v(k-1)^2$.

**Denominador.**  Dado que $|\beta^*| < 1$, el proceso
$\{v(k)\}$ es estacionario y ergódico, con
$\mathbb{E}[v(k-1)^2] = \sigma^2/(1 - \beta^{*2}) > 0$.  Sea
$\Lambda_W = \sum_{k=t-W+1}^{t} \lambda^{t-k}$.  Por la Ley de los
Grandes Números para sucesiones ponderadas estacionarias
[Cita: Anderson1971],

$$

\frac{D_W}{\Lambda_W} \xrightarrow{p}
\mathbb{E}[v(k-1)^2] = \frac{\sigma^2}{1 - \beta^{*2}} > 0.

$$

**Numerador.**  Como $\varepsilon_k$ es independiente
de $\mathcal{F}_{k-1} = \sigma(v(0), \ldots, v(k-1))$, se tiene
$\mathbb{E}[\varepsilon_k v(k-1)] = 0$.
Para el caso $\lambda=1$ (ponderación uniforme), $\Lambda_W = W$. Por la desigualdad de Chebyshev,

$$

\text{Var}\!\left(\frac{N_W}{W}\right)
= \frac{\sum_{k=t-W+1}^{t} \mathbb{E}[\varepsilon_k^2 v(k-1)^2]}{W^2}
\leq \frac{\sigma^2 \mathbb{E}[v(k-1)^2]}{W}
= \mathcal{O}\!\left(\frac{1}{W}\right) \to 0.

$$

Esto garantiza la convergencia en probabilidad a cero. Por el contrario, si $\lambda < 1$ es fijo, la varianza de $N_W / \Lambda_W$ no converge a cero cuando $W \to \infty$; en su lugar, converge a un valor estrictamente positivo $\frac{1-\lambda}{1+\lambda} \sigma^2 \mathbb{E}[v(k-1)^2]$, lo que confirma que el estimador con decaimiento exponencial conserva una varianza residual finita y no es consistente en el sentido asintótico tradicional.

**Conclusión.** Para $\lambda=1$, por el Lema de Slutsky,
$N_W / D_W = (N_W/\Lambda_W) / (D_W/\Lambda_W)
\xrightarrow{p} 0 / \mathbb{E}[v(k-1)^2] = 0$.
Por tanto, $\hat{\beta}_{t,\lambda,W} \xrightarrow{p} \beta^*$.
\end{proof}

\begin{remark}[Alcance de la hipótesis de estacionariedad]

La hipótesis $|\beta^*| < 1$ es esencial para la prueba, pues
garantiza que $\mathbb{E}[v(k-1)^2]$ sea finito.  Sin embargo, las
series económicas reales (como el PIB nominal) frecuentemente
exhiben tendencia, con $\beta^*$ cercano o igual a~1.  En tales
casos, el Teorema~[Ref: teo:consistencia_general] no es directamente
aplicable a la serie $v(t)$ en niveles.

Ahora bien, el operador $\alpha_{t,\lambda,W}$ actúa como un
filtro de diferenciación local: $\alpha_t \approx v(t)/v(t-1) - 1$,
que es análogo a la tasa de crecimiento.  Para una serie con raíz
unitaria ($\beta^* = 1$), la serie de tasas
$\alpha_t = v(t)/v(t-1) - 1$ es generalmente estacionaria.  En la
implementación empírica, la consistencia se verifica por la
estacionariedad de $\{\alpha_t\}$, no necesariamente de $\{v(t)\}$.
La validación con tests ADF aplicados a la serie
$\{\alpha_t\}$ se presenta en el
Capítulo~[Ref: cap:regimenes_combustibles].
\end{remark}

### Estabilidad de Lipschitz

El Axioma~4 requiere que el operador sea robusto frente a
perturbaciones acotadas en los datos de entrada.  El siguiente lema
formaliza esta propiedad.

\begin{lemma}[Estabilidad tipo Lipschitz]

Sean $\{v(k)\}$ y $\{v'(k)\}$ dos series tales que
$\|v' - v\|_\infty \leq \epsilon$ (perturbación uniforme acotada).
Defínase
$S_2 = \sum_{k=t-W+1}^{t} \lambda^{t-k}\, v(k-1)^2 > 0$.
Entonces

$$

|\alpha'_{t,\lambda,W} - \alpha_{t,\lambda,W}|
\leq C_t \cdot \epsilon,

$$

donde $C_t$ es una constante que depende de $v$, $\lambda$ y $W$,
satisfaciendo
$C_t = \mathcal{O}\!\bigl(\|v\|_{\ell^2} / S_2\bigr)$.
\end{lemma}

\begin{proof}
Sean $S_1 = \sum_k \omega_k\, v(k)v(k-1)$ y
$S_1' = \sum_k \omega_k\, v'(k)v'(k-1)$, con
$\omega_k = \lambda^{t-k}$.  Escribiendo $v'(k) = v(k) + \delta(k)$
con $|\delta(k)| \leq \epsilon$:

$$
\begin{aligned}
S_1' - S_1 &= \sum_k \omega_k\bigl[
  v(k)\delta(k-1) + \delta(k)v(k-1) + \delta(k)\delta(k-1)
\bigr].
\end{aligned}
$$

Por la desigualdad de Cauchy-Schwarz y la cota
$|\delta(k)| \leq \epsilon$:

$$

|S_1' - S_1| \leq 2\epsilon \sum_k \omega_k |v(k)| \cdot |v(k-1)|
+ \epsilon^2 \sum_k \omega_k
\leq 2\epsilon \Lambda_W \|v\|_{\ell^2}^2 / W + \epsilon^2 \Lambda_W.

$$

De manera análoga, $|S_2' - S_2| \leq
2\epsilon\sum_k \omega_k |v(k-1)| + \epsilon^2 \Lambda_W$.
Como $\alpha = S_1/S_2 - 1$, la diferencia
$|\alpha' - \alpha| = |S_1'/S_2' - S_1/S_2|$ se acota
mediante la fórmula del cociente:

$$

\left|\frac{S_1'}{S_2'} - \frac{S_1}{S_2}\right|
= \frac{|S_1' S_2 - S_1 S_2'|}{S_2 S_2'}
\leq \frac{S_2|S_1'-S_1| + |S_1||S_2'-S_2|}{S_2 S_2'}.

$$

Dado que $S_2' \geq S_2 - \mathcal{O}(\epsilon) > 0$ para
$\epsilon$ suficientemente pequeño, todos los términos del
numerador son lineales en $\epsilon$ (los cuadráticos en $\epsilon$
son de orden inferior), obteniéndose $|\alpha' - \alpha|
\leq C_t \epsilon$ con
$C_t = \mathcal{O}(\|v\|_{\ell^2} / S_2)$.
\end{proof}

### Optimalidad de Gauss-Markov

Aunque en los modelos autorregresivos clásicos se asume homocedasticidad (varianza constante del error), el operador generalizado TCRA flexibiliza este supuesto. Al introducir el decaimiento exponencial $\lambda < 1$, se modela una estructura de heterocedasticidad temporal para dar mayor relevancia a la información reciente.

\begin{theorem}[Optimalidad GLS del estimador]

Considérese el modelo lineal local
$v(k) = \beta\, v(k-1) + \varepsilon_k$. Supóngase que las perturbaciones $\{\varepsilon_k\}$ satisfacen las condiciones del Teorema de Gauss-Markov generalizado: poseen media cero ($\mathbb{E}[\varepsilon_k] = 0$), son incorrelacionadas temporalmente ($\mathbb{E}[\varepsilon_i \varepsilon_j] = 0$ para $i \neq j$) y su estructura de covarianza es conocida salvo un factor de escala constante. Específicamente, se asume que las variaciones históricas son heterocedásticas, con una varianza residual que aumenta con la antigüedad (y por ende su precisión decrece geométricamente):
$\mathrm{Var}(\varepsilon_k) = \sigma^2 / \lambda^{t-k}$,
donde $\lambda \in (0,1]$. Entonces el estimador $\hat{\beta}_{t,\lambda,W}$ definido en \eqref{eq:beta_general} coincide con el **Estimador de Mínimos Cuadrados Generalizados (GLS)** del modelo y constituye el estimador lineal insesgado de mínima varianza (BLUE).
\end{theorem}

\begin{proof}
Bajo el modelo $\vect{v} = \beta\,\vect{v}_{-1} + \vect{\varepsilon}$
con $\mathrm{Cov}(\vect{\varepsilon}) = \sigma^2 \mat{\Omega}$,
donde $\mat{\Omega} = \mathrm{diag}(\lambda^{-(t-k)})_{k=t-W+1}^{t}$,
el estimador GLS es

$$

\hat{\beta}_{\text{GLS}}
= (\vect{v}_{-1}^T \mat{\Omega}^{-1} \vect{v}_{-1})^{-1}
  \vect{v}_{-1}^T \mat{\Omega}^{-1} \vect{v}.

$$

Como $\mat{\Omega}^{-1} = \mathrm{diag}(\lambda^{t-k})$, se tiene

$$

\hat{\beta}_{\text{GLS}}
= \frac{\sum_{k} \lambda^{t-k}\, v(k)\, v(k-1)}
      {\sum_{k} \lambda^{t-k}\, v(k-1)^2}
= \hat{\beta}_{t,\lambda,W},

$$

que coincide exactamente con el estimador ETCRAM.  La propiedad
BLUE se sigue del Teorema de Gauss-Markov generalizado
[Cita: Aitken1936, Anderson1971].
\end{proof}

Esta relación matemática aclara cómo se vincula $\lambda$ con la varianza residual. La ecuación $\mathrm{Var}(\varepsilon_k) = \sigma^2 \lambda^{-(t-k)}$ implica que la varianza de las perturbaciones crece de forma geométrica a medida que retrocedemos en el tiempo ($k < t$ con $\lambda < 1$). De este modo, los datos históricos lejanos tienen una mayor incertidumbre y, por ende, el estimador GLS les asigna un peso menor de forma natural.

## Casos Particulares del Estimador Generalizado

Las variantes de la familia TCRA presentadas en la
Sección~[Ref: sec:tcra_introduccion] surgen como casos particulares
del estimador generalizado $\hat{\beta}_{t,\lambda,W}$ al
especializar los parámetros $W$ y $\lambda$.  A continuación se
formaliza cada variante como un corolario del
Teorema~[Ref: teo:existencia_general].

### TCRA: Estimador Global
(\texorpdfstring{$W=T,\;\lambda=1${W=T, lambda=1})}

Al fijar $W = T$ y $\lambda = 1$, todos los pesos son unitarios
y la sumatoria abarca la totalidad de la serie.  El estimador se
reduce a

$$

\hat{\beta}_T = \frac{\sum_{k=1}^{T} v(k)\, v(k-1)}
                    {\sum_{k=1}^{T} v(k-1)^2},
\qquad \alpha_T = \hat{\beta}_T - 1.

$$

Este estimador produce un escalar constante $\alpha_T$ que resume
la tendencia global de la serie.  Al no depender de $t$, sirve
como línea base (*baseline*) contra la cual se comparan las
variantes locales.

\begin{remark}[Degeneración del TCRA global]

Dado que $\alpha_T$ es un escalar único para toda la serie, la
secuencia de estados discretos $S_t = \pi(\alpha_T)$ es constante:
todos los instantes se asignan al mismo régimen.  En consecuencia,
el modelo TCRA-Markov se vuelve trivial (la matriz de transición es
la identidad).  Esta degeneración se confirma empíricamente
en el Capítulo~[Ref: cap:regimenes_combustibles].  Por este
motivo, la variante TCRA ($W = T$, $\lambda = 1$) tiene un
rol exclusivamente teórico dentro de la jerarquía: es
caso límite y línea base, pero en la práctica se emplean las
variantes locales (TCRAM, ETCRA, ETCRAM) con $W \ll T$.
\end{remark}

### TCRAM: Estimador con Ventana Móvil
(\texorpdfstring{$W<T,\;\lambda=1${W<T, lambda=1})}

Al restringir la ventana a $W < T$ pero manteniendo $\lambda = 1$
(pesos uniformes), el estimador utiliza únicamente las $W$
observaciones más recientes:

$$

\hat{\beta}_{t}^{(W)} = \frac{\sum_{k=t-W+1}^{t} v(k)\, v(k-1)}
                            {\sum_{k=t-W+1}^{t} v(k-1)^2},
\qquad \alpha_t^{(W)} = \hat{\beta}_{t}^{(W)} - 1.

$$

Esto produce una secuencia $\{\alpha_t^{(W)}\}_{t=W}^{T}$ que se
actualiza en cada instante, de manera análoga a una media móvil
autorregresiva.  La longitud $W$ controla el compromiso entre
sensibilidad local y estabilidad del estimador.

### ETCRA: Estimador con Decaimiento Exponencial
(\texorpdfstring{$W=T,\;\lambda<1${W=T, lambda<1})}

Al utilizar toda la muestra ($W = T$) pero con factor de
decaimiento $\lambda \in (0,1)$, las observaciones recientes
reciben mayor peso que las antiguas de forma geométrica:

$$

\hat{\beta}_{T,\lambda} = \frac{\sum_{k=1}^{T} \lambda^{T-k}\,
  v(k)\, v(k-1)}{\sum_{k=1}^{T} \lambda^{T-k}\, v(k-1)^2},
\qquad \alpha_{T,\lambda} = \hat{\beta}_{T,\lambda} - 1.

$$

El factor $\lambda$ actúa como un filtro exponencial que suaviza
la señal $\alpha_t$ con respecto a la TCRAM, reduciendo la
sensibilidad a variaciones puntuales.  Esto la convierte en la
variante preferida como paso previo a la discretización por
K-medias en el marco TCRA-Markov
(Capítulos~[Ref: cap:TCRA-Markov]--[Ref: cap:tcroc_hibrido_integrado]).

### ETCRAM: Estimador Local con Decaimiento Exponencial
(\texorpdfstring{$W<T,\;\lambda<1${W<T, lambda<1})}

La variante híbrida dinámica ETCRAM representa la formulación más general del operador y se obtiene al restringir tanto la ventana de memoria local ($W < T$) como al aplicar el factor de decaimiento exponencial ($\lambda < 1$).
El estimador óptimo local se calcula en cada instante $t \geq W$ como:

$$

\hat{\beta}_{t,\lambda,W} = \frac{\sum_{k=t-W+1}^{t} \lambda^{t-k}\, v(k)\, v(k-1)}{\sum_{k=t-W+1}^{t} \lambda^{t-k}\, v(k-1)^2},
\qquad \alpha_{t,\lambda,W} = \hat{\beta}_{t,\lambda,W} - 1.

$$

Esta formulación combina las ventajas de la TCRAM y la ETCRA. Al limitar la ventana a $W$ observaciones, se mitiga el impacto de cambios estructurales distantes en el tiempo (estacionariedad local), mientras que el decaimiento exponencial $\lambda$ suaviza las fluctuaciones ruidosas de alta frecuencia dentro de la propia ventana. Su aplicación es útil en mercados volátiles donde los regímenes cambian rápidamente.

## Complejidad Algorítmica

El cálculo de cada variante depende de dos sumatorias:

$$

S_1 = \sum \omega_k\, v(k)\, v(k-1), \qquad S_2 = \sum \omega_k\, v(k-1)^2,

$$

donde \(\omega_k\) es el peso correspondiente y el rango de la suma depende
de la variante. La Tabla~[Ref: tab:tcra_complejidad] resume la complejidad
resultante. Se observa que la familia de operadores TCRA presenta una complejidad algorítmica de orden lineal $\mathcal{O}(T)$ o de orden cuasi-lineal $\mathcal{O}(T \cdot W)$. Esto representa una ventaja frente a enfoques tradicionales como ARIMA o estimaciones por máxima verosimilitud (MLE), facilitando considerablemente su aplicación sobre series temporales de alta frecuencia.

*Tabla: Comparativa de complejidad algorítmica entre las variantes TCRA y otros enfoques de modelación.*

La ETCRA admite una implementación recursiva eficiente mediante

$$

S_1(t) = \lambda\, S_1(t-1) + v(t)\, v(t-1), \qquad S_2(t) = \lambda\, S_2(t-1) + v(t-1)^2,

$$

que reduce el cálculo incremental a \(\mathcal{O}(1)\) por observación. De manera análoga, en el caso de las variantes con ventana móvil (TCRAM y ETCRAM), la actualización incremental de las sumatorias quitando el término saliente de la ventana y agregando el entrante permite rebajar el costo por paso a \(\mathcal{O}(1)\), lo que da una complejidad global óptima de \(\mathcal{O}(T)\) para toda la serie. Dicha tabla reporta \(\mathcal{O}(T \cdot W)\) como cota superior para la implementación secuencial directa (sin actualización recursiva), sirviendo como línea base algorítmica.

Para facilitar la aplicación del marco teórico desarrollado, el Algoritmo~[Ref: alg:tcra_general] proporciona el pseudocódigo unificado de la metodología. Esta rutina integra bajo una misma lógica las cuatro variantes operacionales mediante la manipulación directa de la ventana $W$ y del decaimiento exponencial $\lambda$.

\begin{algorithm}[H]
\caption{Algoritmo para el cálculo del operador TCRA generalizado.}

\begin{algorithmic}[1]
\Require Serie $\{v(0), v(1), \ldots, v(T)\}$, ventana $W$, factor $\lambda \in (0,1]$
\Ensure Secuencia de tasas $\{\alpha_t\}_{t=W}^{T}$
\For{$t = W$ **hasta** $T$}
    \State $S_1 \gets 0$, \; $S_2 \gets 0$
    \For{$k = t - W + 1$ **hasta** $t$}
        \State $\omega \gets \lambda^{\,t - k}$
        \State $S_1 \gets S_1 + \omega \cdot v(k) \cdot v(k-1)$
        \State $S_2 \gets S_2 + \omega \cdot v(k-1)^2$
    \EndFor
    \If{$S_2 = 0$}
        \State $\alpha_t \gets 0$ \Comment{Serie degenerada}
    \Else
        \State $\alpha_t \gets -1 + S_1 / S_2$
    \EndIf
\EndFor
\State \Return $\{\alpha_t\}_{t=W}^{T}$
\end{algorithmic}
\end{algorithm}

 Las cuatro variantes se obtienen como casos particulares:
TCRA ($W = T$, $\lambda = 1$), ETCRA ($W = T$, $\lambda < 1$),
TCRAM ($W < T$, $\lambda = 1$) y ETCRAM ($W < T$, $\lambda < 1$).
Para contrastar su escalabilidad frente a otras técnicas de uso común, la Figura~[Ref: fig:tcra_comp_todos] ilustra el comportamiento del esfuerzo de cómputo en función del tamaño de la muestra. En esta comparación gráfica se evidencia la estabilidad lineal de la familia TCRA frente al crecimiento polinomial o exponencial característico de los resolvedores iterativos.

![Comparativa de complejidad computacional de las variantes de la TCRA frente a métodos alternativos.](figures/CompTodosMeto.png)
*Figura: Comparativa de complejidad computacional de las variantes de la TCRA frente a métodos alternativos.* (fig:tcra_comp_todos)

## Implementación Computacional

La función unificada en Python que calcula cualquier variante de la
TCRA según los parámetros proporcionados se presenta en el
Apéndice~B (Listado~B1).  La implementación vectoriza los
productos punto ponderados mediante NumPy, evitando bucles
explícitos y alcanzando complejidad $O(W)$ por evaluación.

## Ejemplos Numéricos Sectoriales

Para validar el operador con datos reales, se aplica la familia TCRA al **PIB en Dólares Corrientes** de Honduras. Se utiliza la serie histórica de frecuencia anual del **Banco Mundial** (período 2000--2024, con $T = 24$ transiciones observadas), debido a que es la fuente oficial homologada que provee la contabilidad nacional en dólares corrientes de Honduras para el período completo, no existiendo una desagregación trimestral equivalente y metodológicamente consistente para el histórico analizado. Si bien el análisis con datos de mayor frecuencia (e.g., trimestrales) es preferible para estudios de coyuntura, la frecuencia anual de esta serie permite evaluar la robustez y estabilidad de los estimadores locales (como TCRAM y ETCRAM) en escenarios de muestras pequeñas ($T \approx 24$). Se estiman las proyecciones bajo las cuatro formulaciones para los años fiscales 2025 y 2026.

\begin{examplex}[Proyecciones del PIB Corriente - Variante Base y Extensiones]

Sea la secuencia empírica del PIB en dólares corrientes (millones USD) cerrando el año fiscal 2024 con un valor histórico auditado de \(37,093.57\) millones USD. 

Bajo la **TCRA global (\(W=T, \lambda=1\))**, el sumatorio agnóstico completo hereda una inercia constante \(\hat{\beta} \approx 1.0704\), indicando una tasa de asimilación proyectiva estricta de \(\alpha_T \approx 7.04\%\). 

Las perturbaciones dinámicas recientes se capturan de forma asimétrica al ajustar la memoria y el decaimiento. En la Tabla~[Ref: tab:proy_tcra_pib] se presentan los escenarios de proyección según la configuración paramétrica elegida, evidenciando cómo la selección de la memoria ($W$) y el decaimiento ($\lambda$) influye directamente en los pronósticos. De hecho, se observa que las variantes de carácter local captan con mayor fidelidad la aceleración del crecimiento registrada en los últimos años del período analizado. Por su parte, la Figura~[Ref: fig:pib_tcra_grid] ilustra de manera gráfica la divergencia que surge entre las distintas variantes del operador a partir de 2024.

*Tabla: Modelación paramétrica divergente del PIB de Honduras (millones USD) para los años 2025--2026 bajo las cuatro formulaciones de la TCRA.* (tab:proy_tcra_pib)

| lccc@{}}

**Variante Operacional** | **Factor ($\hat{\beta**$)} | **Proyección 2025** | **Proyección 2026** |
| --- | --- | --- | --- |
| TCRA ($W=T, \lambda=1.0$) | 1.0704 | 39\,705.62 | 42\,501.61 |
| TCRAM ($W=5, \lambda=1.0$) | 1.0854 | 40\,259.77 | 43\,696.24 |
| ETCRA ($W=T, \lambda=0.9$) | 1.0754 | 39\,891.59 | 42\,900.67 |
| ETCRAM ($W=5, \lambda=0.9$) | 1.0876 | 40\,343.44 | 43\,878.04 |

\end{examplex}

![Serie histórica del PIB (2000--2024) y proyecciones (2025--2026) bajo las cuatro variantes del operador TCRA.](figures/EjemploPIB_TCRA_4Variantes.png)
*Figura: Serie histórica del PIB (2000--2024) y proyecciones (2025--2026) bajo las cuatro variantes del operador TCRA.* (fig:pib_tcra_grid)

### Validación Cruzada Expansiva en Mercado de Combustibles

Para evaluar el comportamiento del operador ante la volatilidad del mercado energético, se implementó un protocolo de validación cruzada temporal (*Expanding Window Cross-Validation*).

Sobre el historial de precios (2017--2025) de los cuatro combustibles (Superior, Regular, Diésel y Kerosene), se partió el espacio en entrenamiento/prueba con ocho anclajes temporales expansivos y horizonte de un paso adelante:

$$

\mathcal{P} = \{60/40, 65/35, 70/30, 75/25, 80/20, 85/15, 90/10, 95/5\}

$$

donde el primer valor representa el radio de inercia in-sample y el segundo la frontera out-of-sample. Se iteró una malla discreta limitando el ancho temporal $W \in [2, 10]$ semanas, y el factor entrópico $\lambda \in [0.70, 1.00]$, calculando el desempeño de la métrica por barrido empírico general (Tabla~[Ref: tab:cv_combustibles_tcra]). A partir de estos resultados, se observa una reducción constante en el valor del MAPE conforme se amplía la ventana de estimación. Este comportamiento no solo confirma la estabilidad operativa de la TCRA, sino también su viabilidad para modelar series de tiempo caracterizadas por una alta frecuencia de registro.

*Tabla: Evolución del error predictivo porcentual absoluto (MAPE) a lo largo de los anclajes temporales de validación un-paso-adelante para los cuatro combustibles.* (tab:cv_combustibles_tcra)

| lccccccccc@{}} | **Desempeño MAPE (\%) por Partición Expansiva** |  |  |  |  |  |  |  |  |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| \cmidrule(lr){2-9}
**Macro-Serie** | **60/40** | **65/35** | **70/30** | **75/25** | **80/20** | **85/15** | **90/10** | **95/5** | **Promedio Global** |
| **Superior** | 0.71\% | 0.67\% | 0.60\% | 0.53\% | 0.50\% | 0.52\% | 0.53\% | 0.53\% | **0.57\%** |
| **Regular** | 0.70\% | 0.66\% | 0.61\% | 0.57\% | 0.48\% | 0.47\% | 0.51\% | 0.48\% | **0.56\%** |
| **Diésel** | 0.80\% | 0.77\% | 0.65\% | 0.62\% | 0.62\% | 0.58\% | 0.60\% | 0.56\% | **0.65\%** |
| **Kerosene** | 1.07\% | 0.99\% | 0.90\% | 0.80\% | 0.80\% | 0.78\% | 0.82\% | 0.83\% | **0.87\%** |

Los resultados muestran que la configuración óptima para las cuatro series de combustibles requiere una ventana local $W=2$ para el operador de tasa de cambio. Esto no implica que la predicción global del modelo dependa únicamente de dos datos históricos. La ventana $W=2$ determina la longitud del bloque temporal para calcular la tasa de crecimiento local instantánea sin incorporar distorsiones por tendencias pasadas. Una vez calculada la secuencia de tasas $\{\alpha_t\}$, la matriz de transición de la cadena de Markov (que se introduce en el Capítulo~[Ref: cap:TCRA-Markov]) se estima utilizando la muestra completa de datos históricos, capturando la memoria de largo plazo del proceso.

Este hallazgo de $W=2$ refleja que la estructura de autocorrelación lineal en tasas es débil, lo que en finanzas se asocia a la hipótesis de eficiencia débil del mercado. No obstante, esto no implica que la serie se comporte como una caminata aleatoria pura no predecible. Aunque el componente lineal de largo plazo sea ruido, la serie de tasas de cambio relativa $\{\alpha_t\}$ exhibe dinámicas no lineales de cambio de régimen (por ejemplo, períodos de alta volatilidad frente a estabilidad regulada por subsidios estatales). La modelación híbrida mediante cadenas de Markov desarrollada en los capítulos siguientes explota estas dependencias no lineales de régimen para generar pronósticos.

## Relación con Métodos Establecidos

La familia TCRA comparte fundamentos con modelos y métodos clásicos de análisis de series temporales. A continuación se resumen las principales comparaciones.

**Relación entre el Operador TCRA y el Método de Mínimos Cuadrados Ordinarios.** El estimador del operador TCRA global se obtiene resolviendo una funcional mediante el método de mínimos cuadrados ordinarios (OLS). La equivalencia radica en que la formulación matemática de $\hat{\beta}_T$ (Ecuación~\eqref{eq:tcra_global}) es idéntica a la fórmula del estimador OLS para la pendiente de una regresión lineal sin intercepto entre $v(t)$ y $v(t-1)$, compartiendo una complejidad computacional \(\mathcal{O}(T)\).

**Relación del Operador TCRA con los Modelos ARIMA.** Un modelo AR(1) sin intercepto, \(v(t) = \phi\,v(t-1) + \varepsilon(t)\), produce el mismo estimador OLS que la TCRA, con \(\alpha_T = \hat{\phi} - 1\). La distinción de la familia TCRA radica en que no se concibe como un proceso estocástico global estático, sino como un operador de transformación local de series temporales que genera estados dinámicos $\{\alpha_t\}$ aptos para su posterior discretización en cadenas de Markov. Sin embargo, la modelación ARIMA es estructuralmente más completa al generalizar a órdenes superiores (AR($p$), MA($q$), diferenciación), capturando estacionalidad y retardos múltiples a costa de mayor complejidad \(\mathcal{O}(T^2)\) [Cita: Box2015].

**Comparación con la Estimación por Máxima Verosimilitud (MLE).** MLE es un método de estimación que asume un marco probabilístico (e.g., normalidad de errores) y ofrece intervalos de confianza formales, pero requiere optimización iterativa con complejidad \(\mathcal{O}(T^2)\) o superior. La TCRA, sin supuestos distribucionales explícitos, proporciona una solución analítica cerrada [Cita: Greene2018].

**ETCRA y ETCRAM vs.\ GLS.** Como se demostró en el Teorema~[Ref: teo:optimalidad_general], las variantes con decaimiento exponencial coinciden con estimadores GLS bajo estructuras de covarianza proporcionales a \(\lambda^{-(t-k)}\), lo que las posiciona como casos particulares de la teoría de mínimos cuadrados generalizados.

**TCRAM y ETCRAM vs.\ Modelos con ventanas.** El enfoque de ventana móvil es análogo al utilizado en medias móviles y filtros adaptativos, pero la familia TCRA optimiza un factor multiplicativo en lugar de un promedio aritmético, preservando la interpretación como tasa de cambio relativa.

**TCRA vs.\ Modelos de Aprendizaje Profundo (Redes Neuronales Recurrentes).** Modelos como las redes LSTM (*Long Short-Term Memory*) o GRU capturan dependencias no lineales complejas a largo plazo en series temporales. Sin embargo, su entrenamiento requiere optimización no convexa mediante retropropagación a través del tiempo, con un costo computacional de $\mathcal{O}(T \cdot L)$ por época ($L$ representa el número de conexiones y parámetros de la red), además de exigir una muestra de entrenamiento muy grande. En contraste, las variantes TCRA preservan la interpretabilidad analítica, garantizan la convexidad global del estimador y se calculan de forma instantánea incluso en series extremadamente cortas.

## Resumen del Capítulo

En este capítulo se ha desarrollado la familia de métodos TCRA para el modelado de series temporales económicas y financieras, compuesta por cuatro variantes jerárquicamente relacionadas. Los resultados obtenidos se resumen a continuación:

    * Se estableció un **marco general unificado** (ecuación~[Ref: eq:tcra_general]) del cual TCRA, TCRAM, ETCRA y ETCRAM se derivan como casos particulares, controlados por los parámetros $\lambda$ y $W$.

    * Se demostraron rigurosamente las propiedades de **existencia y unicidad** del estimador $\hat{\beta}$ para todas las variantes, basándose en la convexidad estricta de la función objetivo cuadrática.

    * Se probó la **consistencia asintótica** de los estimadores bajo modelos AR(1) estacionarios, la **continuidad** respecto a perturbaciones paramétricas, la **estabilidad** frente a perturbaciones acotadas (con cotas de tipo Lipschitz), y la **optimalidad** en el sentido de Gauss-Markov [Cita: Aitken1936, Greene2018] (OLS para TCRA/TCRAM, GLS para ETCRA/ETCRAM).

    * Se analizó la **complejidad computacional**, que oscila entre \(\mathcal{O}(T)\) para TCRA y ETCRA, y \(\mathcal{O}(T \cdot W)\) para las variantes con ventana móvil, siendo en todos los casos inferior a la de ARIMA.

    * Se proporcionó una **implementación computacional unificada** y ejemplos numéricos con datos del PIB de Honduras, que ilustran la aplicabilidad del método y su capacidad de pronóstico.

Las variantes presentadas se restringen a relaciones lineales y son sensibles a valores atípicos. La integración de técnicas de robustez (estimadores M, funciones de influencia) queda como trabajo futuro. En el capítulo siguiente se introduce la **TCRA-Markov**, que añade dependencias de régimen mediante cadenas de Markov para capturar cambios estructurales en la dinámica de la serie.

\chapter[TCRA con Cadenas de Markov]{Fundamentos Estocásticos:\\ Integración de TCRA con Cadenas de Markov}

Este capítulo conecta el operador TCRA del Capítulo~[Ref: cap:tcra] con cadenas de Markov. Las variantes deterministas estiman tasas de cambio relativas; aquí se añade un componente probabilístico que modela transiciones entre regímenes económicos y permite predecir rangos estocásticos sobre el comportamiento futuro de $v(t)$.

## Fundamentos de Invarianza y Estabilidad

\begin{theorem}[Invarianza estructural bajo transformaciones]

Sea $g\colon \mathbb{R} \to \mathbb{R}$ una transformación monótona
y Lipschitz continua con constante $L_g$. Si la serie $v(t)$ se
reemplaza por $\tilde{v}(t) = g(v(t))$, las propiedades de estabilidad local, existencia y unicidad del operador TCRA se preservan (con constantes escaladas que dependen de $L_g$). Sin embargo, la consistencia asintótica del estimador autorregresivo requiere supuestos restrictivos sobre la linealidad de la transformación $g$ para evitar distorsiones estructurales en el proceso autorregresivo.
\end{theorem}

\begin{remark}[Naturaleza discontinua de $\pi$]

La función de cuantización $\pi\colon \mathbb{R} \to \mathcal{S}$
es, por construcción, una función escalonada: asigna un estado
discreto $s_j$ a cada intervalo del espacio continuo de $\alpha_t$.
Por construcción, $\pi$ es **discontinua** en los umbrales de
decisión y, en sentido estricto, no es Lipschitz continua.  Sin
embargo, la estabilidad del sistema completo se preserva por el
mecanismo descrito en el Lema~[Ref: lem:estabilidad_discreta] a
continuación.
\end{remark}

\begin{lemma}[Estabilidad discreta ante perturbaciones]

Sean $\{v(t)\}$ y $\{\tilde{v}(t)\}$ dos series con
$\sup_t |v(t) - \tilde{v}(t)| \leq \epsilon$. Sea
$\delta_{\min} = \min_{j} |\theta_{j+1} - \theta_j|$ la
separación mínima entre umbrales consecutivos de la cuantización $\pi$. Si la perturbación
en el operador satisface $|\alpha_t - \tilde{\alpha}_t|
\leq C_t \epsilon$ (Lema~[Ref: lem:estabilidad_general]), entonces:

  * Si la distancia de la tasa al conjunto de umbrales satisface $\mathrm{dist}(\alpha_t, \{\theta_j\}) > C_t \epsilon$, el estado cuantizado es invariante ante la perturbación:
        $S_t = \tilde{S}_t$.
  * En general, el número de instantes donde
        $S_t \neq \tilde{S}_t$ está acotado por el número de
        instantes en los cuales la tasa de cambio $\alpha_t$ se encuentra a
        distancia menor o igual que $C_t \epsilon$ de algún umbral.

\end{lemma}

\begin{proof}
El punto~(1) se sigue directamente de la definición de la función escalonada $\pi$. Si la distancia de $\alpha_t$ a cualquier umbral de decisión $\theta_j$ es estrictamente mayor que $C_t \epsilon$, entonces cualquier perturbación local $|\tilde{\alpha}_t - \alpha_t| \leq C_t \epsilon$ no tiene la magnitud suficiente para cruzar la frontera de decisión, manteniendo a $\tilde{\alpha}_t$ en el mismo intervalo abierto de cuantización que $\alpha_t$, por lo que $\pi(\tilde{\alpha}_t) = \pi(\alpha_t)$. El punto~(2) es consecuencia directa, ya que un cambio de régimen cuantizado $S_t \neq \tilde{S}_t$ requiere necesariamente que la tasa caiga en la franja crítica de transición $\alpha_t \in [\theta_j - C_t\epsilon,\, \theta_j + C_t\epsilon]$ para algún umbral $\theta_j$.
\end{proof}

## Modelo Híbrido TCRA-Markov

Sea $\{v(t)\}_{t=0}^T$ una serie temporal económica o financiera.
Se calcula la tasa:

$$
\begin{aligned}
\alpha_{t,\lambda,W} &:= -1 + \frac{\sum_{\tau=t-W+1}^{t}
  \lambda^{t-\tau} v(\tau) v(\tau-1)}{\sum_{\tau=t-W+1}^{t}
  \lambda^{t-\tau} v(\tau-1)^2},
   \\
S_t &:= \pi(\alpha_{t,\lambda,W}) 
\end{aligned}
$$

\begin{definition}[Función de cuantización]

La función de asignación $\pi\colon \mathbb{R} \to \mathcal{S}$
asigna a cada valor de $\alpha_t$ un estado discreto según
umbrales fijos $\{\theta_j\}$:

$$

\pi(x) = \begin{cases}
  s_1 & \text{si } x > \theta_1 \\
  s_2 & \text{si } \theta_2 < x \leq \theta_1 \\
  s_3 & \text{si } \theta_3 \leq x \leq \theta_2 \\
  s_4 & \text{si } x < \theta_3
\end{cases}

$$

con umbrales típicos $\theta_1 = 0.05$, $\theta_2 = 0$,
$\theta_3 = -0.05$
\footnote{Estos valores son consistentes con las categorías de
clasificación del crecimiento del PIB real utilizadas por el
FMI en su herramienta de visualización
DataMapper~[Cita: IMFDataMapper2025] y con la metodología de
regímenes de crecimiento por cadenas de Markov desarrollada
por Imam y Temple~[Cita: ImamTemple2025] para 108 economías en
desarrollo.}.  En la implementación empírica del
Capítulo~[Ref: cap:regimenes_combustibles], los umbrales se
determinan por K-medias, lo que los adapta a la distribución de
cada serie.  La función $\pi$ es discontinua en los umbrales
(véase Observación~[Ref: rem:pi_discontinua]).
\end{definition}

\begin{examplex}
Se presenta a continuación un ejemplo numérico simplificado con una serie de juguete sumamente corta. El objetivo es meramente didáctico e ilustrativo, facilitando al lector la verificación manual y el seguimiento aritmético de las operaciones del operador y la cuantización de estados.

Para la serie artificial $v = [1.0,\, 1.02,\, 1.03,\, 1.025]$ con $\lambda = 1$ y tamaño de ventana local $W = 2$, el cálculo de la tasa óptima generalizada en el instante $t = 2$ involucra los datos $v(1) = 1.02$, $v(2) = 1.03$ y la condición inicial $v(0) = 1.0$:

$$
\begin{aligned}
\alpha_{2,1,2} &= -1 + \frac{v(2)\,v(1) + v(1)\,v(0)}
                           {v(1)^2 + v(0)^2}
  = -1 + \frac{1.03 \times 1.02 + 1.02 \times 1.0}
             {1.02^2 + 1.0^2} \\
  &= -1 + \frac{1.0506 + 1.02}{1.0404 + 1.0}
  = -1 + \frac{2.0706}{2.0404} \approx 0.0148.
\end{aligned}
$$

Dado que la tasa calculada cae dentro del intervalo umbral $0 < 0.0148 \leq 0.05$, el operador de cuantización asigna a este instante el estado discreto $S_2 = s_2$ (crecimiento moderado).
\end{examplex}

## Partición de Estados

Una posible discretización de \(\mathbb{R}\) basada en umbrales económicos es:

$$
\begin{aligned}
s_1 &= (0.05, +\infty) & \text{(Crecimiento fuerte)},\\
s_2 &= (0, 0.05] & \text{(Crecimiento moderado)},\\
s_3 &= [-0.05, 0] & \text{(Decrecimiento leve)},\\
s_4 &= (-\infty, -0.05) & \text{(Decrecimiento severo)}.
\end{aligned}
$$

Los puntos de corte $\pm 0.05$ son consistentes con los rangos de
clasificación del crecimiento real del PIB empleados por el Fondo
Monetario Internacional\footnote{Véase la clasificación visual del
IMF DataMapper~[Cita: IMFDataMapper2025], que emplea los rangos
$\geq 6\%$, $3$--$6\%$, $0$--$3\%$, $-3$--$0\%$ y $< -3\%$
para el crecimiento real del PIB.}.  En los capítulos
empíricos ([Ref: cap:tcroc_hibrido_integrado]
y~[Ref: cap:regimenes_combustibles]), estos umbrales se sustituyen por
los centroides de K-medias, que determinan la partición de forma
endógena a partir de la distribución observada de~$\alpha_t$.

Conviene advertir una diferencia de convención con esos capítulos. Aquí los estados siguen un orden descendente de carácter pedagógico, con $s_1$ como el crecimiento más fuerte y $s_4$ como el decrecimiento severo, según la clasificación del FMI. En la implementación numérica del Capítulo~[Ref: cap:regimenes_combustibles], K-medias ordena los estados de forma ascendente por la magnitud de sus centroides, de modo que $s_1$ pasa a ser la caída más fuerte y $s_K$ el alza mayor. El cambio de orden es solo notacional y no altera la dinámica estimada.

\begin{lemma}
La colección de intervalos discreta \(S = \{s_1, s_2, s_3, s_4\}\) representa una partición matemática formal del espacio real \(\mathbb{R}\). Por consiguiente, cada tasa de cambio estimada $\alpha_t \in \mathbb{R}$ pertenece a un único estado en cualquier instante dado, sin solapamientos ni indeterminaciones.
\end{lemma}

\begin{proof}
La demostración consiste en verificar directamente las dos condiciones definitorias de una partición sobre los intervalos especificados en \eqref{eq:state_assignment}:

* **Cobertura completa y exhaustiva:** La unión de todos los conjuntos cubre la totalidad del espacio real: \(\bigcup_{j=1}^4 s_j = (-\infty, -0.05) \cup [-0.05, 0] \cup (0, 0.05] \cup (0.05, +\infty) = \mathbb{R}\).
* **Disyunción mutua:** Las intersecciones por pares son vacías: \(s_i \cap s_j = \emptyset\) para cualquier par de índices \(i \neq j\).

\end{proof}

**Observación:** La irreducibilidad práctica de \(\{S_t\}\) se garantiza si \(\forall s_i,s_j \exists t: P^t(s_j|s_i) > 0\). Véase también discusión en Apéndice [Ref: apx:teorema_phi_mixing].

## Propiedades Markovianas y Asintóticas

\begin{theorem}[Estructura Markoviana de primer orden]

Sea $\{v(t)\}_{t=0}^T$ un proceso estrictamente estacionario con
momentos de cuarto orden finitos.  Sea $\alpha_t = \alpha_{t,\lambda,W}$
el operador ETCRAM con parámetros fijos $\lambda \in (0,1]$ y
$W \in \mathbb{N}$, y sea $S_t = \pi(\alpha_t)$ la secuencia de
estados discretos inducida por una función de cuantización $\pi$ con
umbrales fijos.  Entonces $\{S_t\}_{t=W}^{T}$ satisface
la propiedad de Markov de primer orden:

$$

\mathbb{P}(S_{t+1} = s_j \mid S_t = s_i, S_{t-1}, \ldots, S_W)
= \mathbb{P}(S_{t+1} = s_j \mid S_t = s_i)
\eqdef P_{ji},

$$

con probabilidades de transición $P_{ji}$ independientes de $t$
(homogeneidad temporal).

Cabe aclarar la notación empleada para los subíndices de la probabilidad condicional de transición $P_{ji}$ en \eqref{eq:transicion_markov}. El primer subíndice ($j$) denota el estado de destino en el instante $t+1$, mientras que el segundo subíndice ($i$) representa el estado de origen en el instante $t$. Esta inversión en el orden usual de la probabilidad condicional responde al estándar del álgebra matricial de procesos estocásticos, facilitando que la matriz de transición $\mat{P}$ multiplique directamente por la izquierda a vectores de estado definidos como columnas, de modo que $\vect{\pi}_{t+1} = \mat{P}\vect{\pi}_t$.
\end{theorem}

\begin{proof}
La demostración procede en tres etapas.

**Dependencia funcional de $\alpha_t$.**
Por definición, $\alpha_t$ depende de $v(t), v(t-1), \ldots,
v(t-W)$, es decir, de exactamente $W+1$ valores de la serie.
Escribimos
$\alpha_t = g(v(t-W), \ldots, v(t))$
donde $g\colon \mathbb{R}^{W+1} \to \mathbb{R}$ es una función
medible determinada por la fórmula~\eqref{eq:tcra_markov_alpha}.

**Markovianidad del bloque.**
Como $\{v(t)\}$ es estrictamente estacionario, el vector
$\vect{z}_t = (v(t-W), \ldots, v(t))^\top$ es un proceso
estacionario en $\mathbb{R}^{W+1}$.  Si además $\{v(t)\}$ es
un proceso AR(1) estacionario (como se supone en el
Capítulo~[Ref: cap:tcra], Teorema~[Ref: teo:consistencia_general]),
entonces $\vect{z}_t$ es un proceso Markoviano en
$\mathbb{R}^{W+1}$, pues $v(t+1)$ depende únicamente de $v(t)$
más el ruido $\varepsilon_{t+1}$.  Esto implica que
$\alpha_{t+1} = g(\vect{z}_{t+1})$ depende del pasado solo a
través de $\vect{z}_t$ (y no de $\vect{z}_{t-1}, \vect{z}_{t-2},
\ldots$).

**Reducción a estados discretos.**
Como $\pi$ es una función determinista, $S_t = \pi(\alpha_t)$ es
una función medible de $\vect{z}_t$.  La propiedad de Markov de
$\vect{z}_t$ se proyecta entonces al proceso discreto: la
distribución condicional de $S_{t+1}$ dado $S_t, S_{t-1}, \ldots$
depende del pasado solo a través de $\vect{z}_t$, y por tanto a
través de $S_t$ (dado que la cuantización preserva la separación
de regímenes bajo estacionariedad).  La homogeneidad temporal se
sigue de la estacionariedad estricta de $\{v(t)\}$.

**Nota.** En rigor, dado que la función de cuantización $\pi$ no es inyectiva, la discretización de una serie continua Markoviana puede violar las condiciones de agrupabilidad (*lumpability*) de Kemeny y Snell (1960). Esto destruye formalmente la propiedad de Markov de primer orden en el espacio discreto, dando lugar a procesos con memoria de largo plazo o a dinámicas asimilables a modelos de Markov ocultos (HMM). Sin embargo, el modelo TCRA-Markov adopta la propiedad de primer orden como una aproximación operativa de diseño, justificada por su desempeño predictivo fuera de muestra.
\end{proof}

\begin{assumption}[Aproximación Markoviana de primer orden]

La secuencia de estados discretos $\{S_t\}_{t=W}^T$ inducida por
la cuantización $S_t = \pi(\alpha_t)$ se modela como una cadena de
Markov homogénea de primer orden. Es decir, se asume que:

$$

\mathbb{P}(S_{t+1} \mid S_t, S_{t-1}, \ldots, S_W)
\approx \mathbb{P}(S_{t+1} \mid S_t) = P_{S_{t+1}, S_t}

$$

para todo $t \geq W$. Este supuesto constituye una aproximación de diseño operacional. En la práctica, dado que las agrupaciones no inyectivas (como K-medias o umbrales fijos) violan casi con certeza las condiciones algebraicas de agrupabilidad exacta (*lumpability*) de Kemeny y Snell~[Cita: Kemeny1960], el proceso discreto $\{S_t\}$ no es estrictamente markoviano en sentido matemático riguroso. Su adopción como estructura de primer orden se justifica mediante validación empírica, verificando a posteriori si la incorporación de retardos adicionales (modelos de segundo orden o superiores) aporta información estadísticamente significativa en el análisis de autocorrelación parcial de la secuencia de estados.
\end{assumption}

Antes de examinar la propiedad de mezcla de la cadena, es oportuno precisar matemáticamente los conceptos de irreducibilidad y aperiodicidad para cadenas de Markov sobre espacios de estados finitos:

    * **Irreducibilidad:** La cadena $\{S_t\}$ es irreducible si todos sus estados se comunican entre sí. Es decir, para cualquier par de estados $s_i, s_j \in \mathcal{S}$, existe una probabilidad no nula de transitar de uno a otro en un número finito de pasos: $\mathbb{P}(S_{t+n} = s_j \mid S_t = s_i) > 0$ para algún $n \geq 1$.
    * **Aperiodicidad:** El periodo de un estado $s_i \in \mathcal{S}$ se define como el máximo común divisor de los tiempos de retorno a sí mismo: $d(s_i) = \mathrm{mcd}\{n \geq 1 : P^n_{ii} > 0\}$. Un estado es aperiódico si $d(s_i) = 1$. La cadena es aperiódica si todos sus estados son aperiódicos, descartando comportamientos oscilatorios fijos.

\begin{theorem}[$\phi$-mixing con tasa geométrica]

Si la cadena $\{S_t\}$ es irreducible y aperiódica sobre el espacio
de estados finito $\mathcal{S} = \{s_1, \ldots, s_K\}$, entonces es
$\phi$-mixing con tasa de decaimiento exponencial, es decir, existen
constantes $C_\phi > 0$ y $\rho \in (0,1)$ tales que

$$

\phi(n) \eqdef
\sup_{A \in \mathcal{F}_0^t,\, B \in \mathcal{F}_{t+n}^\infty}
\bigl|\mathbb{P}(B \mid A) - \mathbb{P}(B)\bigr|
\leq C_\phi \, \rho^n
\quad \forall\, n \geq 1.

$$

\end{theorem}

\begin{proof}
Sea $\mat{P}$ la matriz de transición de la cadena sobre
$\mathcal{S}$, con $K$ estados.  Por irreducibilidad y aperiodicidad
en espacio finito, la cadena posee una única distribución
estacionaria $\vect{\pi}$ con $\pi_j > 0$ para todo $j$
[Cita: Norris1998, Levin2009].  El Teorema
de Convergencia para cadenas ergódicas finitas establece que

$$

\bigl\|P^n(s_i, \cdot) - \vect{\pi}\bigr\|_{\mathrm{TV}}
\leq C_0 \, \rho_0^n,

$$

donde $\rho_0 = 1 - \gamma$ y $\gamma > 0$ es la brecha espectral (*spectral gap*) de
$\mat{P}$ (diferencia entre el primer y segundo autovalor en módulo).
En espacio de estados finito, $\gamma > 0$ si y solo si la cadena es
irreducible y aperiódica [Cita: Levin2009].

La convergencia en variación total implica mezcla uniforme
(*uniform mixing*), que a su vez implica $\phi$-mixing con la
misma tasa exponencial [Cita: Bradley2005].  Concretamente,
$\phi(n) \leq C_\phi \rho_0^n$ con $C_\phi \leq K / \min_j \pi_j$.
\end{proof}

\begin{remark}[Brecha espectral e inercia del mercado]

El parámetro $\gamma$ tiene una interpretación económica directa: una
brecha espectral pequeña indica que el mercado cambia de régimen
lentamente (alta inercia), mientras que una brecha grande indica
transiciones rápidas.  El tiempo de mezcla, definido como
$t_{\mathrm{mix}} = \lceil \gamma^{-1} \ln(2K/\min_j \pi_j) \rceil$,
cuantifica cuántas observaciones son necesarias para que la cadena
"olvide" su estado inicial.  Este parámetro se estima
empíricamente en el Capítulo~[Ref: cap:regimenes_combustibles].
\end{remark}

\begin{proposition}[Normalidad asintótica de $\hat{\mat{P}}$]

Sea $\{S_t\}$ una cadena de Markov ergódica con $K$ estados y matriz
de transición verdadera $\mat{P}$.  Sea $N_{ij}$ el número de
transiciones observadas de $s_j$ a $s_i$ en $T$ pasos, y
$N_{\cdot j} = \sum_{i=1}^K N_{ij}$ el total de salidas del
estado~$s_j$.  El estimador de máxima verosimilitud
$\hat{P}_{ij} = N_{ij} / N_{\cdot j}$ satisface, para cada columna
$j$ fija:

$$

\sqrt{N_{\cdot j}}\,
(\hat{P}_{1j} - P_{1j},\; \ldots,\; \hat{P}_{Kj} - P_{Kj})^\top
\xrightarrow{d}
\mathcal{N}\!\bigl(\vect{0},\;
\mathrm{diag}(\vect{p}_j) - \vect{p}_j \vect{p}_j^\top\bigr),

$$

donde $\vect{p}_j = (P_{1j}, \ldots, P_{Kj})^\top$ es la $j$-ésima
columna de $\mat{P}$.  Además, los estimadores de distintas columnas
$j \neq j'$ son asintóticamente independientes.
\end{proposition}

\begin{proof}
La demostración es un resultado clásico de
Anderson y Goodman~[Cita: AndersonGoodman1957].  Se esboza aquí
la estructura.

**Multinomial condicionada.**
Condicionado a que el proceso visita el estado $s_j$ exactamente
$N_{\cdot j}$ veces, las $N_{\cdot j}$ transiciones de salida de
$s_j$ son, condicionalmente, ensayos multinomiales independientes
con parámetro $\vect{p}_j = (P_{1j}, \ldots, P_{Kj})^\top$.

**TCL multinomial.**
Por el Teorema Central del Límite para la distribución multinomial,

$$

\sqrt{N_{\cdot j}}\,(\hat{\vect{p}}_j - \vect{p}_j)
\xrightarrow{d}
\mathcal{N}(\vect{0}, \mat{\Sigma}_j),
\quad \text{donde } \mat{\Sigma}_j
= \mathrm{diag}(\vect{p}_j) - \vect{p}_j\vect{p}_j^\top.

$$

**Independencia entre columnas.**
Las visitas a distintos estados generan conteos asintóticamente
independientes por la propiedad de mezcla
(Teorema~[Ref: thm:phi_mixing]).  Formalmente, Anderson y
Goodman~[Cita: AndersonGoodman1957] demuestran que las componentes
de distintas columnas son asintóticamente independientes.

**De $N_{\cdot j**$ a $T$.}
Por la ley de los grandes números para cadenas ergódicas,
$N_{\cdot j}/T \xrightarrow{\mathrm{a.s.}} \pi_j > 0$.  Por
el Lema de Slutsky, se puede reescalar por $\sqrt{T}$:

$$

\sqrt{T}\,(\hat{P}_{ij} - P_{ij})
\xrightarrow{d} \mathcal{N}\!\left(0,\;
\frac{P_{ij}(1 - P_{ij})}{\pi_j}\right),

$$

lo que completa la demostración.
\end{proof}

\begin{remark}[Velocidad de convergencia]

Bajo la tasa de mezcla exponencial del
Teorema~[Ref: thm:phi_mixing], la aproximación normal admite una
cota de Berry-Esseen de orden $O(T^{-1/2})$
[Cita: Bolthausen1982]:

$$

\sup_x \bigl|\mathbb{P}(\sqrt{T}(\hat{P}_{ij} - P_{ij}) \leq x)
- \Phi(x)\bigr| = O(T^{-1/2}),

$$

donde $\Phi$ denota la función de distribución normal estándar.
\end{remark}

\begin{corollary}[Intervalos de confianza para $\hat{P}_{ij}$]

Bajo las hipótesis de la Proposición~[Ref: prop:normalidad_asintotica],
un intervalo de confianza asintótico de nivel $1 - \gamma$ para
cada probabilidad de transición $P_{ij}$ es

$$

\hat{P}_{ij} \pm z_{\gamma/2} \cdot
\hat{\sigma}_{ij}, \qquad
\hat{\sigma}_{ij} =
\sqrt{\frac{\hat{P}_{ij}(1 - \hat{P}_{ij})}{N_{\cdot j}}},

$$

donde $z_{\gamma/2}$ es el cuantil $1-\gamma/2$ de la distribución
normal estándar y $N_{\cdot j}$ es el número total de transiciones
observadas desde el estado $s_j$.
\end{corollary}

\begin{proof}
De la Proposición~[Ref: prop:normalidad_asintotica], cada entrada
$\hat{P}_{ij}$ es asintóticamente normal con varianza
$P_{ij}(1-P_{ij})/N_{\cdot j}$.  Sustituyendo $P_{ij}$ por su
estimador $\hat{P}_{ij}$ y aplicando el Lema de Slutsky se
obtiene~\eqref{eq:ic_pij}.
\end{proof}

\begin{remark}[Intervalos de confianza en la práctica]

En los capítulos empíricos
([Ref: cap:tcroc_hibrido_integrado]
y~[Ref: cap:regimenes_combustibles]), las matrices de transición
estimadas se reportan junto con los errores estándar
$\hat{\sigma}_{ij}$ del
Corolario~[Ref: cor:ic_transicion].  Para series con pocas
transiciones ($N_{\cdot j} < 30$), se recomienda complementar
con intervalos bootstrap paramétrico, generando $B$ realizaciones
de la cadena a partir de $\hat{\mat{P}}$ y recalculando el
estimador en cada una.
\end{remark}

\begin{corollary}[Cota del error de predicción]

Bajo las hipótesis del Teorema~[Ref: thm:markov_primer_orden] y
suponiendo $\mathbb{E}[|v(t)|^4] < \infty$, el error cuadrático
medio de predicción del modelo TCRA-Markov satisface, para $W$
fijo y $\lambda \in (0,1]$:

$$

\mathrm{ECM}(\hat{v}_{T+1})
\eqdef \mathbb{E}\bigl[(v(T+1) - \hat{v}(T+1))^2\bigr]
= \sigma_\varepsilon^2
  + \frac{C(\lambda,W)}{W_{\mathrm{eff}}} + o(W_{\mathrm{eff}}^{-1}),

$$

donde:

  * $\sigma_\varepsilon^2$ es la varianza irreducible del ruido,
  * $W_{\mathrm{eff}}(\lambda, W)
        = \sum_{k=0}^{W-1} \lambda^k
        = \frac{1 - \lambda^W}{1 - \lambda}$
        es el tamaño efectivo de muestra,
  * $C(\lambda, W)$ es una constante acotada que depende de los
        momentos de $v(t)$ y es finita para todo
        $\lambda \in (0,1]$ y $W \geq 1$.

\end{corollary}

\begin{proof}
El error de predicción se descompone como

$$

v(T+1) - \hat{v}(T+1)
= \underbrace{\varepsilon_{T+1}}_{\text{ruido irreducible}}
+ \underbrace{(\beta^* - \hat{\beta}_{T,\lambda,W})
  \cdot v(T)}_{\text{error de estimación}}.

$$

El primer término contribuye $\sigma_\varepsilon^2$ al ECM.  Para el
segundo, la varianza de $\hat{\beta}_{T,\lambda,W}$ bajo el modelo
AR(1) es

$$

\mathrm{Var}(\hat{\beta}_{T,\lambda,W})
= \frac{\sigma_\varepsilon^2}{\sum_{k=0}^{W-1}
  \lambda^{2k}\, v(T-k)^2 / \bigl(\sum_{k=0}^{W-1}
  \lambda^k\bigr)^2 \cdot \mathrm{const}}
= O(W_{\mathrm{eff}}^{-1}).

$$

Como $\mathbb{E}[v(T)^2] < \infty$, el error de estimación
contribuye $O(\mathbb{E}[v(T)^2] / W_{\mathrm{eff}})$ al ECM.
La constante $C(\lambda, W) = O(\mathbb{E}[v(T)^2])$ es finita
bajo la hipótesis de momentos de cuarto orden, y el resultado
se sigue.
\end{proof}

\begin{remark}[Diferenciación de niveles de error]

Es fundamental precisar que el error cuadrático medio acotado en el Corolario~[Ref: cor:error_prediccion] corresponde al plano continuo: evalúa la predicción de la serie en niveles $\hat{v}_{T+1}$ a partir del estimador autorregresivo lineal continuo. Este análisis no mide la exactitud cualitativa de la predicción de regímenes discretos $S_{T+1}$ inducidos por la cadena de Markov. La evaluación cualitativa de la clasificación de regímenes utiliza métricas independientes (como la exactitud de clasificación y el F1-score), las cuales se analizan en los capítulos aplicados. Ambos niveles de error (continuo y cualitativo) son complementarios pero estructuralmente distintos.
\end{remark}

\begin{remark}[Comportamiento según $\lambda$]

La cota del Corolario~[Ref: cor:error_prediccion] revela el
compromiso entre $\lambda$ y $W$:

  * Si $\lambda = 1$ (pesos uniformes),
    $W_{\mathrm{eff}} = W$, y el error decrece como $O(1/W)$.
  * Si $\lambda < 1$ y $W$ es grande,
    $W_{\mathrm{eff}} \approx 1/(1-\lambda)$, de modo que el
    error se estabiliza en $O(1-\lambda)$ independientemente
    de cuánto se aumente $W$.
  * Para $\lambda \to 0^+$, $W_{\mathrm{eff}} \to 1$ y el
    estimador depende esencialmente de la última observación.
  * Si $W = T$ y $\lambda = 1$, el operador utiliza toda la
    muestra y produce un escalar constante $\alpha_T$, eliminando
    toda variación local.  En la práctica, $W$ debe elegirse
    mucho menor que $T$ para preservar la resolución temporal.

\end{remark}

## Resumen Visual del Modelo

La integración de todos los componentes del marco TCRA-Markov se ilustra en el diagrama unificado de la Figura~[Ref: fig:tcra_markov_completo].

![Evolución estructural del modelo TCRA hasta su extensión Markoviana.]()
*Figura: Evolución estructural del modelo TCRA hasta su extensión Markoviana.* (fig:tcra_markov_completo)

En este esquema se expone la progresión de la metodología propuesta, donde el nodo central representa la formulación canónica global ($W=T$, $\lambda=1$). Las extensiones convergen en el operador generalizado ETCRAM, cuya discretización mediante la función $\pi$ integra la tasa de cambio con cadenas de Markov. Los fundamentos teóricos, basados en la teoría ergódica, la propiedad de mezcla débil $\phi$-mixing y la estimación asintótica, sustentan la predicción estocástica de regímenes.

## Comparación de Modelos de Transición de Estados

Para situar el modelo integrado TCRA-Markov en el contexto de la literatura de series de tiempo, es necesario compararlo tanto con las variantes deterministas del operador como con los enfoques estocásticos de cambio de régimen. En la Tabla~[Ref: tab:comparacion_tcra_markov] se contrastan las propiedades fundamentales de las variantes del operador con el modelo integrado, evidenciando de qué manera la integración de la cadena de Markov introduce un componente puramente estocástico a un marco originalmente determinista. Este acoplamiento incrementa la capacidad de memoria del modelo, si bien requiere un costo computacional ligeramente superior.

*Tabla: Comparación entre variantes de TCRA y el modelo TCRA-Markov.* (tab:comparacion_tcra_markov)

| **Modelo** | **Determinista** | **Estocástico** | **Memoria** | **Complejidad** |
| --- | --- | --- | --- | --- |
| TCRA | Sí | No | Baja | \(\mathcal{O}(T)\) |
| TCRAM | Sí | No | Alta | \(\mathcal{O}(T \cdot W)\) |
| ETCRA | Sí | No | Media | \(\mathcal{O}(T)\) |
| ETCRAM | Sí | No | Alta | \(\mathcal{O}(T \cdot W)\) |
| **TCRA-Markov** | Sí | Sí | Muy Alta | \(\mathcal{O}(T \cdot W + K^2)\) |

Además de la comparación con sus propias variantes, resulta valioso contrastar el modelo TCRA-Markov con el modelo autorregresivo de cambio de régimen de Markov (Markov-Switching Autoregressive o MS-AR), introducido por Hamilton~[Cita: hamilton1989new] y ampliamente utilizado en econometría financiera. Ambos modelos comparten la hipótesis subyacente de que el comportamiento de la serie temporal cambia entre distintos estados o regímenes de volatilidad y crecimiento. Sin embargo, difieren sustancialmente en su diseño estructural y computacional:

    * **Observabilidad del régimen:** En el modelo MS-AR clásico, la variable de estado o régimen $S_t$ es un proceso latente no observable. Los regímenes deben inferirse probabilísticamente a partir de la serie histórica del PIB o los precios utilizando técnicas de filtrado y suavizado. En cambio, en el modelo TCRA-Markov, los estados son observables de forma determinista y directa en cada instante mediante la cuantización de la serie $\{\alpha_t\}$ resultante del operador local.
    
    * **Complejidad en la estimación de parámetros:** La estimación en los modelos MS-AR requiere maximizar una función de verosimilitud no convexa y altamente no lineal, habitualmente mediante el algoritmo de Esperanza-Maximización (EM) o filtros numéricos iterativos. Este proceso suele sufrir problemas de convergencia, alta sensibilidad a los valores iniciales y la existencia de múltiples óptimos locales. Por el contrario, al ser los estados del modelo TCRA-Markov completamente deterministas tras fijar los parámetros del operador ($W, \lambda$), la estimación de la matriz de transición de Markov se reduce a conteos de frecuencia analíticos sencillos (Proposición~[Ref: prop:normalidad_asintotica]), garantizando soluciones globales únicas de forma casi instantánea.
    
    * **Flexibilidad y suposiciones:** El modelo MS-AR asume una estructura de error paramétrica estricta (usualmente gaussiana). El modelo TCRA-Markov, al fundamentarse en un operador analítico que actúa como filtro local, no impone supuestos distribucionales rígidos sobre los residuos de la serie original, dotándolo de mayor robustez frente a colas pesadas o valores atípicos comunes en series financieras de alta frecuencia.

# Modelo Híbrido TCROC-Markov con Estimación NNLS

Este capítulo presenta una metodología que combina la discretización de regímenes económicos (TCROC-Markov) con la estimación de matrices de transición mediante Mínimos Cuadrados No Negativos (NNLS). Con este enfoque se abordan dos problemas frecuentes en la modelización de dinámicas económicas: la falta de interpretabilidad en modelos continuos y la dificultad de obtener matrices de transición válidas.

El enfoque tiene dos componentes:

[label=\roman*]
    * Una discretización que transforma series temporales continuas en estados económicos discretos, lo que facilita tanto el análisis cualitativo como cuantitativo de regímenes.
    * Un esquema de estimación basado en NNLS que garantiza, por construcción, que las matrices de transición obtenidas sean estocásticas: entradas no negativas y columnas que suman uno.

Se describe el flujo completo desde la obtención de la serie temporal hasta la estimación y validación de la matriz de transición. Se incluyen ejemplos con datos reales del PIB y precios de combustibles.

La Figura~[Ref: fig:diagrama_conceptual] ilustra el proceso completo.

![Diagrama conceptual de la metodología híbrida TCROC-Markov con estimación NNLS.]()
*Figura: Diagrama conceptual de la metodología híbrida TCROC-Markov con estimación NNLS.* (fig:diagrama_conceptual)

\section[Discretización de Regímenes con TCROC-Markov]{Discretización de Regímenes Económicos\\ con TCROC-Markov}

### Cálculo de la Tasa Relativa
El punto de partida es la transformación de una serie temporal económica $\{v(t)\}_{t=0}^T$ en una secuencia de tasas de cambio relativas \(\alpha_t\). Para cada instante \(t \geq W\), la tasa \(\alpha_{t,\lambda,W}\) se obtiene con el operador local generalizado ya definido en la Ecuación~\eqref{eq:tcra_markov_alpha} del Capítulo~[Ref: cap:TCRA-Markov]; para evitar la duplicidad, no se reproduce aquí su expresión y se remite a esa formulación. El cociente que define dicha tasa puede interpretarse como una proyección ponderada del vector de la serie temporal \(v\) sobre su versión rezagada \(v-1\) dentro de una ventana \(W\). El factor \(\lambda\) introduce una memoria exponencial que asigna mayor importancia a las observaciones más recientes.

### Asignación de Estados
A continuación, se define una función de cuantización \(\pi\) que mapea cada tasa continua \(\alpha_t\) a un estado discreto \(S_t\) perteneciente a un conjunto finito de regímenes económicos \(S = \{s_1, s_2, \dots, s_N\}\). Por ejemplo:

$$

S_t = \pi(\alpha_{t,\lambda,W}) = 
\begin{cases}
s_1 & \text{si } \alpha > 0.05 \text{ (Crecimiento fuerte)} \\
s_2 & \text{si } 0 < \alpha \leq 0.05 \text{ (Crecimiento moderado)} \\
s_3 & \text{si } -0.05 \leq \alpha \leq 0 \text{ (Decrecimiento leve)} \\
s_4 & \text{si } \alpha < -0.05 \text{ (Decrecimiento severo)}
\end{cases}

$$

Este proceso genera una secuencia de estados \(\{S_t\}_{t=0}^T\), que forma la base para el modelado de transiciones.  Los umbrales $\pm 0.05$ ilustrados en la Ecuación~[Ref: eq:state_assignment_pi] se basan en las categorías estándar de clasificación del crecimiento del PIB del FMI\footnote{Véase~[Cita: IMFDataMapper2025].  En el Ejemplo~[Ref: ex:discretizacion_tcroc], los umbrales se ajustan al rango empírico del PIB per cápita hondureño; en el Capítulo~[Ref: cap:regimenes_combustibles] se reemplazan por particiones endógenas vía K-Means.}.

![Ilustración del proceso de discretización. El panel superior muestra la evolución de la tasa continua~$\alpha_t$. El panel inferior muestra la secuencia de estados discretos~$S_t$ resultante de aplicar la función de cuantización~$\pi$.]()
*Figura: Ilustración del proceso de discretización. El panel superior muestra la evolución de la tasa continua~$\alpha_t$. El panel inferior muestra la secuencia de estados discretos~$S_t$ resultante de aplicar la función de cuantización~$\pi$.* (fig:proceso_discretizacion)

\begin{examplex}[Discretización y Preparación de Datos]

Considérese una serie temporal hipotética de inflación trimestral \(v(t)\) y los parámetros \(\lambda=1, W=2\). La secuencia de datos es:

$$
 v = \{v_0, v_1, v_2, v_3, v_4\} = \{4.0, 4.1, 4.15, 4.10, 4.05 \} 
$$

Dado que \(W=2\), la primera tasa \(\alpha_t\) que podemos calcular es para \(t=2\).
Aplicando la fórmula se obtienen los siguientes valores y la clasificación en estados:

\begin{center}
\begin{tabular}{cccl}
\toprule
\(t\) & \(\alpha_t\) & Estado & Justificación \\
\midrule
2 & \(\approx 0.0184\) & \(s_2\) & \(0 < 0.0184 \leq 0.05\) \\
3 & \(\approx -0.0001\) & \(s_3\) & \(-0.05 \leq -0.0001 \leq 0\) \\
4 & \(\approx -0.0121\) & \(s_3\) & \(-0.05 \leq -0.0121 \leq 0\) \\
\bottomrule
\end{tabular}
\end{center}

La secuencia de regímenes generada es \(\{S_2, S_3, S_4\} = \{s_2, s_3, s_3\}\). Esta secuencia se convierte a su representación *one-hot*\footnote{Codificación indicadora: cada estado categórico se representa como un vector binario con un único componente igual a 1 y el resto iguales a 0. Véase el Glosario de Términos.} (e.g., \(s_2 \rightarrow [0, 1, 0, 0]^T\)) para construir las matrices de datos \(X_0\) y \(X_1\), que son los insumos para la estimación descrita en la Sección [Ref: sec:estimacion_nnls].
\end{examplex}

![Visualización del proceso de discretización para el Ejemplo [Ref: ex:discretizacion_inflacion]()
*Figura: Visualización del proceso de discretización para el Ejemplo \ref{ex:discretizacion_inflacion* (fig:discretizacion_ejemplo_numerico)

\begin{examplex][Discretización del PIB per cápita usando TCROC]

Considérese la serie temporal del PIB per cápita en Lempiras a precios constantes de Honduras (2000--2023), presentada en la Tabla~[Ref: tbl:SeriesEvolution]. Aplicamos el paso de discretización TCROC con ventana \(W = 2\) y factor de memoria \(\lambda = 1\), usando la fórmula:

$$

\alpha_t = -1 + \frac{v(t)v(t-1) + v(t-1)v(t-2)}{v(t-1)^2 + v(t-2)^2}, \quad t \geq 2.

$$

Para \(t=2\) (año 2002), se obtiene \(\alpha_{2} \approx 0.0052\).
Como \(-0.01 \leq 0.0052 \leq 0.02\), el estado es \(\mathbf{S_2 = s_2}\).

Los valores de \(\alpha_t\) para \(t = 3\) hasta \(t = 23\) se calcularon con la misma fórmula. La siguiente tabla resume los resultados y la clasificación en cuatro regímenes:

    * \(s_1\): \(\alpha_t < -0.01\) (contracción fuerte)
    * \(s_2\): \(-0.01 \leq \alpha_t \leq 0.02\) (estabilidad o crecimiento débil)
    * \(s_3\): \(0.02 < \alpha_t \leq 0.04\) (crecimiento moderado)
    * \(s_4\): \(\alpha_t > 0.04\) (crecimiento fuerte)

\begin{center}
\begin{tabular}{c|c|c|c}
\(t\) & Año & \(\alpha_t\) & Estado \\
\hline
2  & 2002 & 0.0052 & \(s_2\) \\
3  & 2003 & 0.0147 & \(s_2\) \\
4  & 2004 & 0.0277 & \(s_3\) \\
5  & 2005 & 0.0358 & \(s_3\) \\
6  & 2006 & 0.0381 & \(s_3\) \\
7  & 2007 & 0.0393 & \(s_3\) \\
8  & 2008 & 0.0282 & \(s_3\) \\
9  & 2009 & -0.0136 & \(s_1\) \\
10 & 2010 & -0.0167 & \(s_1\) \\
11 & 2011 & 0.0166 & \(s_2\) \\
12 & 2012 & 0.0186 & \(s_2\) \\
13 & 2013 & 0.0139 & \(s_2\) \\
14 & 2014 & 0.0095 & \(s_2\) \\
15 & 2015 & 0.0151 & \(s_2\) \\
16 & 2016 & 0.0192 & \(s_2\) \\
17 & 2017 & 0.0246 & \(s_3\) \\
18 & 2018 & 0.0241 & \(s_3\) \\
19 & 2019 & 0.0134 & \(s_2\) \\
20 & 2020 & -0.0500 & \(s_1\) \\
21 & 2021 & -0.0106 & \(s_1\) \\
22 & 2022 & 0.0611 & \(s_4\) \\
23 & 2023 & 0.0211 & \(s_3\) \\
\end{tabular}
\end{center}

La secuencia de regímenes resultante es:

$$

\{S_t\}_{t=2}^{23} = \{s_2, s_2, s_3, s_3, s_3, s_3, s_3, s_1, s_1, s_2, s_2, s_2, s_2, s_2, s_2, s_3, s_3, s_2, s_1, s_1, s_4, s_3\}

$$

Esta secuencia se codifica en formato *one-hot* para construir las matrices de datos \(X_0\) y \(X_1\), utilizadas en la estimación de la matriz de transición mediante NNLS (Sección~[Ref: sec:estimacion_nnls]).
\end{examplex}

![Proceso completo de discretización del PIB per cápita en Lempiras (2002--2023). El panel superior muestra la evolución de la tasa~$\alpha_t$; el inferior, la secuencia de estados discretos~$S_t$. Se observan regímenes de contracción (2009, 2020), estabilidad y crecimiento fuerte~(2022).]()
*Figura: Proceso completo de discretización del PIB per cápita en Lempiras (2002--2023). El panel superior muestra la evolución de la tasa~$\alpha_t$; el inferior, la secuencia de estados discretos~$S_t$. Se observan regímenes de contracción (2009, 2020), estabilidad y crecimiento fuerte~(2022).* (fig:discretizacion_completa_real)

La dinámica de los regímenes económicos para el PIB per cápita, visible en la Figura~[Ref: fig:discretizacion_completa_real], ofrece una perspectiva cuantitativa de la historia económica reciente de Honduras. Se pueden extraer dos conclusiones principales:

\paragraph{Persistencia de Regímenes Durante un Período Político Extendido}
Se observa una notable persistencia de los estados \(s_2\) (estabilidad) y \(s_3\) (crecimiento moderado) durante la mayor parte del período analizado, particularmente entre 2004-2008 y 2011-2019. Este comportamiento de estabilidad y crecimiento predecible coincide en gran medida con los períodos de gobierno del Partido Nacional, incluyendo la administración de Juan Orlando Hernández (2014-2022). Las transiciones abruptas a \(s_1\) (contracción) en 2009 y 2020 marcan eventos de crisis bien conocidos (la crisis política de 2009 y la pandemia de COVID-19), interrumpiendo estos largos ciclos de estabilidad.

\paragraph{El Estado \(s_4\) como Anomalía Post-Pandemia}
La aparición única del estado \(s_4\) (crecimiento fuerte) en el año 2022 es particularmente significativa. Este régimen, que no se observa en ningún otro punto de los 22 años analizados, puede interpretarse como un efecto de "rebote" o una corrección anómala de la economía tras la drástica contracción de 2020. Su carácter excepcional sugiere que no representa una dinámica estructural sostenible, sino más bien una consecuencia transitoria de la recuperación post-COVID y de las condiciones macroeconómicas globales de ese año. La escasez de este estado en la muestra limita la capacidad del modelo para predecir eventos de crecimiento tan acelerado.

### Análisis de Sensibilidad de los Parámetros de Discretización
La discretización de la serie temporal mediante TCROC depende de dos hiperparámetros clave: la ventana de tiempo $W$ y el factor de memoria $\lambda$. Para evaluar la robustez de nuestra discretización, se realizó un análisis de sensibilidad exhaustivo sobre los datos del PIB per cápita, variando $W \in \{2, 3, 4, 5\}$ y $\lambda$ en el rango $[0.80, 1.0]$. La Tabla~[Ref: tab:sensibilidad_tcroc] presenta una selección representativa de estos resultados. El código completo utilizado para generar estos datos se encuentra en el Apéndice~[Ref: apx:codigo_sensibilidad].

*Tabla: Análisis de sensibilidad: Distribución de estados (\%) del PIB per cápita según los parámetros $W$ y $\lambda$.* (tab:sensibilidad_tcroc)

| **$W$** | **$\lambda$** | **\% $s_1$** | **\% $s_2$** | **\% $s_3$** | **\% $s_4$** |
| --- | --- | --- | --- | --- | --- |
| **2** | **1.00** | **18.2** | **40.9** | **36.4** | **4.5** |
| 2 | 0.90 | 13.6 | 45.5 | 36.4 | 4.5 |
| 3 | 1.00 | 4.8 | 57.1 | 33.3 | 4.8 |
| 3 | 0.80 | 4.8 | 57.1 | 38.1 | 0.0 |
| 4 | 1.00 | 5.0 | 70.0 | 25.0 | 0.0 |
| 4 | 0.80 | 5.0 | 65.0 | 30.0 | 0.0 |
| 5 | 1.00 | 0.0 | 79.0 | 21.1 | 0.0 |
| 5 | 0.80 | 5.3 | 68.4 | 26.3 | 0.0 |

Del análisis se desprenden dos conclusiones clave. Por un lado, **la metodología es altamente robusta a la elección de $\lambda$**; para una ventana $W$ fija, las variaciones en el factor de memoria tienen un impacto mínimo en la distribución de estados. Fijar $\lambda=1.0$ no es una decisión neutra: ese valor cancela el decaimiento exponencial y reparte el mismo peso entre todas las observaciones de la ventana, sin privilegiar las más recientes. Aquí esa ponderación uniforme basta, porque la robustez observada muestra que priorizar lo reciente no altera de forma apreciable la clasificación de estados. Por eso se adopta $\lambda=1.0$ por su simplicidad y efectividad.\\

Por otro lado, **el parámetro más influyente es la ventana $W$**, que actúa como un operador de suavizado. A medida que $W$ aumenta, la detección de regímenes extremos ($s_1$ y $s_4$) disminuye, favoreciendo al estado de estabilidad ($s_2$). En contrapartida, una ventana más amplia gana robustez frente al ruido de alta frecuencia, pero lo paga con un retraso dinámico: amortigua y demora la detección de los cambios bruscos de tendencia. Una ventana corta hace lo contrario, eleva la sensibilidad del operador y, con ella, la varianza de la clasificación de estados. Se seleccionó $W=2$ para el estudio principal, ya que representa un equilibrio que, si bien confirma el predominio de la estabilidad, es lo suficientemente sensible para capturar las contracciones económicas agudas correspondientes a eventos históricos conocidos. Una ventana de solo dos observaciones tampoco compromete la representatividad estadística del modelo: $W=2$ fija únicamente el bloque con el que se mide la tasa de cambio local instantánea, mientras que la matriz de transición $\hat{\mat{P}}$ se estima sobre la serie histórica completa de $T$ observaciones. La consistencia de los resultados a través de los parámetros valida la fiabilidad de las conclusiones del modelo.\\

La elección de una ventana corta como $W=2$ se alinea conceptualmente con la propiedad de Markov de primer orden que se asume en la fase de modelado. Si bien la propiedad de Markov se aplica a la secuencia de estados ($S_t$) y no a los datos brutos ($v(t)$), el uso de una ventana de discretización mínima evita incorporar una memoria a largo plazo en la definición de los propios estados, manteniendo así la simplicidad y consistencia metodológica en todo el proceso.

## Estimación de la Matriz de Transición Lineal con NNLS

Una vez obtenida la secuencia de estados discretos $\{S_t\}_{t=0}^{T}$, el siguiente paso es modelar su dinámica. Asumiendo un proceso de Markov de primer orden, la relación se describe mediante el modelo lineal $x(t+1) = Px(t)$. Para estimar la matriz de transición $P$, se emplea un enfoque de **Mínimos Cuadrados No Negativos (NNLS)**, siguiendo la metodología detallada en Vides y Lopez Gutierrez (2025)~[Cita: vides_lopez_srep_2025].

Este método resulta central porque garantiza, por construcción, las propiedades estocásticas de la matriz estimada ($\hat{P}_{ij} \geq 0$ y $\mathbf{1}^T \hat{\mat{P}} = \mathbf{1}^T$), resolviendo el siguiente problema de optimización:

$$

\hat{\mat{P}} = \arg\min_{P \in \mathcal{M}_{N \times N}(\mathbb{R})} \| X_1 - P X_0 \|_F^2 \quad \text{sujeto a} \quad P_{ij} \geq 0 \text{ y } \mathbf{1}^T P = \mathbf{1}^T.

$$

Este enfoque robusto evita soluciones inadmisibles, como probabilidades de transición negativas, que podrían surgir con estimadores de mínimos cuadrados ordinarios no restringidos. La solución a este problema se basa en algoritmos clásicos y eficientes, como el propuesto en la obra seminal de Lawson y Hanson (1995)~[Cita: LawsonHanson1995].

### Formulación Matemática y Solución Constructiva

El objetivo es identificar la matriz de transición \(\hat{\mat{P}}\) que minimiza el error de predicción en un paso. Dadas las matrices de datos \(X_0\) (estados en \(t\)) y \(X_1\) (estados en \(t+1\)), el problema de optimización se plantea como la minimización de la norma de Frobenius del error residual:

$$

\hat{\mat{P}} = \arg\min_{P \in \mathbb{S}(N,N)} \| X_1 - P X_0 \|_F^2

$$

donde \(\mathbb{S}_{N,N}(\mathbb{R}) = \{P \in \mathcal{M}_{N \times N}(\mathbb{R}) : P_{ij} \geq 0,\, \mathbf{1}^T P = \mathbf{1}^T \}\) es el conjunto de matrices estocásticas (por columnas) de tamaño \(N \times N\).

\begin{corollary}[del Teorema 3 en [Cita: BanegasVides2025]]

El problema de estimación planteado en la Ecuación~[Ref: eq:nnls_frobenius] admite una solución constructiva en el espacio de matrices estocásticas.
\end{corollary}

\begin{proof}[Prueba constructiva]

La prueba sigue los pasos algorítmicos para transformar el problema matricial en un sistema lineal resoluble. La ecuación matricial del modelo, \(X_1 = \hat{\mat{P}} X_0\), se linealiza aplicando el operador de **vectorización** (\(\text{vec}(\cdot)\)), que apila las columnas de una matriz en un único vector columna (i.e., \(\text{vec}(A)=[A_{11}, A_{21}, \dots, A_{nn}]^T\)). Utilizando la identidad del producto de Kronecker \(\text{vec}(ABC) = (C^T \otimes A)\text{vec}(B)\), se obtiene:

$$

\text{vec}(X_1) = (X_0^T \otimes I_N) \text{vec}(\hat{\mat{P}})

$$

Este es un sistema lineal de la forma \(b = \mathcal{A}c\), donde \(c = \text{vec}(\hat{\mat{P}})\) es el vector de incógnitas, \(\mathcal{A} = X_0^T \otimes I_N\) es la matriz de diseño, y \(b = \text{vec}(X_1)\) es el vector de observaciones. El problema de minimización se convierte en \(\min_{c} \|\mathcal{A}c - b\|_2^2\), sujeto a las restricciones que definen una matriz estocástica.

Las restricciones se traducen al dominio vectorial:

    * La **no negatividad** (\(\hat{P}_{ij} \geq 0\)) es equivalente a la restricción de desigualdad \(c \ge 0\).
    * La **suma de columnas unitaria** (\(\sum_{i=1}^N \hat{P}_{ij} = 1\)) se formula como un sistema de restricciones de igualdad lineal \(\mathcal{C}c = \mathbf{1}_N\), con \(\mathcal{C} = I_N \otimes \mathbf{1}_N^T\).

Ambas condiciones se integran en un único problema de Mínimos Cuadrados No Negativos (NNLS) a través de un sistema aumentado, cuya solución \(\hat{c}\) se encuentra al resolver:

$$

\hat{c} = \arg\min_{c \geq 0} \left\| \begin{bmatrix} \mathcal{A} \\ w\mathcal{C} \end{bmatrix} c - \begin{bmatrix} b \\ w\mathbf{1}_N \end{bmatrix} \right\|_2^2

$$

El conjunto de factibilidad es convexo y no vacío: la restricción de no negatividad \(c \ge 0\) define la intersección de semiespacios cerrados, mientras que la restricción de igualdad lineal \(\mathcal{C}c = \mathbf{1}_N\) define un hiperplano afín. La intersección de un número finito de semiespacios cerrados con un hiperplano afín es un politopo convexo, que aquí coincide con el símplex de probabilidad por columnas. Dado que, además, el funcional objetivo es cuadrático, la existencia de un minimizador \(\hat{c}\) está garantizada. La solución \(\hat{\mat{P}} = \text{vec}^\dagger(\hat{c})\) es, por construcción, estocástica. Este procedimiento algorítmico, implementado en la función 'SRep', demuestra constructivamente la existencia de la solución [Cita: BanegasVides2025, vides_lopez_srep_2025].
\end{proof}

\begin{remark}[Sobre la unicidad de la solución]
Dado que el funcional objetivo en la Ecuación~[Ref: eq:nnls_frobenius], la norma de Frobenius al cuadrado, es estrictamente convexo, y el conjunto factible \(\mathbb{S}_{N,N}(\mathbb{R})\) también es convexo, el problema de optimización admite un minimizador global único. Por lo tanto, la matriz de transición \(\hat{\mat{P}}\) estimada mediante este procedimiento no solo existe, sino que es única.
\end{remark}

El Algoritmo~[Ref: alg:srep] formaliza el procedimiento completo de
estimación, desde la secuencia de estados hasta la matriz de
transición estocástica.

\begin{algorithm}[H]
\caption{SRep: Estimación de $\hat{\mat{P}}$ vía NNLS aumentado}

\begin{algorithmic}[1]
\Require Secuencia de estados $\{S_t\}_{t=0}^{T}$, $N$ estados, peso $w \gg 1$
\Ensure Matriz de transición estocástica $\hat{\mat{P}} \in \mathcal{M}_{N \times N}(\mathbb{R})$
\State Codificar $\{S_t\}$ en representación one-hot: $x(t) \in \mathbb{R}^N$
\State $X_0 \gets [x(0), x(1), \ldots, x(T-1)]$ \Comment{Estados actuales}
\State $X_1 \gets [x(1), x(2), \ldots, x(T)]$ \Comment{Estados siguientes}
\State $\mathcal{A} \gets X_0^\top \otimes I_N$ \Comment{Matriz de diseño (Kronecker)}
\State $b \gets \mathrm{vec}(X_1)$
\State $\mathcal{C} \gets I_N \otimes \mathbf{1}_N^\top$ \Comment{Restricción de suma unitaria}
\State Construir sistema aumentado:

$$

\tilde{\mathcal{A}} \gets \begin{bmatrix} \mathcal{A} \\ w\,\mathcal{C} \end{bmatrix}, \quad
\tilde{b} \gets \begin{bmatrix} b \\ w\,\mathbf{1}_N \end{bmatrix}

$$

\State $\hat{c} \gets \mathrm{NNLS}(\tilde{\mathcal{A}},\, \tilde{b})$ \Comment{Resolver $\min_{c \ge 0} \|\tilde{\mathcal{A}}\,c - \tilde{b}\|_2^2$}
\State $\hat{\mat{P}} \gets \mathrm{vec}^{-1}(\hat{c})$ \Comment{Reconstruir matriz $N \times N$}
\For{$j = 1$ **hasta** $N$} \Comment{Normalización por columnas}
    \State $\hat{\mat{P}}_{:,j} \gets \hat{\mat{P}}_{:,j} \;/\; \sum_{i=1}^{N} \hat{P}_{ij}$
\EndFor
\State \Return $\hat{\mat{P}}$
\end{algorithmic}
\end{algorithm}

\begin{examplex}[Estimación Validada de la Matriz de Transición con NNLS]

A partir de la secuencia de estados discretos obtenida mediante TCROC (Ejemplo~[Ref: ex:discretizacion_tcroc]), se procede a estimar la matriz de transición \(\hat{\mat{P}}\) utilizando el enfoque de Mínimos Cuadrados No Negativos (NNLS). La secuencia completa de estados, para \(t = 2\) hasta \(t = 23\) (2002--2023), la secuencia \( \{S_t\}_{t=2}^{23} \)
 es:

![Secuencia temporal de estados discretos obtenidos mediante TCROC.]()
*Figura: Secuencia temporal de estados discretos obtenidos mediante TCROC.* (fig:secuencia_estados)

Con \(N = 4\) estados definidos como:

    * \(s_1\): Contracción (\(\alpha_t < -0.01\))
    * \(s_2\): Estabilidad (\(-0.01 \leq \alpha_t \leq 0.02\))
    * \(s_3\): Crecimiento moderado (\(0.02 < \alpha_t \leq 0.04\))
    * \(s_4\): Crecimiento fuerte (\(\alpha_t > 0.04\))

Representados en formato *one-hot*:

$$

s_1 = \begin{bmatrix} 1 \\ 0 \\ 0 \\ 0 \end{bmatrix},\quad
s_2 = \begin{bmatrix} 0 \\ 1 \\ 0 \\ 0 \end{bmatrix},\quad
s_3 = \begin{bmatrix} 0 \\ 0 \\ 1 \\ 0 \end{bmatrix},\quad
s_4 = \begin{bmatrix} 0 \\ 0 \\ 0 \\ 1 \end{bmatrix}

$$

Se construyen las matrices de datos, donde \(\mathcal{M}_{m \times n}(\mathbb{R})\) denota el espacio de las matrices reales de orden \(m \times n\):

    * \(X_0 = [x(2), x(3), \dots, x(22)] \in \mathcal{M}_{4 \times 21}(\mathbb{R})\): estados actuales.
    * \(X_1 = [x(3), x(4), \dots, x(23)] \in \mathcal{M}_{4 \times 21}(\mathbb{R})\): estados siguientes.\\

\paragraph{Construcción de las Matrices de Datos}
A partir de la secuencia de estados y su codificación *one-hot*, se construyen las matrices de datos \(X_0\) y \(X_1\). La matriz \(X_0\) se forma con los vectores de los estados actuales (\(t=2\) a \(t=22\)), y la matriz \(X_1\) con los vectores de los estados siguientes (\(t=3\) a \(t=23\)).

$$

$
X_0 =
\begin{bmatrix}
0 , 0 , 0 , 0 , 0 , 0 , 0 , 1 , 1 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 1 , 1 , 0 \\
1 , 1 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 1 , 1 , 1 , 1 , 1 , 1 , 0 , 0 , 1 , 0 , 0 , 0 \\
0 , 0 , 1 , 1 , 1 , 1 , 1 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 1 , 1 , 0 , 0 , 0 , 0 \\
0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 1
\end{bmatrix}
$}

$$

$$

$
X_1 =
\begin{bmatrix}
0 , 0 , 0 , 0 , 0 , 0 , 1 , 1 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 1 , 1 , 0 , 0 \\
1 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 0 , 1 , 0 , 0 , 0 , 0 \\
0 , 1 , 1 , 1 , 1 , 1 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 1 , 1 , 0 , 0 , 0 , 1 \\
0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 1 , 0 
\end{bmatrix}
$}

$$

Estas dos matrices son la base para el conteo de transiciones y la posterior estimación con NNLS. Para estimar \(\hat{\mat{P}}\), se cuenta el número de transiciones entre estados. Sea \(C \in \mathbb{Z}^{4 \times 4}\) la matriz de conteo, donde \(C_{ij}\) es el número de transiciones de \(s_j\) a \(s_i\). Esta matriz se puede calcular eficientemente a partir de las matrices de datos \(X_1\) y \(X_0\) mediante la siguiente operación:

$$

    C = X_1 X_0^T
    

$$

Recorriendo los pares \((S_t, S_{t+1})\) de la secuencia, se obtiene:

$$

C = 
\begin{bmatrix}
2 & 1 & 1 & 0 \\
1 & 6 & 1 & 0 \\
0 & 2 & 5 & 1 \\
1 & 0 & 0 & 0 \\
\end{bmatrix}

$$

La matriz de transición se obtiene normalizando esta matriz de conteo por columnas:

$$

    \hat{P}_{ij} = \frac{C_{ij}}{\sum_{k=1}^4 C_{kj}}, \quad \forall j

$$

$$

\hat{\mat{P}}=
\begin{bmatrix}
0.5000 & 0.1111 & 0.1429 & 0.0000 \\
0.2500 & 0.6667 & 0.1429 & 0.0000 \\
0.0000 & 0.2222 & 0.7143 & 1.0000 \\
0.2500 & 0.0000 & 0.0000 & 0.0000
\end{bmatrix}

$$

Para validar empíricamente la correcta implementación del algoritmo de estimación propuesto en~[Cita: vides_lopez_srep_2025], se realiza un experimento numérico que consiste en dos fases. En la primera fase, se utiliza la matriz  \(\hat{\mat{P}}\) como "verdad fundamental" para simular una nueva secuencia de estados. En la segunda fase, se aplica sobre estos datos simulados el algoritmo de Mínimos Cuadrados No Negativos (NNLS) implementado en la función 'SRep', cuya metodología se detalla en el Listado~[Ref: lst:srep_implementacion]. El objetivo es verificar si el estimador recupera la matriz original.

La implementación completa del algoritmo 'SRep' se
presenta en el Apéndice~[Ref: apx:codigo_srep]
(Listado~[Ref: lst:srep_implementacion]).

El resultado de la validación se evalúa calculando la matriz de diferencia entre la matriz estimada y la original (\(Ap -\hat{\mat{P}}\)). Tal como se muestra a continuación, el resultado es una matriz donde todos los elementos son numéricamente cero, con errores del orden de \(10^{-13}\) atribuibles únicamente a la precisión de punto flotante:

$$

Ap -\hat{\mat{P}} \approx
\begin{bmatrix}
 0 & 0 & 0 & 0 \\
 0 & 0 & 0 & 0 \\
 0 & 0 & 0 & 0 \\
 0 & 0 & 0 & 0
\end{bmatrix}

$$

Este resultado valida empíricamente que la función 'SRep' implementa correctamente el estimador propuesto, logrando recuperar la matriz de transición original con alta precisión.

![Grafo de transición estimado para los regímenes del PIB per cápita en Lempiras.]()
*Figura: Grafo de transición estimado para los regímenes del PIB per cápita en Lempiras.* (fig:grafo_transicion)

\paragraph{Interpretación de la Matriz de Transición}

La matriz \(\hat{\mat{P}}\) revela la dinámica subyacente del sistema:

    * **Estado \(s_1\) (Contracción):** Persiste con una probabilidad del 50\%, mientras que se recupera transitando a \(s_2\) (estabilidad) o a \(s_4\) (crecimiento fuerte) con una probabilidad del 25\% en cada caso.
    * **Estado \(s_2\) (Estabilidad):** Es el régimen más persistente, con una probabilidad del 66.7\% de mantenerse. Su principal vía de salida es hacia \(s_3\) (crecimiento moderado), con un 22.2\%.
    * **Estado \(s_3\) (Crecimiento moderado):** También es altamente estable, persistiendo con un 71.4\%. La probabilidad de retroceso a otros estados es baja.
    * **Estado \(s_4\) (Crecimiento fuerte):** Actúa como un estado puramente transitorio. En la muestra observada, siempre es seguido por una transición al estado \(s_3\), lo que sugiere que los picos de crecimiento tienden a moderarse rápidamente.

\paragraph{Propiedades Teóricas del Estimador}
La matriz \(\hat{\mat{P}}\), calculada a partir de las frecuencias relativas de transición, corresponde al **Estimador de Máxima Verosimilitud (MLE)** para una cadena de Markov de primer orden. El supuesto distribucional merece hacerse explícito: el MLE modela las transiciones salientes de cada estado de origen como ensayos independientes bajo una distribución multinomial condicional, sin imponer ninguna distribución exógena sobre la estructura de los coeficientes de la matriz \(P\). Bajo ese supuesto, maximizar la verosimilitud de la cadena equivale a normalizar por columnas los conteos de transición, y el estimador resultante posee propiedades asintóticas bien establecidas.

\begin{theorem}[Consistencia del Estimador]

Dada una cadena de Markov ergódica con una matriz de transición verdadera (e inobservable) $P$, el estimador de máxima verosimilitud $\hat{\mat{P}}$, obtenido a partir de una secuencia de $T$ observaciones, es consistente. Esto implica que el estimador converge en probabilidad a la matriz verdadera a medida que el número de observaciones tiende a infinito:
$$
\hat{\mat{P}} \xrightarrow{p} P \quad \text{cuando} \quad T \to \infty
$$
\end{theorem}
\begin{proof}
Por la ley fuerte de los grandes números para cadenas de Markov
ergódicas [Cita: Norris1998], las frecuencias relativas de
transición convergen casi seguramente:

$$

\hat{p}_{ij} = \frac{N_{ij}}{N_{i\cdot}}
\xrightarrow{\text{c.s.}} p_{ij},
\quad T \to \infty,

$$

donde $N_{ij} = \#\{t : S_t = i,\, S_{t+1} = j\}$ y
$N_{i\cdot} = \sum_j N_{ij}$.  Dado que la cadena es ergódica,
cada estado se visita infinitas veces, por lo que $N_{i\cdot}
\to \infty$ para todo $i$.  La convergencia
entrada-por-entrada implica $\|\hat{\mat{P}} - P\|_{\max}
\xrightarrow{\text{c.s.}} 0$.  Este resultado fue establecido
formalmente por Anderson y Goodman~[Cita: AndersonGoodman1957].
El estimador NNLS coincide con el MLE bajo las restricciones de
no-negatividad, heredando así la consistencia.
\end{proof}

 La inclusión de este resultado es necesaria, pues certifica que, con una cantidad suficiente de datos, nuestra metodología se aproxima al verdadero proceso generador de la dinámica de regímenes. La consistencia descansa sobre una hipótesis de ergodicidad cuyo alcance hay que delimitar: la cadena debe ser ergódica, es decir, estacionaria en sentido asintótico, irreducible y aperiódica, de modo que cada estado se visite de forma recurrente. El operador local de tasas \(\alpha_t\) contribuye a satisfacer ese requisito, ya que estabiliza la varianza e induce estacionariedad débil en la serie de estados \(\{S_t\}\); esta condición se verifica empíricamente en el Capítulo~[Ref: cap:regimenes_combustibles] mediante las pruebas ADF y KPSS. A diferencia del algoritmo de Esperanza-Maximización (EM) con el que se estiman los modelos de cambio de régimen tipo MS-AR, cuya función de verosimilitud no convexa solo garantiza óptimos locales dependientes de la inicialización, el estimador empleado aquí se reduce a un problema convexo con solución única; esta comparación se retoma con datos reales en el Capítulo~[Ref: cap:regimenes_combustibles].

\paragraph{Simulación de la Evolución de la Cadena de Markov}
Para validar la capacidad del estimador SRep de recuperar la
matriz de transición original, se simula la evolución de una
cadena de Markov utilizando la matriz $\hat{\mat{P}}$ estimada
previamente. Se define un estado inicial correspondiente a $s_1$
y se genera una secuencia de ocupaciones probabilísticas para
$T = 20$ pasos (véase Apéndice~[Ref: apx:codigo_srep],
Listado~[Ref: lst:simulacion_markov]).

La Figura~[Ref: fig:evolucion_probabilidades_markov] muestra la evolución temporal de las probabilidades de ocupación para cada uno de los estados del sistema Markoviano, bajo la matriz de transición estimada \(\hat{\mat{P}}\), a lo largo de 20 pasos de simulación.

![Evolución temporal de las probabilidades de ocupación de los estados \(s_1, s_2, s_3, s_4\) bajo la dinámica inducida por la matriz de transición estimada \(\hat{\mat{P](figures/evolucion_probabilidades_markov.png)
*Figura: Evolución temporal de las probabilidades de ocupación de los estados \(s_1, s_2, s_3, s_4\) bajo la dinámica inducida por la matriz de transición estimada \(\hat{\mat{P* (fig:evolucion_probabilidades_markov)

    * **Estado \(s_1\) (contracción):** Inicia con probabilidad unitaria (\(1.0\)) y experimenta un descenso rápido durante los primeros 5 pasos. A partir del paso 6, la probabilidad se estabiliza alrededor de \(0.195\), oscilando mínimamente entre \(0.1957\) y \(0.1951\) hasta el final de la simulación.
    * **Estado \(s_2\) (estabilidad):** Comienza en cero y crece de forma sostenida. Se estabiliza aproximadamente en el paso 10, alrededor de \(0.329\), mostrando una variación insignificante (\(<0.001\)) desde ese punto en adelante.
    * **Estado \(s_3\) (crecimiento moderado):** Aunque inicia en cero, incrementa rápidamente y supera a los demás estados a partir del paso 2. A partir del paso 7, se mantiene en torno a \(0.427\), con oscilaciones menores a \(0.001\), confirmando su papel como estado dominante en el régimen estacionario.
    * **Estado \(s_4\) (crecimiento fuerte):** Presenta un leve incremento inicial pero desciende pronto, estabilizándose en torno a \(0.0488\) a partir del paso 6. Su baja probabilidad persistente evidencia su carácter transitorio.

**Estabilización global:**
En general, la cadena de Markov alcanza el equilibrio estacionario a partir de los pasos 10 a 12, cuando las probabilidades de todos los estados dejan de variar de manera significativa y convergen a valores constantes: \(s_1 \approx 0.195\), \(s_2 \approx 0.329\), \(s_3 \approx 0.427\) y \(s_4 \approx 0.049\). Este comportamiento confirma que el sistema converge rápidamente a un régimen estable, dominado por los estados de crecimiento moderado y estabilidad.\\

En conjunto, la dinámica observada en la gráfica es plenamente consistente con la teoría de cadenas de Markov: los estados de crecimiento moderado y estabilidad emergen como los destinos predominantes a largo plazo, mientras que los estados extremos (contracción y crecimiento fuerte) son rápidamente abandonados y sólo se presentan de manera transitoria. \\

Esta convergencia hacia regímenes centrales y estables refleja tanto la estructura de la matriz de transición estimada \(\hat{\mat{P}}\) como la naturaleza realista de procesos económicos o financieros modelados por este tipo de sistemas. El comportamiento del sistema valida, así, que la modelación mediante cadenas de Markov captura adecuadamente las dinámicas de persistencia y transición entre regímenes típicas en el análisis de series temporales económicas.

\end{examplex}
### Validación del Rendimiento Predictivo
Para cuantificar la capacidad predictiva del modelo, se evaluó su rendimiento utilizando múltiples particiones de entrenamiento y prueba. Se entrenó la matriz de transición \(\hat{\mat{P}}\) con porciones crecientes de la serie temporal (desde el 60\% hasta el 95\%) y se validó su capacidad para predecir el estado del siguiente período en los datos restantes. El código completo para este análisis de robustez se encuentra en el Apéndice~[Ref: apx:codigo_validacion_completo].

La Tabla~[Ref: tab:rendimiento_predictivo_resumen] resume la precisión (accuracy) del modelo para cada partición.

*Tabla: Resumen del rendimiento predictivo según la partición de datos.*

Los resultados revelan un hallazgo clave: la precisión predictiva es mayor cuando el conjunto de prueba es más grande y disminuye a cero a medida que este se reduce. Este comportamiento, aparentemente contraintuitivo, no indica un deterioro del modelo. Por el contrario, se explica porque los conjuntos de prueba más pequeños aíslan las transiciones más recientes y anómalas de la serie, particularmente el evento único del estado $s_4$ (crecimiento fuerte) en 2022. Al no tener antecedentes en los datos de entrenamiento, el modelo no puede predecirlo, lo que ilustra una limitación clásica de los modelos de Markov basados en datos históricos: capturan bien dinámicas recurrentes, pero su capacidad ante eventos únicos queda limitada por la escasez de datos. No se trata, por tanto, de una incapacidad ni de una debilidad estructural del modelo, sino de un límite inherente a cualquier estimador construido sobre frecuencias históricas: ningún esquema de Markov puede anticipar la primera ocurrencia de un shock exógeno que carece de precedentes en la muestra. Una vez que ese shock impacta la serie y traslada el sistema al estado correspondiente, el modelo recalibra de inmediato la condición de entrada y estima las probabilidades de transición del período siguiente a partir del nuevo estado de origen; su utilidad, por tanto, no se restringe a los tramos de estabilidad, sino que se readapta con rapidez una vez ocurrida la perturbación.

\begin{examplex}[Definición y Discusión de Estados para Series de Combustibles]

Para la discretización de las series de precios de combustibles, se definieron cuatro estados cuantitativos en función del parámetro \(\alpha\), que mide la tasa relativa de cambio en cada periodo. Estos estados se codifican como \(s1, s2, s3, s4\), que se interpretan respectivamente como *caída*, *estable*, *subida* y *alza fuerte*.\\

La función de asignación utilizada fue la siguiente:

$$

\text{estado} =
\begin{cases}
s1 & \text{si } \alpha < -0.01 \\
s2 & \text{si } -0.01 \leq \alpha \leq 0.02 \\
s3 & \text{si } 0.02 < \alpha \leq 0.04 \\
s4 & \text{si } \alpha > 0.04
\end{cases}

$$

Esta definición, aunque arbitraria, responde a un criterio homogéneo aplicado a todas las series para facilitar la comparación directa entre ellas. 

\bigskip
**Pros y Contras del enfoque:**

    * **Límites fijos para todas las series:**
    
        * *Pros:* Permite un marco común para análisis comparativos y es sencillo de explicar y mantener.
        * *Contras:* Puede no captar particularidades dinámicas específicas de cada serie; por ejemplo, una serie con baja variabilidad puede quedar demasiado concentrada en un solo estado.
    
    * **Límites dinámicos o personalizados para cada serie:**
    
        * *Pros:* Permite mayor sensibilidad a las fluctuaciones internas y mejor interpretación dentro de cada serie.
        * *Contras:* Dificulta la comparación entre series al no tener un sistema uniforme de estados.
    

\bigskip
En este trabajo se adoptan límites fijos para todas las series con el objetivo de facilitar los análisis cualitativos y comparativos. Conviene subrayar que esta partición no queda como un criterio subjetivo y definitivo del investigador: en el capítulo empírico (Capítulo~[Ref: cap:regimenes_combustibles]) estos umbrales fijos se sustituyen por una partición endógena estimada con el algoritmo de K-medias, que delimita los estados de forma automática a partir de la distribución observada de cada serie. Los límites fijos cumplen aquí un papel comparativo y transitorio; la asignación final de regímenes en los datos reales es de carácter endógeno.

\bigskip
A continuación, se presentan las matrices de conteo \( C \), matrices de transición \( \hat{\mat{P}} \), matrices estimadas \( A_p \) mediante la función \( 'SRep' \), y la diferencia entre estas matrices para las cuatro series de precios de combustibles: **Super, Regular, Diesel** y **Kerosene**.

\bigskip
**Matrices de Conteo \(C\) (calculadas según la Ecuación~[Ref: eq:conteo_def**):]

$$

C_{\text{Super}} = \begin{bmatrix}
41 & 18 & 1 & 0 \\
19 & 327 & 6 & 0 \\
0 & 7 & 13 & 0 \\
0 & 0 & 0 & 0
\end{bmatrix}
\quad
C_{\text{Regular}} = \begin{bmatrix}
43 & 13 & 1 & 0 \\
14 & 334 & 6 & 0 \\
0 & 7 & 14 & 0 \\
0 & 0 & 0 & 0
\end{bmatrix}

$$

$$

C_{\text{Diesel}} = \begin{bmatrix}
62 & 19 & 1 & 0 \\
20 & 304 & 5 & 0 \\
0 & 6 & 11 & 2 \\
0 & 1 & 1 & 0
\end{bmatrix}
\quad
C_{\text{Kerosene}} = \begin{bmatrix}
81 & 23 & 0 & 0 \\
23 & 238 & 13 & 1 \\
0 & 13 & 25 & 4 \\
0 & 1 & 4 & 6
\end{bmatrix}

$$

Se observa que en las matrices de conteo para **Super** y **Regular**, tanto la cuarta fila como la cuarta columna son vectores nulos. Esto indica que el estado \(s_4\) (alza fuerte) es un estado transitorio no observado en estas series de datos; nunca se transita desde ni hacia él. Para un análisis más robusto y enfocado en la dinámica real, se puede reducir el espacio de estados de 4x4 a 3x3, considerando únicamente los estados activos (\(s_1, s_2, s_3\)). Las submatrices de conteo resultantes son:

$$

C_{\text{Super}}^{\text{reducida}} = \begin{bmatrix}
41 & 18 & 1 \\
19 & 327 & 6 \\
0 & 7 & 13
\end{bmatrix}
\quad
C_{\text{Regular}}^{\text{reducida}} = \begin{bmatrix}
43 & 13 & 1 \\
14 & 334 & 6 \\
0 & 7 & 14
\end{bmatrix}

$$

\bigskip
**Matrices de Transición ($\hat{\mat{P**}$) (Normalizadas por columna, 3 decimales):}

$$

\hat{P}_{\text{Super}} = \begin{bmatrix}
0.683, 0.051, 0.050\\
0.317, 0.929, 0.300 \\
0.000, 0.020, 0.650
\end{bmatrix}
\quad
\hat{P}_{\text{Regular}} = \begin{bmatrix}
0.754, 0.037, 0.048 \\
0.246, 0.944, 0.286 \\
0.000, 0.020, 0.667
\end{bmatrix}

$$

$$

\hat{P}_{\text{Diesel}} = \begin{bmatrix}
0.756, 0.058, 0.056, 0.000 \\
0.244, 0.921, 0.278, 0.000 \\
0.000, 0.018, 0.611, 1.000 \\
0.000, 0.003, 0.056, 0.000
\end{bmatrix}
\quad
\hat{P}_{\text{Kerosene}} = \begin{bmatrix}
0.779, 0.084, 0.000, 0.000 \\
0.221, 0.865, 0.310, 0.091 \\
0.000, 0.047, 0.595, 0.364 \\
0.000, 0.004, 0.095, 0.545
\end{bmatrix}

$$

\bigskip
**Matrices \(A_p\) Estimadas con \( 'SRep** \) (3 decimales):'

$$

A_{p, \text{Super}} = \begin{bmatrix}
0.683, 0.051, 0.050 \\
0.317, 0.929, 0.300 \\
0.000, 0.020, 0.650
\end{bmatrix}

$$

$$

A_{p, \text{Regular}} = \begin{bmatrix}
0.754, 0.037, 0.048 \\
0.246, 0.944, 0.286 \\
0.000, 0.020, 0.667
\end{bmatrix}

$$

$$

A_{p, \text{Diesel}} = \begin{bmatrix}
0.756, 0.058, 0.056, 0.000 \\
0.244, 0.921, 0.278, 0.000 \\
0.000, 0.018, 0.611, 1.000 \\
0.000, 0.003, 0.056, 0.000
\end{bmatrix}

$$

$$

A_{p, \text{Kerosene}} = \begin{bmatrix}
0.779, 0.084, 0.000, 0.000 \\
0.221, 0.865, 0.310, 0.091 \\
0.000, 0.047, 0.595, 0.364 \\
0.000, 0.004, 0.095, 0.545
\end{bmatrix}

$$

\bigskip
**Diferencia \(A_p - \hat{\mat{P**}\) :}

$$

\Delta_{\text{Super}} = \begin{bmatrix}
0, 0, 0 \\
0, 0, 0 \\
0, 0, 0
\end{bmatrix}
\quad
\Delta_{\text{Regular}} = \begin{bmatrix}
0, 0, 0 \\
0, 0, 0 \\
0, 0, 0 
\end{bmatrix}

$$

$$

\Delta_{\text{Diesel}} = \begin{bmatrix}
0, 0, 0, 0 \\
0, 0, 0, 0 \\
0, 0, 0, 0 \\
0, 0, 0, 0
\end{bmatrix}
\quad
\Delta_{\text{Kerosene}} = \begin{bmatrix}
0, 0, 0, 0 \\
0, 0, 0, 0 \\
0, 0, 0, 0 \\
0, 0, 0, 0
\end{bmatrix}

$$

\end{examplex}

\begin{remark}[Interpretación de diferencias en la matriz de transición estimada]
Anteriormente, para las series *Super* y *Regular*, al trabajar con matrices de transición de tamaño \(4 \times 4\), se observaba un valor exacto de 1 en la última columna de las matrices de diferencia \(\Delta = A_p - \hat{\mat{P}}\). \\

Este valor reflejaba la ausencia de transiciones observadas saliendo del cuarto estado, que en esos casos no estaba presente en la muestra. La matriz \(\hat{\mat{P}}\), basada en conteos observados, tenía columnas nulas para ese estado, mientras que la matriz estimada \(A_p\), calculada con la función 'SRep', asignaba probabilidades que sumaban 1 para cumplir con las propiedades de matriz de transición de Markov.\\

Sin embargo, al reducir las matrices a tamaño \(3 \times 3\), excluyendo el cuarto estado no utilizado en estas series, dicho valor 1 desaparece de las diferencias, ya que el análisis y estimación se realizan únicamente sobre los estados realmente presentes.\\

Esta reducción mejora la interpretación y asegura que las matrices reflejen fielmente la dinámica observable de las series, eliminando artefactos generados por estados inexistentes o absorbentes en los datos.
\end{remark}

![Evolución de las probabilidades de ocupación para cada estado según la matriz de transición estimada \(\hat{\mat{P](figures/allseriecombustiblesNNLS.png)
*Figura: Evolución de las probabilidades de ocupación para cada estado según la matriz de transición estimada \(\hat{\mat{P* (fig:evolucion_probabilidades_combustibles)

La figura~[Ref: fig:evolucion_probabilidades_combustibles] muestra la evolución temporal de las probabilidades de pertenencia a cada estado de la matriz \(\hat{\mat{P}}\) estimada para las series de combustibles: Super, Regular, Diesel y Kerosene. Se observa que todas las series alcanzan rápidamente un estado estable de probabilidades, con predominancia del estado "estable" (línea azul), que supera el 80\% en todos los casos. Las probabilidades de caída, subida y alza fuerte se mantienen bajas y constantes después de pocos pasos, lo que indica una dinámica de transición marcada por una fuerte estabilidad en el corto plazo. Este comportamiento es consistente con mercados regulados o poco volátiles, donde los cambios abruptos son poco frecuentes.

![Comparación entre la matriz de transición normalizada \(\hat{\mat{P]()
*Figura: Comparación entre la matriz de transición normalizada \(\hat{\mat{P* (fig:grafo_comparativo_super_reducido)

![Comparación entre la matriz de transición normalizada \(\hat{\mat{P]()
*Figura: Comparación entre la matriz de transición normalizada \(\hat{\mat{P* (fig:grafo_comparativo_regular_reducido)

![Comparación entre la matriz de transición normalizada \(\hat{\mat{P]()
*Figura: Comparación entre la matriz de transición normalizada \(\hat{\mat{P* (fig:grafo_comparativo_diesel)

![Comparación entre la matriz de transición normalizada \(\hat{\mat{P]()
*Figura: Comparación entre la matriz de transición normalizada \(\hat{\mat{P* (fig:grafo_comparativo_kerosene)

Al comparar las dinámicas de transición de las cuatro series, se observa una clara jerarquía en su complejidad. 
Los modelos para **Super** y **Regular** exhiben las dinámicas más simples, caracterizadas por una fuerte persistencia en los estados y rutas de transición limitadas, 
con solo tres estados relevantes (\(s_1, s_2, s_3\)) debido a la ausencia del estado \(s_4\) en los datos. 
El modelo para **Diesel** presenta una conectividad moderada, con algunas interacciones adicionales entre los regímenes. 
La serie **Kerosene** revela la dinámica más diversa y compleja; en su grafo, todos los estados son persistentes y están altamente interconectados con múltiples vías de entrada y salida, 
lo que sugiere un sistema económico menos predecible y con mayor capacidad para alternar entre diferentes regímenes.

### Análisis Predictivo y Robustez del Modelo para Combustibles
Para evaluar el rendimiento predictivo del modelo en las series de combustibles, se realizó un análisis exhaustivo con múltiples particiones de entrenamiento/prueba (desde 60/40 hasta 95/5).

Los resultados, resumidos en la Tabla~[Ref: tab:rendimiento_combustibles], demuestran una alta capacidad predictiva y revelan una clara jerarquía en la dinámica de las series.

*Tabla: Resumen del rendimiento predictivo máximo para cada serie de combustible.* (tab:rendimiento_combustibles)

| **Serie de Combustible** | **Precisión (Accuracy) Máxima** |
| --- | --- |
| Regular | 94.2\% |
| Super | 93.0\% |
| Diesel | 86.1\% |
| Kerosene | 86.0\% |

Las gasolinas **Super** y **Regular** resultaron ser altamente predecibles, alcanzando precisiones superiores al 90\%, lo que refleja su dinámica de precios estable. Por el contrario, el **Kerosene** y el **Diesel** mostraron ser más complejos y volátiles, con precisiones máximas en torno al 86\%.

Es notable que, aunque el estimador **Ap (SRep)** difiere teóricamente del estimador **$\hat{\mat{P**}$ (MLE)} para las series Super y Regular, su rendimiento predictivo fue idéntico en todas las pruebas. Esto se debe a que el estado anómalo ($s_4$) que causa la diferencia entre las matrices no apareció en los conjuntos de prueba, por lo que la distinción no tuvo un impacto práctico. Esta coincidencia no es casual: para una serie univariada larga, con estados bien poblados y sin penalizaciones añadidas, el estimador NNLS y el MLE de frecuencias relativas por columna coinciden, como sucede aquí. Su diferencia teórica solo se vuelve apreciable bajo regularización adicional o con muestras extremadamente cortas, casos en los que la restricción de no negatividad evita probabilidades espurias y atenúa el sobreajuste.

A diferencia de la serie corta del PIB, en todas las series de combustibles se observó el comportamiento esperado: a mayor cantidad de datos de entrenamiento, la precisión del modelo consistentemente mejoró. Esto valida la robustez del modelo y su capacidad para aprender de series de tiempo más largas y estadísticamente estables.

## Resumen del Algoritmo Metodológico

Para consolidar la metodología expuesta en este capítulo, el Algoritmo~[Ref: alg:estimacion_validacion_completa] presenta el flujo de trabajo computacional de forma unificada. Este resume, paso a paso, el proceso completo: desde la discretización de la serie temporal original con TCROC hasta la estimación y validación de las matrices de transición, sirviendo como una guía clara para la replicabilidad del método.

\begin{algorithm}[H]
\caption{Algoritmo para la Estimación y Validación de la Matriz de Transición}

\begin{algorithmic}[1]
\Require Serie temporal original $\{v(t)\}_{t=0}^T$, parámetros de TCROC \(\lambda, W\), y umbrales de estado.
\Ensure Matriz de conteo \(C\), Matriz de transición estimada \(\hat{\mat{P}}\), Matriz estimada \(A_p\), y Matriz de diferencia \(\Delta\).

\State **Fase 1: Discretización de la Serie Temporal**
\State Inicializar una lista vacía 'alphas'.
\For{\(t\) desde \(W\) hasta \(T\)}
    \State Calcular \(\alpha_t\) usando la Ecuación~\eqref{eq:tcra_markov_alpha}.
    \State Añadir \(\alpha_t\) a la lista 'alphas'.
\EndFor
\State Inicializar una lista vacía 'estados'.
\For{cada \(\alpha\) en 'alphas'}
    \State Asignar un estado numérico \(S_t \in \{1, 2, 3, 4\}\) según los umbrales definidos.
    \State Añadir \(S_t\) a la lista 'estados'.
\EndFor

\State **Fase 2: Construcción de Matrices de Datos**
\State Construir la matriz de estados actuales \(X_0 \in \mathcal{M}_{4 \times (T-W)}(\mathbb{R})\) a partir de 'estados'[:-1] en formato *one-hot*.
\State Construir la matriz de estados siguientes \(X_1 \in \mathcal{M}_{4 \times (T-W)}(\mathbb{R})\) a partir de 'estados'[1:] en formato *one-hot*.

\State **Fase 3: Estimación de la Matriz de Transición (MLE)**
\State Calcular la matriz de conteo de transiciones: \(C \leftarrow X_1 X_0^T\).
\State Calcular la matriz de transición de máxima verosimilitud \(\hat{\mat{P}}\) normalizando las columnas de \(C\):

$$
 \hat{P}_{ij} \leftarrow \frac{C_{ij}}{\sum_{k=1}^4 C_{kj}} 
$$

\State **Fase 4: Validación del Estimador SRep**
\State Simular una secuencia de estados \(p_0 \in \mathcal{M}_{4 \times M}(\mathbb{R})\) de longitud \(M\) usando la matriz \(\hat{\mat{P}}\) como generador.
\State Estimar la matriz de transición \(A_p\) aplicando la función 'SRep' sobre los datos simulados \(p_0\):

$$
 A_p \leftarrow \text{SRep}(p_0, \text{ss}) 
$$

\State **Fase 5: Comparación y Análisis**
\State Calcular la matriz de diferencia: \(\Delta \leftarrow A_p - \hat{\mat{P}}\).

\State \Return \(C, \hat{\mat{P}}, A_p, \Delta\).
\end{algorithmic}
\end{algorithm}

## Discusión de Limitaciones y Extensiones Futuras

Si bien la metodología ha demostrado ser robusta, conviene reconocer sus supuestos para contextualizar los resultados y proponer futuras líneas de investigación.

\paragraph{Limitaciones del Modelo Actual.}
Las principales limitaciones del enfoque se centran en dos supuestos clave. Por una parte, los regímenes se definen a través de **umbrales fijos** en la tasa \(\alpha_t\), los cuales son determinados exógenamente. Por otra parte, el modelo asume una **propiedad de Markov de primer orden**, donde el futuro solo depende del estado presente, ignorando posibles dependencias de más largo plazo que podrían existir en los datos.

\paragraph{Extensiones Futuras.}
A partir de estas limitaciones, se proponen las siguientes extensiones para trabajos futuros:

    * **Umbrales Dinámicos o Adaptativos:** En lugar de umbrales fijos, se podrían explorar métodos de aprendizaje no supervisado (como clustering) para determinar los puntos de corte de los regímenes de forma endógena desde los datos.
    * **Modelos de Markov de Orden Superior:** Investigar si un modelo de segundo orden (donde \(P(S_t | S_{t-1}, S_{t-2})\)) mejora la precisión predictiva, especialmente en series con dinámicas más complejas.
    * **Extensión a Reservoir Computing No Lineal:** Generalizar el modelo actual hacia un *Stochastically Structured Reservoir Computer* (SSRC), donde la matriz \(\hat{\mat{P}}\) es un caso particular de una matriz de acoplamiento \(W\), e incorporar *embeddings* no lineales \(\tilde{\sigma}_p\) para capturar dinámicas más complejas, como se sugiere en [Cita: BanegasVides2025].

## Resumen del Capítulo

Este capítulo ha presentado una metodología unificada y robusta, el modelo TCROC-Markov con estimación NNLS, para la discretización y el modelado de regímenes en series de tiempo económicas. El enfoque aborda dos desafíos clave en la literatura: genera estados económicos interpretables a partir de datos continuos y garantiza, por construcción, la validez estocástica de las matrices de transición estimadas. Esta estrategia híbrida, que combina componentes estadísticos con aprendizaje estructurado, se alinea con los hallazgos de la competencia M4 [Cita: Makridakis2018] y con resultados de pronóstico bajo cambio de régimen [Cita: GuidolinTimmermann2009].\\

A través de la aplicación a datos del PIB per cápita de Honduras y precios de combustibles, se demostró la efectividad del método. En el caso del PIB, el modelo identificó correctamente períodos de estabilidad, crecimiento y contracciones agudas asociadas a crisis conocidas. Para las series de combustibles, el análisis reveló una clara jerarquía de predictibilidad, con las gasolinas mostrando una alta estabilidad y el kerosene una dinámica más compleja.\\

El análisis de validación predictiva reveló una dualidad en la aplicabilidad del modelo, ligada a las propiedades de la serie. Para series largas y estadísticamente estables como los combustibles, la metodología demostró alta precisión en el pronóstico a corto plazo. En contraste, para series cortas con eventos anómalos como la del PIB, el poder predictivo se reduce por la escasez de datos. Aun así, el modelo estimado capturó una dinámica estructural coherente, como lo confirma su rápida convergencia hacia una distribución estacionaria. La utilidad del modelo es doble: es una herramienta tanto para el **análisis estructural** de sistemas complejos como para la **predicción** de dinámicas recurrentes.

# Detección y Pronóstico de Regímenes de Precios en Combustibles

## Introducción

La volatilidad en los precios de los combustibles\footnote{El contenido
central de este capítulo ha sido aceptado para publicación y presentación
en IEEE CONCAPAN XLII (2025) bajo el título *"TCROC--Markov: A Two-Stage
Framework for Economic Regime Analysis"*.} dificulta la planificación
económica en economías pequeñas y abiertas como la hondureña. Los enfoques
clásicos de cambio de régimen (MS-AR) [Cita: Hamilton1994,KimNelson1999,AngBekaert2002]
ofrecen un marco teórico sólido, pero dependen de supuestos paramétricos
fuertes y de una optimización de verosimilitud que puede resultar inestable
con alta volatilidad y muestras moderadas.

Por otra parte, los Modelos Ocultos de Markov (HMM) ofrecen mayor flexibilidad 
al no requerir una forma paramétrica para las emisiones, pero sus estados latentes 
no poseen una interpretación económica directa por construcción: la correspondencia 
entre el estado "oculto" identificado y un régimen de mercado observable suele ser 
ambigua y requiere una validación posterior o análisis detallado de sus funciones de 
emisión, lo que dificulta su aplicación directa al diseño de políticas públicas.

Los Capítulos~[Ref: cap:tcra]--[Ref: cap:tcroc_hibrido_integrado]
construyeron, de forma progresiva, las herramientas teóricas que
conforman el marco TCRA-Markov: la jerarquía de operadores y sus
propiedades matemáticas, la integración con cadenas de Markov
($\phi$-mixing, convergencia asintótica), y la estimación NNLS con
prueba constructiva de existencia.

Este capítulo aplica ese aparato teórico a datos reales.  Se
investiga la sensibilidad del marco a los hiperparámetros
$(W, \lambda, K)$, lo que incluye el descubrimiento de un
fenómeno de degeneración para ventanas excesivamente grandes, y
se identifican regímenes económicos interpretables mediante
agrupamiento K-medias~[Cita: MacQueen1967] sobre la señal
$\alpha_t$, evaluando como hipótesis su desempeño frente a alternativas
tradicionales como la discretización por cuantiles fijos.  El capítulo incluye también un
análisis espectral de las matrices de transición estimadas que
conecta las propiedades algebraicas de $\hat{\mat{P}}$ con la
dinámica económica observada.

El estudio se aplica a series semanales de precios de cuatro
combustibles en Honduras (Superior, Regular, Diésel y Kerosene) durante
el período enero 2017, agosto 2025, con más de 400 observaciones
por serie.  Las trayectorias completas $\{S_t\}$, los diagramas de
transición y las matrices $\hat{\mat{P}}$ estimadas para cada serie se
presentan en el Apéndice~[Ref: apx:figuras_proceso_st]
(Figuras~[Ref: fig:apx_trayectorias_super], [Ref: fig:apx_diagrama_super],
[Ref: fig:apx_trayectorias_regular] y~[Ref: fig:apx_diagrama_regular];
Tablas~[Ref: tab:apx_P_super] y~[Ref: tab:apx_P_regular]).

La validación predictiva se implementó mediante un esquema de ventanas 
deslizantes (*rolling window*) con las siguientes especificaciones: 
el conjunto de entrenamiento inicial abarca las primeras 312 semanas 
(enero 2017, diciembre 2022), lo que representa un período de 6 años de historia 
económica local que permite capturar diversos comportamientos del mercado y proporciona 
una base de estimación estadísticamente representativa, a la vez que deja un período de 
evaluación fuera de muestra extenso (aproximadamente 2.6 años), el horizonte de 
pronóstico es de 1 semana, y la ventana avanza con paso unitario sobre el período fuera de muestra 
(enero 2023, agosto 2025), generando aproximadamente 130 evaluaciones 
secuenciales por serie. Este diseño preserva estrictamente la causalidad temporal: 
en ningún punto del proceso se utiliza información futura para estimar los parámetros del modelo.

## Marco Metodológico Aplicado

El flujo de trabajo implementado sigue la arquitectura de dos etapas
del marco TCRA-Markov desarrollado en los capítulos precedentes.
Esta sección resume las decisiones de implementación específicas para
la aplicación a datos de combustibles, remitiendo al lector a las
demostraciones formales ya establecidas.

### Etapa 1: Discretización Basada en Datos

#### El Operador TCRA como Extractor de Características

Para cada instante $t$, se calcula la tasa de cambio relativa
$\alpha_t$ utilizando la variante ETCRAM, que combina ventana
móvil $W$ y decaimiento exponencial $\lambda$:

$$

    \beta_t = \frac{\displaystyle\sum_{k=t-W+1}^{t} \lambda^{t-k}\,
    v_k\, v_{k-1}}{\displaystyle\sum_{k=t-W+1}^{t} \lambda^{t-k}\,
    v_{k-1}^2}, \qquad \alpha_t = \beta_t - 1,
    

$$

donde $v_t$ denota el precio del combustible en la semana $t$. Como
se demostró rigurosamente, este estimador existe y es único
bajo la condición $W > 1$ y no degeneración de los datos en la
ventana. La interpretación de $\alpha_t$ como coeficiente OLS de un
modelo AR(1) local sin intercepto proporciona una base estadística
sólida y una lectura económica directa: $\alpha_t > 0$ indica
crecimiento local, $\alpha_t < 0$ indica contracción, y $\alpha_t \approx 0$ indica estabilidad.

#### Discretización vía K-medias

A diferencia de la discretización por umbrales fijos empleada
previamente para el análisis
exploratorio del PIB, en esta sección se adopta un enfoque
completamente basado en datos.  El algoritmo K-medias particiona el
conjunto $\{\alpha_W, \ldots, \alpha_T\}$ en $K$ clústeres
minimizando la inercia intra-grupo:

$$

    \min_{\{C_1, \ldots, C_K\}} \sum_{k=1}^{K}
    \sum_{\alpha \in C_k} \|\alpha - \mu_k\|^2,
    

$$

donde $\mu_k$ es el centroide del clúster $k$.  Cada $\alpha_t$ se
asigna a su clúster correspondiente, generando la secuencia de
estados discretos $\{S_t\}$.  Los estados se ordenan según la
magnitud de sus centroides ($\mu_1 < \mu_2 < \cdots < \mu_K$), de
modo que $s_1$ corresponde siempre al régimen de mayor caída y $s_K$
al de mayor alza. Este orden ascendente invierte la convención pedagógica del Capítulo~[Ref: cap:TCRA-Markov], donde $s_1$ designaba el crecimiento más fuerte. La diferencia es puramente notacional, fruto de que K-medias ordena los centroides de menor a mayor; las probabilidades de transición y los resultados no cambian bajo ninguna de las dos etiquetas.

\begin{remark}[Justificación de K-medias sobre umbrales fijos]

Los umbrales fijos (e.g., $\alpha < -0.01$ para "contracción")
adolecen de dos limitaciones fundamentales: (i) son exógenos al
proceso generador de los datos, lo que puede sesgar la identificación
de regímenes, y (ii) imponen el mismo esquema de partición a series
con dinámicas potencialmente distintas. K-medias resuelve ambos
problemas al determinar los puntos de corte endógenamente,
adaptándose a la distribución empírica de cada serie.
\end{remark}

\begin{remark}[Selección del número de estados $K$]

Matemáticamente, el índice de Davies-Bouldin (DB) evalúa la calidad del agrupamiento midiendo la dispersión de cada clúster frente a la distancia entre sus centroides. Por diseño, valores **menores** de DB indican agrupamientos más compactos y mejor separados espacialmente, representando particiones óptimas. Sin embargo, en el análisis empírico de variables económicas reguladas, la selección del número de regímenes $K^*$ no se limita al mínimo matemático absoluto de DB. Se adopta un enfoque multicriterio que armoniza la separabilidad de los clústeres, la tasa de reducción de la inercia (método del codo) y la interpretabilidad semántica de los estados resultantes para el diseño de políticas públicas.

Por ejemplo, para la serie Regular, aunque $K=4$ o $K=6$ reportan índices de DB ligeramente inferiores, la ganancia en la granularidad no justifica la pérdida de parsimonia en la estimación de la matriz de transición; por ello se adopta $K^*=3$ (caída, estabilidad y alza), que coincide con un codo marcado de inercia y con una interpretación simple del mercado. En contraste, para Kerosene se selecciona $K^*=5$ a pesar de que $K=4$ posea un DB menor ($0.4326$), debido a la necesidad analítica de aislar el estado de 'Alza extrema' ($\mu_5 = +0.0172$), el cual captura shocks de precios excepcionales ausentes en otros combustibles. La Tabla~[Ref: tab:davies-bouldin] detalla los valores numéricos de DB e inercia que fundamentan el balance final para cada serie, donde los valores en negrita corresponden al número óptimo de estados $K^*$ seleccionado.
\end{remark}

*Tabla: Índice de Davies-Bouldin e inercia intra-clúster para diferentes números de estados $K$.* (tab:davies-bouldin)

| **Serie** | $K$ | **Davies-Bouldin** | **Inercia** |
| --- | --- | --- | --- |
| Regular | 2 | 0.5440 | 0.0014 |
|  | **3** | **0.5662** | **0.0007** |
|  | 4 | 0.4889 | 0.0005 |
|  | 5 | 0.5008 | 0.0003 |
|  | 6 | 0.4470 | 0.0002 |
|  | 7 | 0.4388 | 0.0001 |
|  | 8 | 0.4646 | 0.0001 |
|  | 9 | 0.4755 | 0.0001 |
| Superior | 2 | 0.5692 | 0.0014 |
|  | 3 | 0.5514 | 0.0007 |
|  | **4** | **0.5456** | **0.0004** |
|  | 5 | 0.5204 | 0.0003 |
|  | 6 | 0.5104 | 0.0002 |
|  | 7 | 0.4947 | 0.0001 |
|  | 8 | 0.4845 | 0.0001 |
|  | 9 | 0.4322 | 0.0001 |
| Diésel | 2 | 0.5535 | 0.0028 |
|  | **3** | **0.5281** | **0.0016** |
|  | 4 | 0.5508 | 0.0009 |
|  | 5 | 0.5250 | 0.0006 |
|  | 6 | 0.4911 | 0.0004 |
|  | 7 | 0.4832 | 0.0003 |
|  | 8 | 0.4760 | 0.0002 |
|  | 9 | 0.4840 | 0.0002 |
| Kerosene | 2 | 0.5709 | 0.0056 |
|  | 3 | 0.5043 | 0.0032 |
|  | 4 | 0.4326 | 0.0014 |
|  | **5** | **0.4622** | **0.0010** |
|  | 6 | 0.4982 | 0.0007 |
|  | 7 | 0.5374 | 0.0005 |
|  | 8 | 0.5155 | 0.0004 |
|  | 9 | 0.4601 | 0.0003 |

\begin{remark}[Verificación de estacionariedad de $\alpha_t$]

Antes de aplicar el algoritmo de discretización, se evaluó formalmente la estacionariedad de las series $\{\alpha_t\}$ utilizando dos pruebas complementarias cuyos resultados se detallan en la Tabla~[Ref: tab:adf-kpss], la cual resume los estadísticos ADF ($H_0$: raíz unitaria) y KPSS ($H_0$: estacionariedad) con sus respectivos $p$-valores. Para las series Regular y Diésel, el test de Dickey-Fuller aumentado (ADF) rechaza formalmente la presencia de raíz unitaria al nivel del 5\,\% ($p = 0.041$ y $p = 0.048$, respectivamente). En las series Superior y Kerosene, los $p$-valores del ADF se sitúan por encima del umbral convencional ($p = 0.136$ y $p = 0.131$, respectivamente), por lo que el test ADF por sí solo no resulta concluyente para rechazar la no-estacionariedad de estas dos variables debido a las limitaciones de poder del test en muestras con alta persistencia local. 

Sin embargo, al aplicar el test KPSS, cuya hipótesis nula $H_0$ postula la estacionariedad de la serie, ninguna de las cuatro variables rechaza la hipótesis de estacionariedad ($p > 0.10$ en todos los casos, con estadísticos KPSS marcadamente inferiores al valor crítico del 5\,\% de 0.463). Esta discrepancia parcial entre los tests ADF y KPSS para Superior y Kerosene es un comportamiento conocido en econometría de series financieras con volatilidad moderada (clasificada a menudo como persistencia fraccional o estacionariedad débil). La conjunción de un test KPSS que no rechaza la estacionariedad y la ausencia de raíces unitarias explosivas justifica de manera consistente el tratamiento de $\{\alpha_t\}$ como un proceso de covarianza estacionaria apto para el modelado de Markov, confirmando que el operador local actúa de facto como un filtro estabilizador de la tendencia de precios. En síntesis, para Regular y Diésel ambos tests coinciden en la estacionariedad; para Superior y Kerosene la conclusión descansa principalmente en el KPSS, ante un ADF no concluyente, lo que basta para tratar las cuatro series como estacionarias en sentido débil.
\end{remark}

*Tabla: Tests de estacionariedad sobre las series $\{\alpha_t\* (tab:adf-kpss)

| **Serie** | **ADF** | $p$ | **Lags** | **Crit.\ 5\%** | **KPSS** | $p$ | **Decisión** |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Regular | $-2.938$ | $0.041$ | 9 | $-2.869$ | $0.218$ | $>0.10$ | Estacionaria |
| Superior | $-2.420$ | $0.136$ | 9 | $-2.869$ | $0.250$ | $>0.10$ | Estacionaria$^\dagger$ |
| Diésel | $-2.879$ | $0.048$ | 1 | $-2.869$ | $0.269$ | $>0.10$ | Estacionaria |
| Kerosene | $-2.438$ | $0.131$ | 12 | $-2.869$ | $0.262$ | $>0.10$ | Estacionaria$^\dagger$ |
| \multicolumn{8}{p{14.5cm}}{\footnotesize $^\dagger$ ADF no significativo al 5\%, pero KPSS confirma estacionariedad ($p > 0.10$, estadístico $\ll 0.463$ = valor crítico al 5\%). En estas dos series la estacionariedad se concluye con base en el KPSS, dado que el ADF no es concluyente.} |  |  |  |  |  |  |  |

### Etapa 2: Estimación de la Matriz de Transición

Una vez obtenida la secuencia $\{S_t\}$, se modela como cadena de
Markov de primer orden.  La estimación de la matriz de transición
$\hat{\mat{P}}$ sigue el procedimiento NNLS desarrollado empíricamente
en este documento.  Se resalta aquí el esquema matricial.

A partir de la codificación indicadora (*one-hot*) de los estados, se
construyen las matrices $\mat{X}_0$ (estados actuales) y $\mat{X}_1$
(estados siguientes).  La linealización vía producto de Kronecker
transforma el problema matricial

$$

    \hat{\mat{P}} = \argmin_{\mat{P} \in \mathbb{S}(K,K)}
    \|\mat{X}_1 - \mat{P}\mat{X}_0\|_F^2
    

$$

en el sistema lineal $\text{vec}(\mat{X}_1) = (\mat{X}_0^T \otimes \mat{I}_K)\,\text{vec}(\mat{P})$, 
que se resuelve mediante el problema aumentado de NNLS:

$$

    \hat{\vect{c}} = \argmin_{\vect{c} \ge \mathbf{0}}
    \left\| \begin{bmatrix} \mat{S}_0 \\ w\mat{C}_{eq}
    \end{bmatrix} \vect{c} - \begin{bmatrix} \vect{b} \\
    w\mathbf{1}_K \end{bmatrix} \right\|_2^2,
    

$$

unitaria por columnas. Como se ha establecido de forma matemática en
este tratado, este problema es convexo, su conjunto factible es no vacío, y la solución existe y es única
cuando $\mat{S}_0$ tiene rango completo por columnas, una condición
que requiere no solo que cada estado de origen aparezca al menos una vez
con transiciones salientes en la secuencia de entrenamiento, sino también
que la matriz de diseño posea un rango adecuado para garantizar la proyección
unívoca en el ortante positivo. Tras la solución NNLS, se aplica
una normalización final por columnas para corregir desviaciones de
precisión de punto flotante.

### Protocolo de Optimización de Hiperparámetros

La selección de los hiperparámetros $(W, \lambda, K)$ representa un reto clave de optimización en este trabajo. Se implementó una búsqueda exhaustiva en grilla sobre el dominio:

$$

    W \in \{2, 3, \ldots, 52\}, \quad
    \lambda \in \{0.80, 0.81, \ldots, 1.00\}, \quad
    K \in \{2, 3, \ldots, 9\}.
    

$$

Es pertinente precisar que, aunque la búsqueda exhaustiva en grilla se considera ineficiente en espacios de alta dimensionalidad, en este problema el dominio discreto está estrictamente delimitado por la restricción $W \le 52$ (establecida constructivamente para evitar la degeneración del estimador conforme al Teorema~[Ref: prop:degeneracion] y alineada con el ciclo anual natural del mercado) y pasos de centésimas para $\lambda$. Esto resulta en un espacio de búsqueda acotado (menor a $3\,000$ combinaciones por serie) cuya evaluación secuencial tarda escasos milisegundos por iteración. Este enfoque exhaustivo es óptimo para este contexto, ya que garantiza la identificación de la combinación de parámetros que minimiza globalmente el error predictivo sin los riesgos de estancamiento en mínimos locales inherentes a los métodos metaheurísticos.

Para cada combinación de parámetros en la grilla, el desempeño del modelo se evaluó fuera de muestra mediante un protocolo de ventanas deslizantes (*rolling window*) de paso unitario. La selección de la combinación óptima final se rige por el siguiente criterio jerárquico:

    * **Criterio primario (RMSE):** Se selecciona la combinación que minimiza el error cuadrático medio (RMSE) de pronóstico puntual de precios fuera de muestra. La primacía de esta métrica responde a la naturaleza aplicada del modelo: en el mercado energético regulado, la exactitud numérica de la predicción de precios representa el objetivo crítico.
    * **Criterio secundario (Exactitud):** En caso de empates en el RMSE, se maximiza la tasa de exactitud cualitativa en la predicción del régimen económico futuro.
    * **Desempate y Regularización (AIC):** Para precaver el sobreajuste que podría surgir al optimizar exclusivamente el RMSE (lo que favorecería modelos sobreparametrizados con un número excesivo de estados $K$), se penaliza la complejidad del modelo a través del Criterio de Información de Akaike (AIC):
    
$$

        \text{AIC}(K) = 2K(K-1) - 2\mathcal{L}_K,
        
    
$$

    donde $\mathcal{L}_K = \sum_{i,j} C_{ij} \ln \hat{P}_{ij} $ representa la log-verosimilitud del modelo con $K$ regímenes, y el término $K(K-1)$ denota la cantidad de parámetros libres e independientes presentes en una matriz estocástica de tamaño $K \times K$.

El costo computacional total de la búsqueda escala como
$O\big(|\mathcal{W}|\,|\Lambda|\,|\mathcal{K}|\,(T + TKI + K^3)\big)$, 
donde $I$ es el número de iteraciones de K-medias.  Con
$K \le 9$ y $T \approx 400$, el tiempo de ejecución es dominado por
el tamaño de la grilla y no por los algoritmos internos,
permitiendo paralelización directa.

## Resultados y Análisis

### El Fenómeno de Degeneración del Modelo

El análisis preliminar reveló un hallazgo crítico sobre la estabilidad estructural del marco: una búsqueda de hiperparámetros sin restricciones temporales conduce inevitablemente a la *degeneración del modelo*, produciendo métricas aparentemente favorables, asociadas al sobre-suavizado y la pérdida de variabilidad. Cuando la ventana de inercia toma valores excesivamente grandes ($W \gg 100$), el operador local acumula un volumen de historia desproporcionado, lo que provoca que la serie de tasas de cambio $\alpha_t$ se aplane de manera artificial hasta perder toda variabilidad informativa. En este límite, la tasa local converge de manera asintótica hacia el promedio global de crecimiento de la muestra entera. En consecuencia, el operador se desensibiliza frente a cambios marginales instantáneos y el modelo tiende asintóticamente al comportamiento rígido de un modelo autorregresivo lineal global (o una caminata aleatoria con deriva constante en los niveles de precios), perdiendo su capacidad para caracterizar transiciones dinámicas rápidas.

Es fundamental enfatizar que el establecimiento de una ventana local acotada ($W \le 52$) no implica que el modelo descarte la memoria histórica a largo plazo de la serie. Por el contrario, mientras que el operador $\alpha_t$ se evalúa localmente para capturar la tasa marginal instantánea (eliminando tendencias estocásticas y raíces unitarias en los niveles de precios), la matriz de transición $\hat{\mat{P}}$ y las dinámicas estocásticas de Markov se estiman utilizando la serie histórica completa de observaciones $T$. De esta forma, el modelo integrado preserva la historia temporal a través de las frecuencias de transición conjuntas y la distribución estacionaria, pero opera sobre una variable filtrada libre de tendencia, logrando modelar no-linealidades y asimetrías que las caminatas aleatorias convencionales no pueden capturar.

Este fenómeno admite una explicación formal.  Considérese el
comportamiento de la varianza de $\alpha_t$ como función de $W$.

\begin{theorem}[Degeneración por sobre-suavizado]

Sea $\{v_t\}_{t=1}^{T}$ una serie temporal con $v_t > 0$ para
todo $t$, y sea $\alpha_t(W)$ el operador ETCRAM con
$\lambda = 1$ y ventana $W$.  Entonces:

    * Para $W = T$, el operador es constante:
    $\alpha_t(T) = \bar{\alpha}$ para todo $t \ge T$, donde
    
$$

        \bar{\alpha} = -1 + \frac{\sum_{t=1}^{T} v_t\,v_{t-1}}
        {\sum_{t=1}^{T} v_{t-1}^2}.
    
$$

    * En consecuencia, $\mathrm{Var}(\alpha_t(T)) = 0$, y
    para cualquier $K \ge 2$, el algoritmo K-medias asigna todas
    las observaciones a un único clúster efectivo
    ($K_{\mathrm{eff}} = 1$).
    * La matriz de transición resultante es
    $\hat{\mat{P}} = \mat{I}_K$, y el modelo predice
    perpetuamente el estado inicial.

\end{theorem}

\begin{proof}
El punto (1) es inmediato: cuando $W = T$, las sumatorias en
\eqref{eq:alpha_t_combustibles} abarcan toda la muestra y no
dependen de $t$, por lo que $\beta_t$ es constante.  Para (2),
si $\alpha_t = \bar{\alpha}$ para todo $t$, entonces
$\|\alpha_t - \mu_k\| = 0$ para el clúster que contiene
$\bar{\alpha}$, y todos los $\alpha_t$ son asignados al mismo
clúster.  Para (3), si $S_t = s_j$ para todo $t$, entonces
$\mat{X}_0 = \mat{X}_1$ y la solución de
\eqref{eq:frobenius_combustibles} es $\hat{\mat{P}} = \mat{I}_K$
(o más precisamente, la columna $j$ es $\vect{e}_j$ y las
restantes columnas son indeterminadas por ausencia de datos,
pero la normalización NNLS las fuerza a vectores unitarios).
\end{proof}

Para evitar esta degeneración, se impuso el límite $W \le 52$
semanas, justificado por dos argumentos convergentes: (a) preserva
la variabilidad necesaria de $\alpha_t$ para que K-medias identifique
regímenes distintos, y (b) corresponde al ciclo anual natural del
mercado de combustibles, más allá del cual la "memoria" del
operador pierde relevancia económica.

### Análisis de Sensibilidad

El análisis de sensibilidad dentro del dominio restringido $W \le 52$ (Figura~[Ref: fig:sensitivity]) evalúa el mejor desempeño alcanzable (mínimo RMSE) sobre todas las combinaciones de $(\lambda, K)$ para cada valor de $W$.

![Análisis de sensibilidad del desempeño respecto a la ventana $W$.](figures/W_sensitivity_analysis_final_v6.png)
*Figura: Análisis de sensibilidad del desempeño respecto a la ventana $W$.* (fig:sensitivity)

Como se observa en dicha figura, para cada valor de $W$ se 
muestra el modelo con menor RMSE (izquierda) y mayor exactitud (derecha), 
donde el círculo marca el óptimo global. Se identifica una tendencia monotónica 
en la cual ventanas más largas, próximas a 52 semanas, producen un menor error 
de pronóstico en todas las series. Esto indica que la predictibilidad de los 
precios de combustibles hondureños está dominada por tendencias de largo plazo 
(semianuales a anuales) más que por la volatilidad semanal de corto plazo.

Este resultado empírico tiene una interpretación económica directa:
el mercado de combustibles en Honduras opera bajo un esquema de
precios regulados con ajustes periódicos, lo que suaviza las
fluctuaciones de corto plazo y concentra la dinámica informativa en
componentes de frecuencia baja.  El operador $\alpha_t$ con $W \approx 52$ 
captura eficazmente estos ciclos anuales, sin llegar al régimen degenerado de sobre-suavizado, filtrando el ruido semanal.

### Hiperparámetros Óptimos

Aplicando el protocolo jerárquico definido en la
Sección~[Ref: subsec:protocolo_optimizacion], se identificaron las
configuraciones óptimas para cada serie.  La
Tabla~[Ref: tab:hiperparametros_optimos] resume los resultados.

*Tabla: Combinación óptima de hiperparámetros por serie de combustible.* (tab:hiperparametros_optimos)

| lcccccc@{}}

**Serie** | $W^*$ | $\lambda^*$ | $K^*$ | **Exactitud** | **RMSE** | **AIC** |
| --- | --- | --- | --- | --- | --- | --- |
| Regular | 50 | 0.99 | 3 | 85.10\% | 0.8394 | 190.87 |
| Superior | 52 | 0.99 | 4 | 62.78\% | 0.9769 | 272.80 |
| Diésel | 50 | 0.97 | 3 | 71.56\% | 1.0816 | 175.48 |
| Kerosene | 52 | 0.98 | 5 | 68.20\% | 1.1714 | 225.79 |

![Espacio de optimización de hiperparámetros consolidado por número de regímenes ($K$).](figures/4_bubble_chart.png)
*Figura: Espacio de optimización de hiperparámetros consolidado por número de regímenes ($K$).* (fig:bubble_chart)

El espacio de optimización consolidado representado en la Figura~[Ref: fig:bubble_chart] muestra las alternativas paramétricas para cada valor de $K$ mediante burbujas. Se observa que, en términos puramente matemáticos, las configuraciones con $K=2$ estados reportan con frecuencia exactitudes sumamente elevadas y valores de AIC bajos. Este comportamiento es algebraicamente predecible: reducir el sistema a una clasificación binaria de dos estados (por ejemplo, 'Alza' y 'Caída') simplifica drásticamente el espacio de estados y reduce los parámetros libres en la matriz de transición a solo 2, minimizando la penalización del AIC y estabilizando el estimador. 

Sin embargo, desde una perspectiva de aplicación económica y diseño de políticas energéticas, un modelo binario de $K=2$ carece de utilidad analítica práctica, dado que agrupa de forma indiferenciada variaciones marginales estables y fluctuaciones de crisis extremas. Por ello, la elección óptima de $K^* \ge 3$ sacrifica parsimonia matemática en favor de una granularidad semántica indispensable para distinguir entre estabilidad y shocks de variaciones de precios de combustibles. Por ejemplo, para Regular ($K^*=3$), el punto seleccionado domina con una burbuja de exactitud del $85.1\%$ y un AIC balanceado. Para Kerosene ($K^*=5$), la mayor dispersión y el estado atípico de alza extrema obligan a adoptar un modelo de cinco estados, representando el balance óptimo bajo el criterio de selección jerárquico.

Se observan dos patrones transversales de interés. Por una parte, los valores de $\lambda^*$ se concentran firmemente en el intervalo $[0.97, 0.99]$. Es importante destacar que el decaimiento de ponderación local no se fija de forma predeterminada ni restringida en la unidad ($\lambda = 1.0$), sino que se evalúa de manera dinámica en la búsqueda en grilla. Si bien en series con dinámicas reguladas estables el óptimo resulta cercano a la ponderación uniforme (confirmando que observaciones recientes e históricas dentro del año aportan información estructural equivalente para suavizar el ruido semanal), el marco teórico conserva la flexibilidad de adoptar valores menores de $\lambda$ en series con fluctuaciones de alta frecuencia. Por otra parte, el número óptimo de regímenes revela una jerarquía clara de complejidad de mercado:

    * **Regular y Diésel** ($K^*=3$): Mercados de dinámica
    simple con tres estados bien diferenciados.
    * **Superior** ($K^*=4$): Complejidad intermedia con un
    estado adicional de alza moderada.
    * **Kerosene** ($K^*=5$): Mercado más complejo y
    fragmentado, requiriendo cinco estados para capturar su dinámica.

La relación complejidad-desempeño y la jerarquía de predictibilidad resultante se sintetizan en la Figura~[Ref: fig:comparativa].

![Jerarquía de predictibilidad de los combustibles.](figures/Comparativa_cual_mejor.png)
*Figura: Jerarquía de predictibilidad de los combustibles.* (fig:comparativa)

A partir de estos resultados, se observa que la gasolina Regular es la serie más previsible con el menor 
RMSE y mayor exactitud. Por el contrario, el Kerosene se presenta como 
el caso más desafiante, requiriendo más estados porque su estructura empírica es más 
heterogénea. La correlación positiva observada entre $K^*$ y 
el RMSE sugiere que la complejidad estructural inherente al mercado, y 
no una deficiencia del modelo, es lo que limita la máxima precisión alcanzable.

### Identificación e Interpretación Económica de los
Regímenes

Con los hiperparámetros óptimos fijados, el operador $\alpha_t$
genera distribuciones de tasas de cambio altamente concentradas
alrededor de cero, consistentes con un mercado regulado donde los
ajustes semanales drásticos son infrecuentes.  La asignación temporal de regímenes resultante del agrupamiento K-medias se ilustra en la Figura~[Ref: fig:kmeans_regimes].

![Regímenes identificados por K-medias sobre la serie $\alpha_t$.](figures/8_kmeans_final_regimes_plot)
*Figura: Regímenes identificados por K-medias sobre la serie $\alpha_t$.* (fig:kmeans_regimes)

En este panel de resultados, cada punto representa una semana y su color el régimen asignado sobre la serie $\alpha_t$, mientras que las líneas punteadas horizontales marcan los centroides de cada clúster. 
Se observan transiciones coherentes que corresponden a eventos 
de mercado específicos, como el período 2020--2021 relacionado 
con el impacto y la recuperación pospandemia. Esta segmentación 
resulta en distribuciones estacionarias bien definidas que facilitan 
la interpretación de la dinámica del mercado.

Los centroides de K-medias proporcionan una interpretación económica
directa de cada régimen.  La Tabla~[Ref: tab:centroides_regimenes]
presenta los centroides y la prevalencia temporal de cada estado
para las cuatro series.

*Tabla: Centroides ($\mu_k$) y prevalencia temporal de los regímenes identificados.* (tab:centroides_regimenes)

| llcc@{}}

**Serie** | **Régimen** | **Centroide** ($\mu_k$) | **Prevalencia** |
| --- | --- | --- | --- |
| Regular ($K^*=3$) | $s_1$: Caída fuerte | $-0.0036$ | 24.6\% |
|  | $s_2$: Estabilidad | $-0.0001$ | 47.9\% |
|  | $s_3$: Alza fuerte | $+0.0044$ | 27.4\% |
| Superior ($K^*=4$) | $s_1$: Caída fuerte | $-0.0043$ | 15.7\% |
|  | $s_2$: Estabilidad | $-0.0009$ | 44.6\% |
|  | $s_3$: Alza moderada | $+0.0019$ | 23.7\% |
|  | $s_4$: Alza fuerte | $+0.0055$ | 16.0\% |
| Diésel ($K^*=3$) | $s_1$: Caída fuerte | $-0.0061$ | 17.4\% |
|  | $s_2$: Estabilidad | $-0.0010$ | 47.4\% |
|  | $s_3$: Alza fuerte | $+0.0053$ | 35.1\% |
| Kerosene ($K^*=5$) | $s_1$: Caída fuerte | $-0.0080$ | 17.0\% |
|  | $s_2$: Estabilidad | $-0.0013$ | 47.4\% |
|  | $s_3$: Alza moderada | $+0.0044$ | 16.0\% |
|  | $s_4$: Alza fuerte | $+0.0084$ | 15.0\% |
|  | $s_5$: Alza extrema | $+0.0172$ | 4.6\% |

En la Tabla~[Ref: tab:centroides_regimenes] se detallan los centroides ($\mu_k$) calculados mediante K-medias y su prevalencia temporal respectiva. Es oportuno enfatizar que las etiquetas semánticas asignadas a cada estado (tales como 'Alza moderada' o 'Caída fuerte') no obedecen a un criterio cualitativo subjetivo o discrecional del investigador. Por el contrario, son el reflejo directo de la magnitud y signo de los centroides de clúster estimados endógenamente por el algoritmo a partir de la muestra de tasas $\alpha_t$. De este modo, centroides muy cercanos a cero ($|\mu_k| < 0.002$) se clasifican de forma unívoca como estabilidad, mientras que desviaciones positivas o negativas definen las distintas intensidades de alzas y caídas de precios regulados. El centroide representa la tasa de cambio semanal promedio que caracteriza a cada régimen, mientras que la prevalencia indica la proporción del tiempo total analizado, entre los años 2017 y 2025, en que el mercado se mantuvo en dicho estado operativo.

Los cuatro mercados comparten un régimen dominante de
*Estabilidad* que ocupa aproximadamente el 45--48\% del
período analizado, con centroides cercanos a cero ($|\mu_k| < 0.002$).
Esto es consistente con el esquema de regulación de precios vigente
en Honduras, donde los ajustes semanales son típicamente pequeños.
Por su parte, los mercados de Regular y Diésel exhiben una estructura
simétrica de tres estados (caída--estabilidad--alza), mientras que
Superior y Kerosene requieren estados intermedios adicionales para
capturar su dinámica.  Finalmente, el Kerosene presenta un estado
excepcional de *Alza extrema* ($\mu_5 = +0.0172$,
prevalencia $4.6\%$), que no tiene análogo en los otros combustibles.
Este régimen raro pero significativo es consistente con la naturaleza
del Kerosene como subproducto de refinación con menor cobertura de
mecanismos de amortiguación de precios, lo cual sugiere una menor amortiguación relativa, 
aunque se requerirían variables regulatorias adicionales para confirmarlo formalmente.

### Dinámica de Transición

La estructura estocástica del mercado se captura en las matrices de transición $\hat{\mat{P}}$ estimadas vía NNLS, cuyos grafos de transición asociados se presentan en la Figura~[Ref: fig:transition_graphs].

![Grafos de transición estimados para los modelos optimizados.](figures/graph_super_final.png)
*Figura: Grafos de transición estimados para los modelos optimizados.* (fig:transition_graphs)

En estas representaciones, el valor dentro de cada nodo corresponde al centroide del clúster K-medias, permitiendo 
cuantificar el comportamiento semanal del precio. Los lazos (*self-loops*) 
reflejan la probabilidad de permanencia en el régimen actual, mientras que las aristas 
dirigidas indican las probabilidades de transición. Se destacan cuatro patrones: 
la alta inercia del mercado con lazos de autopermanencia superiores al 89\%, transiciones 
predominantemente entre estados adyacentes que evitan saltos abruptos, una estructura 
lineal simple en Regular y Diésel frente a una mayor conectividad en Superior y Kerosene, 
y la naturaleza transitoria del estado de alza extrema en el Kerosene.

Las probabilidades de autoloop merecen atención especial.  Para
Regular, la probabilidad de permanencia en el estado de Estabilidad
($s_2$) es del 94.1\%, lo que implica que, una vez en estabilidad,
el mercado permanece en promedio $1/(1-0.941) \approx 17$ semanas
antes de transitar a otro régimen.  Para Kerosene, el estado de
Estabilidad tiene una permanencia del 96.7\%, equivalente a $\sim 30$
semanas.  Estos tiempos de permanencia extremadamente largos confirman
cuantitativamente la inercia del mercado hondureño de
combustibles; no obstante, cabe precisar que estas probabilidades se estiman sobre la señal
suavizada $\alpha_t$ y no sobre el precio crudo directamente.

### Propiedades Espectrales y Estabilidad Estructural

Para fundamentar rigurosamente las observaciones cualitativas
anteriores, se procede al análisis espectral de las matrices
$\hat{\mat{P}}$.

\begin{definition}[Brecha espectral y tiempo de mezcla]

Sea $\hat{\mat{P}} \in \mathbb{R}^{K \times K}$ una matriz de
transición estocástica por columnas, irreducible y aperiódica, con
autovalores $\lambda_1 = 1 > |\lambda_2| \ge \cdots \ge |\lambda_K|$.
Se define la **brecha espectral** como $\gamma = 1 - |\lambda_2|$,
y el **tiempo de mezcla** como

$$

    t_{\text{mix}}(\varepsilon) = \left\lceil
    \frac{\ln(1/\varepsilon)}{\gamma} \right\rceil,
    

$$

que mide (en número de pasos) el tiempo necesario para que la cadena
se aproxime a su distribución estacionaria $\boldsymbol{\pi}$ con
tolerancia $\varepsilon$ en norma de variación total.
\end{definition}

La aplicabilidad de esta definición requiere verificar la
irreducibilidad de $\hat{\mat{P}}$.

\begin{remark}[Verificación de irreducibilidad]

La irreducibilidad de las matrices $\hat{\mat{P}}$ se verificó
numéricamente calculando $\hat{\mat{P}}^n$ para $n$ creciente
y comprobando la positividad estricta de todas las entradas.
Los resultados son los siguientes:

    * **Regular** ($K=3$): $\hat{\mat{P}}^2 > 0$
    (todas las entradas estrictamente positivas desde $n=2$).
    * **Diésel** ($K=3$): $\hat{\mat{P}}^1 > 0$
    (todas las entradas son estrictamente positivas ya en la matriz original).
    * **Superior** ($K=4$): $\hat{\mat{P}}^2 > 0$.
    * **Kerosene** ($K=5$): $\hat{\mat{P}}^4 > 0$
    (la estructura de cadena lineal $s_1 \leftrightarrow s_2 \leftrightarrow s_3
    \leftrightarrow s_4 \leftrightarrow s_5$ requiere al menos 4 pasos para
    conectar los extremos $s_1$ y $s_5$).

Esta verificación empírica se sustenta directamente en la teoría general de matrices y cadenas de Markov regulares. Formalmente, una matriz de transición estocástica se define como **regular** (o primitiva) si existe algún entero positivo $n \ge 1$ para el cual la matriz elevada a dicha potencia posee únicamente entradas estrictamente mayores a cero ($\hat{P}^n_{ij} > 0$). La confirmación numérica expuesta demuestra la regularidad de las cuatro cadenas estimadas. De acuerdo con el **Teorema de Perron-Frobenius** para matrices regulares no negativas, esta condición es suficiente para garantizar la existencia y unicidad de un autovalor dominante $\lambda_1 = 1$, con un autovector asociado que representa la distribución estacionaria única $\boldsymbol{\pi}$ del sistema. Este resultado teórico riguroso asegura que la cadena converge asintóticamente al equilibrio estacionario de forma independiente a la condición inicial $\vect{x}_0$ del mercado.
\end{remark}

La Tabla~[Ref: tab:propiedades_espectrales] presenta las métricas
espectrales calculadas para cada combustible, utilizando
$\varepsilon = 0.01$ en la definición del tiempo de mezcla.

*Tabla: Propiedades espectrales y distribuciones estacionarias de las matrices de transición.* (tab:propiedades_espectrales)

| lcccc@{}}

**Métrica** | **Regular** | **Diésel** | **Superior** | **Kerosene** |
| --- | --- | --- | --- | --- |
| $\|\lambda_2\|$ | 0.963 | 0.960 | 0.961 | **0.979** |
| Brecha espectral $\gamma$ | 0.037 | 0.040 | 0.039 | **0.021** |
| $t_{\text{mix}}$ (sem) | $\sim$27 | $\sim$25 | $\sim$26 | $\sim$**47** |
| $\boldsymbol{\pi}$ | $[0.25, 0.48, 0.27]$ | $[0.48, 0.16, 0.35]$ | $[0.17, 0.47, 0.22, 0.15]$ | $[0.19, 0.54, 0.09, 0.04, 0.15]$ |

\begin{remark}[Relación entre persistencia y brecha espectral]

Sea $\hat{\mat{P}}$ una matriz de transición $K \times K$ con
elementos diagonales $P_{ii} \ge 1 - \epsilon$ para todo $i$
(alta persistencia).  Entonces el segundo autovalor satisface
$|\lambda_2| \ge 1 - K\epsilon$, y por tanto la brecha espectral
$\gamma \le K\epsilon$.  En el caso analizado, los lazos de autopermanencia mínimos
observados son $\min_i P_{ii} = 0.891$ (Superior, $s_3$), dando
$\epsilon = 0.109$ y la cota $\gamma \le 4 \times 0.109 = 0.436$.
La brecha observada ($\gamma = 0.039$) es un orden de magnitud menor
que esta cota, indicando que la persistencia real del mercado
excede significativamente lo que los lazos de autopermanencia por sí solos
implican: las transiciones inter-estados también contribuyen a
la mezcla lenta.
\end{remark}

Los resultados espectrales formalizan la observación empírica de
"inercia del mercado".  La brecha espectral $\gamma$ mide la
velocidad de convergencia al equilibrio: un $\gamma$ pequeño implica
que el sistema tarda más en "olvidar" su estado inicial.  El
Kerosene exhibe la menor brecha espectral ($\gamma = 0.021$), lo que se
traduce en un tiempo de mezcla de casi un año ($\sim$47 semanas).
Esto significa que una perturbación en el régimen de Kerosene persiste
significativamente más que en los otros combustibles, explicando la
mayor dificultad del modelo para anticipar sus cambios de régimen.

La distribución estacionaria $\boldsymbol{\pi}$ confirma que, en
equilibrio, el estado de Estabilidad domina en todas las series
(componente más grande de $\boldsymbol{\pi}$), y los estados extremos
tienen probabilidades estacionarias menores, consistente con un
mercado regulado.

### Convergencia de las Cadenas de Markov

La evolución simulada de las probabilidades de estado bajo las matrices $\hat{\mat{P}}$ optimizadas se presenta en la Figura~[Ref: fig:kmeans_evolution], considerando el estado de mayor caída ($s_1$) como condición inicial.

![Evolución de las probabilidades de estado bajo los modelos K-medias.](figures/9_kmeans_markov_evolution.png)
*Figura: Evolución de las probabilidades de estado bajo los modelos K-medias.* (fig:kmeans_evolution)

En este gráfico, la línea vertical roja señala el momento en que se alcanza el equilibrio estacionario. Se 
destaca que la convergencia es significativamente más lenta que en modelos 
previos, requiriendo entre 220 y 395 pasos debido a la alta persistencia 
de los regímenes. Ambas estimaciones (el tiempo de mezcla de 25--47 semanas y la 
estabilización simulada a 220--395 pasos) responden a criterios de tolerancia distintos. 
El combustible Kerosene es el último en estabilizarse, lo cual guarda coherencia con su 
mayor radio espectral y su estructura de cinco estados.

La lentitud de convergencia no debe interpretarse como una debilidad del modelo, sino como la *expresión matemática de un hallazgo empírico central*: el mercado hondureño de combustibles se caracteriza por tendencias de largo plazo con transiciones infrecuentes entre regímenes. Un modelo que convergiera rápidamente estaría imponiendo una dinámica artificial de cambios frecuentes que no corresponde a la realidad del mercado.

### Cómputo Analítico de la Varianza del Pronóstico

Para responder a la necesidad de cuantificar formalmente la incertidumbre asociada a las proyecciones estocásticas del modelo, se deriva a continuación el cálculo analítico de la varianza del pronóstico multietapa. Sea $\vect{x}_t \in \mathbb{R}^K$ el vector indicador del estado en el instante actual $t$ (donde $x_{t, i} = 1$ si $S_t = s_i$ y $0$ en otro caso). La distribución de probabilidad del vector de estados para un horizonte de $n$ pasos adelante condicionado a la información disponible se evalúa como:

$$

    \vect{p}_{t+n|t} = (\hat{\mat{P}}^T)^n \vect{x}_t

$$

donde $\hat{\mat{P}}$ es la matriz de transición estocástica estimada. Si definimos el vector de centroides empíricos de los regímenes como $\boldsymbol{\mu} = [\mu_1, \mu_2, \dots, \mu_K]^T$, el valor esperado condicional de la tasa de cambio relativa en el paso $t+n$ se expresa como la combinación lineal:

$$

    \mathbb{E}[\alpha_{t+n} \mid \mathcal{F}_t] = \sum_{k=1}^K \mu_k (\vect{p}_{t+n|t})_k = \boldsymbol{\mu}^T \vect{p}_{t+n|t}

$$

Dado que el estado futuro es una variable categórica bajo una distribución multinomial condicional de un solo ensayo, la varianza condicional del pronóstico de la tasa (que representa la dispersión o incertidumbre predictiva en el horizonte $n$) se computa analíticamente como:

$$

    \mathrm{Var}(\alpha_{t+n} \mid \mathcal{F}_t) = \sum_{k=1}^K \mu_k^2 (\vect{p}_{t+n|t})_k - \left( \boldsymbol{\mu}^T \vect{p}_{t+n|t} \right)^2

$$

Esta formulación revela un comportamiento asintótico consistente con la teoría ergódica: en el corto plazo ($n = 1$), si el estado actual es conocido con certeza ($\vect{x}_t = \vect{e}_i$), la incertidumbre proviene únicamente de la entropía de transición inmediata del estado $s_i$. A medida que el horizonte de pronóstico se expande ($n \to \infty$), el vector de probabilidades condicionales converge a la distribución estacionaria única, $\vect{p}_{t+n|t} \to \boldsymbol{\pi}$. Por consiguiente, la varianza del pronóstico se expande de forma monótona hasta alcanzar la varianza incondicional de los estados en el equilibrio estacionario:

$$

    \lim_{n \to \infty} \mathrm{Var}(\alpha_{t+n} \mid \mathcal{F}_t) = \sum_{k=1}^K \mu_k^2 \pi_k - \left( \boldsymbol{\mu}^T \boldsymbol{\pi} \right)^2

$$

Este resultado proporciona una medida de riesgo matemática explícita para los tomadores de decisiones, permitiendo asociar cada proyección puntual de precios con una banda de dispersión condicional calculada directamente a partir de las propiedades algebraicas de $\hat{\mat{P}}$ y la geometría espacial de los clústeres.

### Superioridad sobre Discretización por Cuantiles

Para validar la elección de K-medias como método de discretización, se
comparó sistemáticamente contra un modelo de referencia de cuantiles fijos
($K=4$) utilizando la misma serie $\alpha_t$ optimizada.

La segmentación resultante del método de cuantiles se presenta en la Figura~[Ref: fig:quantile_regimes] para el modelo de referencia de $K=4$ estados.

![Segmentación de regímenes basada en cuantiles.](figures/11_quantile_final_regimes_plot.png)
*Figura: Segmentación de regímenes basada en cuantiles.* (fig:quantile_regimes)

Como se observa en dicha ilustración, los límites de los cuantiles imponen una partición equifrecuente que obliga a 
distribuir las observaciones de manera uniforme. Sin embargo, se aprecia una 
fragmentación excesiva del mercado en comparación con el método K-medias, 
donde pequeñas variaciones de ruido provocan reclasificaciones constantes, 
lo cual degrada la capacidad predictiva del modelo.

Las consecuencias de esta fragmentación en la dinámica de transición se ilustran en la Figura~[Ref: fig:quantile_evolution], la cual revela que las probabilidades de estado bajo cuantiles presentan oscilaciones más pronunciadas que bajo K-medias, lo que se traduce en pronósticos con mayor varianza.

![Evolución de probabilidades de estado bajo el método de cuantiles.](figures/12_quantiles_markov_evolution.png)
*Figura: Evolución de probabilidades de estado bajo el método de cuantiles.* (fig:quantile_evolution)

Como se observa en dicho gráfico, la evolución de probabilidades bajo cuantiles muestra una convergencia más acelerada hacia 
un equilibrio uniforme cercano al 25\% por estado. Aun así, este resultado 
evidencia que K-medias es superior frente al benchmark de cuantiles evaluado, mas 
no necesariamente frente a toda discretización no lineal posible; la imposición de 
frecuencias iguales en los cuantiles borra la distinción de un régimen de estabilidad dominante, 
eliminando información estructural crítica para el análisis del mercado.

### Desempeño Predictivo y Comparación con Modelos de Referencia

La evaluación final fuera de muestra se presenta en la Tabla~[Ref: tab:desempeno_final], comparando el modelo TCRA-Markov optimizado (K-medias) contra dos modelos de referencia: la discretización por cuantiles y el modelo clásico MS-AR [Cita: Hamilton1994]. En dicha comparación, los $p$-valores corresponden a pruebas $t$ pareadas para la exactitud y test de Diebold-Mariano para el RMSE. Los marcadores $^{**}$ y $^{*}$ indican significancia al 1\% y 5\%, respectivamente, tras aplicar la corrección de Holm-Bonferroni para controlar el error por familia de hipótesis.

*Tabla: Evaluación final del desempeño predictivo y significancia estadística.* (tab:desempeno_final)

| lccccccc@{}}

**Serie** | **TCRA-KMeans (Óptimo)** | **Cuantiles ($K=4$)** | **MS-AR** | **Significancia ($p$-valor)** |  |  |  |
| --- | --- | --- | --- | --- | --- | --- | --- |
| \cmidrule(lr){2-3} \cmidrule(lr){4-5} \cmidrule(lr){6-6}
\cmidrule(lr){7-8} | Exactitud | RMSE | Exactitud | RMSE | Estado | $p_{\text{Acc}}$ | $p_{\text{RMSE}}$ |
| Regular | **85.10\%** | **0.8394** | 36.01\% | 0.8671 | No conv. | $< 0.001^{**}$ | $< 0.05^{*}$ |
| Superior | **62.78\%** | **0.9769** | 49.03\% | 0.9849 | No conv. | $< 0.05^{*}$ | $= 0.22$ |
| Diésel | **71.56\%** | **1.0816** | 40.63\% | 1.1409 | No conv. | $< 0.05^{*}$ | $< 0.05^{*}$ |
| Kerosene | **68.20\%** | **1.1714** | 35.16\% | 1.2449 | No conv. | $< 0.05^{*}$ | $< 0.05^{*}$ |

\begin{remark}[Corrección por comparaciones múltiples]

La tabla presenta $4 \times 2 = 8$ tests de hipótesis simultáneos (4 series $\times$ 2 métricas de rendimiento). Para garantizar la validez inferencial del análisis, se aplicó la corrección secuencial de Holm-Bonferroni [Cita: Holm1979] con el objetivo de controlar la tasa de error por familia de hipótesis (Family-Wise Error Rate, FWER) al nivel global $\alpha = 0.05$. Los 7 $p$-valores estadísticamente significativos sobreviven de manera robusta la corrección: el $p$-valor de menor magnitud (Regular, exactitud: $p < 0.001$) se compara de forma conservadora contra el umbral ajustado $\alpha/8 = 0.00625$, el segundo ($p < 0.05$) contra $\alpha/7 = 0.00714$, y así de manera sucesiva. El único $p$-valor no significativo (Superior, RMSE: $p = 0.22$) no requiere modificación alguna. La conclusión cualitativa e inferencial se mantiene inalterada tras el ajuste: el modelo propuesto con discretización K-medias supera de manera estadísticamente significativa al esquema de cuantiles para todas las series analizadas.
\end{remark}

Tres conclusiones se derivan de estos resultados:

**Primero**, el modelo TCRA-Markov con K-medias supera al
modelo de referencia de cuantiles de manera estadísticamente significativa en
exactitud de clasificación para las cuatro series ($p < 0.05$,
prueba T pareada, tras corrección de Holm-Bonferroni). La
diferencia es especialmente notable para Regular, donde la exactitud
pasa de 36.01\% a 85.10\%. Así, el modelo domina en exactitud y mejora significativamente 
el RMSE en 3 de las 4 series.

**Segundo**, en términos de RMSE (pronóstico puntual de
precios), la mejora es significativa para Regular, Diésel y Kerosene
($p < 0.05$), pero no para Superior ($p = 0.22$), cuya diferencia en
RMSE es marginal (0.9769 vs.\ 0.9849) [Cita: HyndmanKoehler2006].  Esto sugiere que la ganancia
principal de K-medias reside en la clasificación cualitativa de
regímenes más que en la precisión numérica puntual, aunque ambas
están correlacionadas.

**Tercero**, el modelo de referencia MS-AR **no convergió bajo los datos y el protocolo de validación de esta tesis**\footnote{Se utilizó la implementación del filtro de Hamilton en la librería 'statsmodels' v0.14 de Python, con 50 reinicios aleatorios y tolerancia $10^{-6}$. Se consideró no convergencia cuando el algoritmo EM excedió 1000 iteraciones sin alcanzar la tolerancia o produjo valores de verosimilitud no finitos.}, produciendo verosimilitudes no finitas en múltiples ventanas de validación. Es importante precisar que este resultado no representa una falla conceptual del modelo MS-AR, sino una limitación en la optimización numérica de su función de verosimilitud no convexa mediante el algoritmo EM bajo las características específicas de las series de precios regulados en Honduras. Esto subraya una ventaja práctica del enfoque NNLS: al formular la estimación como un problema convexo restringido, se evita la inestabilidad numérica característica de los métodos iterativos. El modelo TCRA-Markov convergió en el 100\% de los casos del estudio.

## Limitaciones y Supuestos

Es necesario reconocer las limitaciones del enfoque propuesto para
contextualizar los resultados:

**Homogeneidad temporal.**  El modelo asume que las
probabilidades de transición son constantes durante el período
2017--2025.  Dado que este intervalo incluye la pandemia de COVID-19,
cambios regulatorios y fluctuaciones del crudo WTI, la suposición es
potencialmente violada.  Una extensión natural sería particionar la
muestra y evaluar la estabilidad de $\hat{\mat{P}}$ en subperiodos
(prepandemia, pandemia, pospandemia).

**Propiedad de Markov de primer orden.**  La cadena asume
$P(S_t | S_{t-1}, \ldots, S_0) = P(S_t | S_{t-1})$.  Aunque
restrictiva, esta suposición se mitiga parcialmente por el uso de
$W \approx 52$ en el cálculo de $\alpha_t$: la definición del estado
ya incorpora implícitamente una memoria de largo plazo.  Pruebas de
dependencia de orden superior (test $\chi^2$ para Markov de segundo
orden) quedan como trabajo futuro.

**Modelo univariante.**  El marco no incorpora explícitamente
variables exógenas (precio WTI, tipo de cambio, decisiones
regulatorias).  Se asume que toda la información relevante está
contenida en la dinámica de precios pasada, lo cual es un supuesto
fuerte en un mercado regulado.

**Selección de la muestra.** El análisis empírico se limita a 4 combustibles 
hondureños seleccionados por disponibilidad histórica y consistencia de datos, por lo que la 
generalización de los parámetros óptimos a otros mercados requiere verificación empírica independiente.

**Selección del peso $w$.**  El parámetro $w$ en el sistema aumentado 
NNLS~\eqref{eq:nnls_aumentado_combustibles} controla el cumplimiento de las 
restricciones de suma unitaria.  En esta implementación se utilizó $w = 10^3$, 
valor que satisface la condición $w^2 \gg \|\mat{S}_0\|_F^2$ (donde $\|\mat{S}_0\|_F^2 \approx T$ para matrices indicadoras), 
garantizando que la penalización por violación de estocasticidad domine sobre el error de ajuste.  
La normalización final por columnas corrige desviaciones residuales de precisión de punto flotante, 
reduciendo la sensibilidad práctica a la elección exacta de $w$.  
Un análisis de sensibilidad formal queda como extensión futura.

**Robustez al split entrenamiento/prueba.**  Los resultados
se obtuvieron con un split fijo 80\%/20\%.  Para evaluar la
sensibilidad a la partición, se realizó validación cruzada temporal
con ventana expansiva: se entrenó sobre los primeros $T_0$ datos y
se pronosticó el bloque siguiente, incrementando $T_0$ en pasos de
26 semanas.  Las métricas promediadas sobre 5 splits muestran
desviaciones inferiores al 3\% respecto al split fijo, confirmando la
estabilidad de los hiperparámetros seleccionados.

**Incertidumbre en los pronósticos.**  Los pronósticos
puntuales de estado se acompañan de su respectivo cálculo analítico de la
varianza del pronóstico derivado en la Ecuación~\eqref{eq:varianza_pronostico_analitica}. 
Sin embargo, la propagación de esta incertidumbre hacia intervalos de confianza continuos 
sobre el nivel físico del precio en centavos de combustible es no lineal. 
Una implementación computacional mediante bootstrap paramétrico 
(remuestreo de las transiciones observadas) queda planteada como trabajo futuro 
para reportar bandas de predicción empíricas completas en los tableros de visualización.

## Resumen del Capítulo

Este capítulo ha presentado la validación empírica integral del marco
TCRA-Markov desarrollado a lo largo de esta tesis.  Al aplicar la
metodología completa, la cual abarca desde el operador ETCRAM y su teoría de
existencia y unicidad, pasando por la integración Markoviana
y la estimación NNLS, hasta la optimización de
hiperparámetros y el análisis espectral tratados en esta sección, a datos
reales de precios de combustibles en Honduras, se obtuvieron los
siguientes resultados:

**Sobre la sensibilidad paramétrica y la degeneración.**  Se
identificó y formalizó un fenómeno de degeneración del modelo para
ventanas $W$ excesivamente grandes, donde la serie $\alpha_t$ pierde
variabilidad y K-medias colapsa a un clúster efectivo.  La restricción
$W \le 52$, fundamentada en el ciclo anual del mercado, previene esta
degeneración y revela la dominancia de $W \approx 52$ en las series hondureñas analizadas, 
donde la predictibilidad de los precios está regida por tendencias de largo plazo.

**Sobre la identificación de regímenes.**  La discretización por
K-medias produce regímenes económicamente interpretables, con
centroides que cuantifican directamente las tasas de cambio promedio
por estado.  Los cuatro mercados analizados comparten un régimen
dominante de estabilidad ($\sim$47\% del período), pero difieren en
su complejidad: Regular y Diésel requieren 3 estados, Superior 4 y
Kerosene 5.

**Sobre las propiedades espectrales.**  Las matrices estimadas
son irreducibles, con brechas espectrales en el rango $[0.021, 0.040]$
y tiempos de mezcla de 25 a 47 semanas.  Estos valores confirman
matemáticamente la alta persistencia de los regímenes y explican la dificultad
diferencial para predecir el Kerosene ($\gamma = 0.021$, menor brecha)
frente a la Regular ($\gamma = 0.037$).

**Sobre la robustez frente a modelos de referencia.**  El marco demostró
superioridad estadísticamente significativa sobre la discretización
por cuantiles en exactitud ($p < 0.05$) para todas las series y en
RMSE ($p < 0.05$) para tres de cuatro series.  El modelo de referencia MS-AR no convergió bajo nuestro protocolo de validación y conjunto de datos, lo que resalta la ventaja práctica de la formulación NNLS convexa frente a la inestabilidad de la optimización iterativa EM en este tipo de series.

La eficiencia computacional del método, que escala linealmente en
$T$ para $K$ fijo, sugiere su aplicabilidad a datos de mayor
frecuencia (diarios o intradiarios) con paralelización de la
búsqueda en grilla.  El trabajo futuro debería abordar tres direcciones complementarias.  
La primera línea es la generalización a otros mercados centroamericanos, especialmente Guatemala 
y El Salvador, que poseen estructuras regulatorias distintas, 
permitiría evaluar si la dominancia de $W \approx 52$ es un artefacto del mercado hondureño 
o un patrón regional vinculado a ciclos anuales del crudo.  La segunda línea involucra la incorporación de 
variables exógenas (precio WTI, tipo de cambio Lempira/USD) como covariables en la etapa de 
discretización podría mejorar la identificación de regímenes en periodos de alta volatilidad externa.  
La tercera línea es la evaluación formal de la estabilidad temporal de $\hat{\mat{P}}$ mediante partición de la 
muestra en subperiodos (prepandemia 2017--2019, pandemia 2020--2021, pospandemia 2022--2025) 
permitiría cuantificar el grado en que el supuesto de homogeneidad temporal se sostiene en la práctica.

# Extensión No Lineal: Computación de Reservorio Estructurado

## Introducción y Motivación

El capítulo anterior demostró que el marco TCRA-Markov identifica
regímenes económicos interpretables y supera a los modelos de referencia evaluados.
La arquitectura, no obstante, es enteramente lineal: el operador $\alpha_t$
es un estimador de mínimos cuadrados ponderados, la discretización
K-medias opera sobre el espacio unidimensional de $\alpha_t$, y la
cadena de Markov modela transiciones lineales entre estados discretos.
La interpretabilidad y la estabilidad que esto proporciona tienen como contrapartida
una capacidad limitada para capturar dependencias no lineales.
Este capítulo presenta una extensión que preserva la estimación NNLS e
introduce capacidad de aproximación no lineal mediante
**Computación de Reservorio** (*Reservoir Computing*, RC)
[Cita: jaeger2001echo, lukosevicius2009reservoir]: la señal $\alpha_t$
se proyecta a un espacio de alta dimensión mediante una transformación no
lineal fija (el *reservorio*) y la capa de
lectura se estima con el mismo procedimiento NNLS.

Este capítulo formaliza el Computador de Reservorio
Estructurado Estocástico (SSRC, *Stochastically
Structured Reservoir Computer*) [Cita: BanegasVides2025] como operador
sobre espacios de señales, incluida la variante Leaky-ESN, y
demuestra que el marco TCRA-Markov del capítulo anterior es un caso
particular (degenerado) del SSRC, lo que establece una jerarquía formal
de modelos.  El capítulo verifica además que la capa de lectura NNLS
hereda las propiedades de existencia y unicidad ya establecidas, analiza
la estabilidad del pronóstico bajo perturbaciones con evaluación empírica
de las cotas teóricas, y compara ambos marcos sobre los mismos datos de
combustibles del Capítulo~[Ref: cap:regimenes_combustibles] mediante el
test de Diebold-Mariano para significancia estadística.

El estudio utiliza las mismas cuatro series semanales de precios de
combustibles en Honduras (Superior, Regular, Diésel y Kerosene) y el
mismo protocolo de ventanas deslizantes del capítulo anterior, con
conjunto de entrenamiento inicial de 312 semanas (enero 2017 --
diciembre 2022) y horizonte de pronóstico de 1 semana, para garantizar
la comparabilidad estricta de los resultados.

## Marco Teórico del Computador de Reservorio

### Sistemas Dinámicos con Entrada

Esta sección sitúa la computación de reservorio dentro del marco
general de los sistemas dinámicos discretos con entrada.

\begin{definition}[Sistema dinámico con entrada]

Sean $\set{U} \subseteq \mathbb{R}^d$ el espacio de entradas,
$\set{H} \subseteq \mathbb{R}^D$ el espacio de estados internos, y
$\set{Y} \subseteq \mathbb{R}^m$ el espacio de salidas.  Un
**sistema dinámico discreto con entrada** es una tripleta
$(\set{U}, F, G)$ donde

$$
\begin{aligned}
    F &: \set{H} \times \set{U} \to \set{H}, \\
    G &: \set{H} \to \set{Y}, 
\end{aligned}
$$

de modo que la evolución del sistema se rige por

$$

    \vect{h}_t = F(\vect{h}_{t-1}, \vect{u}_t), \qquad
    \vect{y}_t = G(\vect{h}_t),
    

$$

para $t = 1, 2, \ldots$, con condición inicial
$\vect{h}_0 \in \set{H}$.
\end{definition}

En este marco, la *computación* consiste en aprender el mapa
de lectura $G$ a partir de datos observados, manteniendo el mapa de
estado $F$ fijo.  Esta separación es la idea central de la
computación de reservorio.

### El Computador de Reservorio (*Echo State Network)*

\begin{definition}[Computador de Reservorio]

Un **Computador de Reservorio** (RC) de dimensión $D$ con
función de activación $\sigma: \mathbb{R} \to \mathbb{R}$ es un
sistema dinámico con entrada $(\set{U}, F_{\text{RC}}, G_{\text{RC}})$
donde:

    * El mapa de estado tiene la forma
    
$$

        F_{\text{RC}}(\vect{h}, \vect{u}) =
        \sigma\!\left(\mat{W}_{\text{in}}\,\vect{u} +
        \mat{W}_{\text{res}}\,\vect{h}\right),
        
    
$$

    con $\mat{W}_{\text{in}} \in \mathbb{R}^{D \times d}$ la
    **matriz de entrada**,
    $\mat{W}_{\text{res}} \in \mathbb{R}^{D \times D}$ la
    **matriz de reservorio**, y $\sigma$ aplicada
    componente a componente.
    * El mapa de lectura es lineal:
    
$$

        G_{\text{RC}}(\vect{h}) = \mat{W}_{\text{out}}\,\vect{h},
        
    
$$

    con $\mat{W}_{\text{out}} \in \mathbb{R}^{m \times D}$ la
    **matriz de lectura**.
    * Las matrices $\mat{W}_{\text{in}}$ y
    $\mat{W}_{\text{res}}$ son **fijas** (no se optimizan);
    solo $\mat{W}_{\text{out}}$ se estima a partir de datos.

\end{definition}

La ecuación de estado se expande recursivamente como

$$

    \vect{h}_t = \sigma\!\left(\mat{W}_{\text{in}}\,\vect{u}_t +
    \mat{W}_{\text{res}}\,
    \sigma\!\left(\mat{W}_{\text{in}}\,\vect{u}_{t-1} +
    \mat{W}_{\text{res}}\,\vect{h}_{t-2}\right)\right),
    

$$

lo que muestra que $\vect{h}_t$ es una función no lineal de toda la
historia de entradas $(\vect{u}_1, \ldots, \vect{u}_t)$ y de la
condición inicial $\vect{h}_0$.  El reservorio actúa como una
**memoria no lineal** que codifica el pasado de la señal en un
espacio de dimensión $D \gg d$.

### Diferenciación y Relación con Redes Neuronales Convencionales

Aunque la computación de reservorio comparte fundamentos con las redes neuronales recurrentes clásicas, posee distinciones teóricas y operacionales críticas. En las arquitecturas recurrentes tradicionales (como las RNN simples, LSTM o GRU), todos los parámetros de la red, incluyendo las matrices de pesos de entrada, recurrentes ocultas y de salida, se optimizan simultáneamente. Este proceso requiere algoritmos de retropropagación a través del tiempo basados en gradiente descendente estocástico, los cuales enfrentan problemas de inestabilidad numérica como el desvanecimiento o la explosión del gradiente, además de exigir una carga computacional elevada.

El computador de reservorio mantiene inmutables las matrices de entrada $\mat{W}_{\text{in}}$ y de reservorio $\mat{W}_{\text{res}}$ tras su generación aleatoria inicial. El reservorio oculto opera como un atractor dinámico no lineal y de alta dimensión que proyecta la señal de entrada al espacio de estados $\set{H}$, funcionando como una memoria asociativa. El entrenamiento se limita exclusivamente a la estimación de la matriz de lectura $\mat{W}_{\text{out}}$ en la capa de salida. 

En esta tesis, la capa de lectura se optimiza bajo la restricción de no negatividad (NNLS). Los detalles estructurales del modelo propuesto se resumen de la siguiente manera:

    * **Capa oculta (Reservorio):** Consiste en una única capa recurrente de dimensión $D$ (donde $D$ es un hiperparámetro optimizado en la búsqueda en grilla dentro del intervalo $[20, 150]$). Sus conexiones internas y de entrada permanecen fijas tras la inicialización.
    * **Función de activación:** Se utiliza la tangente hiperbólica ($\tanh$) en los nodos del reservorio para introducir no linealidad, mientras que la capa de salida emplea una función de activación identidad, permitiendo una lectura lineal directa.
    * **Mecanismo de optimización:** La estimación de los pesos de salida $\mat{W}_{\text{out}}$ se realiza a través de mínimos cuadrados no negativos (NNLS) mediante el algoritmo determinista de Lawson-Hanson. Esto evita la necesidad de configurar tasas de aprendizaje o épocas de entrenamiento, eliminando el riesgo de mínimos locales y garantizando la unicidad de la solución.

![Diagrama de la arquitectura del reservorio SSRC.]()
*Figura: Diagrama de la arquitectura del reservorio SSRC.* (fig:diagrama_esn_conceptual)

Como se ilustra en el diagrama conceptual de la Figura~[Ref: fig:diagrama_esn_conceptual], la señal temporal preprocesada $u_t = \alpha_t$ alimenta a un reservorio dinámico de dimensión $D$ a través de pesos fijos $\mat{W}_{\text{in}}$. Los estados internos $h_i$ evolucionan interconectados de forma dispersa mediante la matriz de reservorio $\mat{W}_{\text{res}}$ bajo la condición de radio espectral $\rho < 1$. Finalmente, los pesos de salida de la capa de lectura $\mat{W}_{\text{out}}$ se estiman de forma óptima mediante el estimador de mínimos cuadrados no negativos (NNLS).

### Variante con Integración de Fuga (*Leaky-ESN)*

En aplicaciones donde la señal de entrada exhibe alta persistencia,
como ocurre en el caso del mercado regulado de combustibles hondureños
(Capítulo~[Ref: cap:regimenes_combustibles]), resulta ventajoso
introducir un mecanismo de suavizado temporal explícito en la
ecuación de estado.

\begin{definition}[Computador de Reservorio con Integración de Fuga]

Un **Leaky-ESN** [Cita: jaeger2001echo] es un Computador de
Reservorio (Definición~[Ref: def:computador_reservorio]) cuyo mapa
de estado se modifica como:

$$

    \vect{h}_t = (1 - a)\,\vect{h}_{t-1} + a\,\sigma\!\left(
    \mat{W}_{\text{in}}\,\vect{u}_t +
    \mat{W}_{\text{res}}\,\vect{h}_{t-1}\right),
    

$$

donde $a \in (0, 1]$ es el **factor de fuga**
(*leak rate*). El caso $a = 1$ recupera la ESN estándar
de la Definición~[Ref: def:computador_reservorio]. Es crucial excluir el valor $a = 0$ de la formulación; de lo contrario, la Ecuación~\eqref{eq:leaky_esn} colapsaría a una dinámica recursiva degenerada $\vect{h}_t = \vect{h}_{t-1}$, provocando que el estado interno permaneciera congelado en su condición inicial $\vect{h}_0$ e inmune a las variaciones del estímulo externo $u_t$. Así, la restricción $a > 0$ es indispensable para garantizar el acoplamiento del reservorio con la señal temporal de entrada.
\end{definition}

El parámetro $a$ controla la *escala temporal efectiva* del
reservorio.  Valores $a \ll 1$ producen estados que cambian
lentamente, actuando como un filtro paso-bajo que enfatiza tendencias
de largo plazo sobre fluctuaciones semanales.  Formalmente, la
ecuación~\eqref{eq:leaky_esn} puede interpretarse como la
discretización de Euler de un sistema dinámico continuo con constante
de tiempo $\tau = 1/a$, de modo que $a = 0.1$ corresponde a
$\tau = 10$ pasos temporales de memoria efectiva.

\begin{remark}[Preservación de la ESP para Leaky-ESN]

La variante Leaky-ESN preserva la ESP bajo la misma condición
$\rho(\mat{W}_{\text{res}}) < 1$ del Teorema~[Ref: teo:esp_suficiente].
La demostración es análoga: la mezcla convexa
$(1-a)\vect{h}_{t-1} + a\,\sigma(\cdot)$ mantiene la contractibilidad
pues $(1-a) + a \cdot \|\mat{W}_{\text{res}}\| < 1$ cuando
$\|\mat{W}_{\text{res}}\| < 1$.
\end{remark}

### El Computador de Reservorio Estructurado Estocástico (SSRC)

\begin{definition}[Computador de Reservorio Estructurado Estocástico
(SSRC)]

Un **SSRC** [Cita: BanegasVides2025] es un Computador de Reservorio
con Integración de Fuga
(Definición~[Ref: def:leaky_esn]) con las siguientes
especializaciones:

    * La función de activación es $\sigma = \tanh$.
    * La matriz de entrada $\mat{W}_{\text{in}}$ se genera
    aleatoriamente con entradas i.i.d.\ de una distribución
    uniforme en $[-\omega, \omega]$ para algún $\omega > 0$.
    * La matriz de reservorio $\mat{W}_{\text{res}}$ se genera
    como una matriz dispersa aleatoria y se reescala a un radio
    espectral objetivo $\rho_{\text{res}}$:
    
$$

        \mat{W}_{\text{res}} \leftarrow
        \rho_{\text{res}} \cdot
        \frac{\mat{W}_{\text{res}}^{(0)}}
        {\rho(\mat{W}_{\text{res}}^{(0)})},
        
    
$$

    donde $\mat{W}_{\text{res}}^{(0)}$ es la matriz dispersa
    inicial y $\rho(\cdot)$ denota el radio espectral.
    * La matriz de lectura $\mat{W}_{\text{out}}$ se estima
    mediante NNLS.

Los hiperparámetros del SSRC son la tripleta
$(D, \rho_{\text{res}}, a)$: dimensión, radio espectral y factor
de fuga, respectivamente.
\end{definition}

### La Propiedad de Estado de Eco

La utilidad práctica de un computador de reservorio depende de que la
influencia de la condición inicial $\vect{h}_0$ se desvanezca
asintóticamente, de modo que la salida dependa únicamente de la
secuencia de entradas.  Esta condición se formaliza a continuación.

\begin{definition}[Propiedad de Estado de Eco (ESP)]

Sea $(\set{U}, F_{\text{RC}}, G_{\text{RC}})$ un Computador de
Reservorio.  Se dice que posee la **Propiedad de Estado de Eco**
(ESP) si, para toda secuencia de entradas acotada
$\{\vect{u}_t\}_{t \in \mathbb{Z}} \subset \set{U}$, existe una
única secuencia de estados $\{\vect{h}_t\}_{t \in \mathbb{Z}}
\subset \set{H}$ compatible con~\eqref{eq:mapa_estado_rc}, y esta
secuencia es independiente de la condición inicial.  Formalmente,
para cualesquiera $\vect{h}_0, \vect{h}_0' \in \set{H}$:

$$

    \lim_{t \to \infty} \left\|
    F^{(t)}(\vect{h}_0, \vect{u}_{1:t}) -
    F^{(t)}(\vect{h}_0', \vect{u}_{1:t})
    \right\| = 0,
    

$$

donde $F^{(t)}$ denota la composición $t$-ésima del mapa de estado.
\end{definition}

\begin{theorem}[Condición suficiente para la ESP]

Sea $(\set{U}, F_{\text{RC}}, G_{\text{RC}})$ un Computador de
Reservorio con $\sigma = \tanh$ y entradas acotadas
$\|\vect{u}_t\| \le U_{\max}$ para todo $t$.  Si

$$

    \rho(\mat{W}_{\text{res}}) < 1,
    

$$

entonces el sistema posee la ESP.
\end{theorem}

\begin{proof}
Consideremos dos trayectorias $\{\vect{h}_t\}$ y
$\{\vect{h}_t'\}$ generadas por la misma secuencia de entradas
pero con condiciones iniciales distintas $\vect{h}_0 \ne
\vect{h}_0'$.  Para $t \ge 1$:

$$
\begin{aligned}
    \vect{h}_t - \vect{h}_t' &=
    \sigma(\mat{W}_{\text{in}}\vect{u}_t +
    \mat{W}_{\text{res}}\vect{h}_{t-1}) -
    \sigma(\mat{W}_{\text{in}}\vect{u}_t +
    \mat{W}_{\text{res}}\vect{h}_{t-1}').
    
\end{aligned}
$$

Dado que $\tanh$ es Lipschitz continua con constante $L = 1$
(pues $|\tanh'(x)| = 1 - \tanh^2(x) \le 1$ para todo
$x \in \mathbb{R}$), se tiene componente a componente:

$$

    \|\vect{h}_t - \vect{h}_t'\| \le
    \|\mat{W}_{\text{res}}(\vect{h}_{t-1} - \vect{h}_{t-1}')\|
    \le \|\mat{W}_{\text{res}}\|\,
    \|\vect{h}_{t-1} - \vect{h}_{t-1}'\|,
    

$$

donde $\|\mat{W}_{\text{res}}\|$ denota la norma espectral
(mayor valor singular).  Aplicando inductivamente:

$$

    \|\vect{h}_t - \vect{h}_t'\| \le
    \|\mat{W}_{\text{res}}\|^t\,
    \|\vect{h}_0 - \vect{h}_0'\|.
    

$$

Por la fórmula de Gelfand, $\rho(\mat{W}_{\text{res}}) =
\lim_{n \to \infty} \|\mat{W}_{\text{res}}^n\|^{1/n}$, de modo
que la condición $\rho(\mat{W}_{\text{res}}) < 1$ implica la
existencia de $n_0 \in \mathbb{N}$ y $\delta \in (0,1)$ tales que
$\|\mat{W}_{\text{res}}^n\| \le \delta^n$ para todo $n \ge n_0$.
Reagrupando la iteración en bloques de tamaño $n_0$:

$$

    \|\vect{h}_{kn_0} - \vect{h}_{kn_0}'\| \le
    \delta^k\, C\, \|\vect{h}_0 - \vect{h}_0'\|,
    

$$

donde $C = \max_{0 \le j < n_0} \|\mat{W}_{\text{res}}^j\|$.
Como $\delta < 1$, el lado derecho converge a cero
exponencialmente conforme $k \to \infty$, lo que
establece~\eqref{eq:esp_formal}.
\end{proof}

\begin{remark}[Norma espectral frente a radio espectral]

Conviene separar dos condiciones que la literatura suele tratar como equivalentes. La cota de contracción paso a paso de la Ecuación~\eqref{eq:contraccion_paso} es estricta únicamente cuando $\|\mat{W}_{\text{res}}\|_2 < 1$, es decir, cuando el mayor valor singular de la matriz es menor que uno. Esa desigualdad sobre la norma espectral es la única garantía analítica rigurosa de contractividad uniforme ante cualquier señal de entrada acotada, y se sostiene pese a la no linealidad de la tangente hiperbólica. El radio espectral $\rho(\mat{W}_{\text{res}}) < 1$ es una condición más débil. Por la fórmula de Gelfand asegura contracción asintótica, no en cada paso, y solo coincide con la norma bajo hipótesis restrictivas como la simetría de $\mat{W}_{\text{res}}$. En las implementaciones se adopta $\rho < 1$ porque es verificable *a priori* y se impone con el reescalado~\eqref{eq:reescalado_espectral}, pero su validez descansa en el desempeño experimental y no constituye una cota general de estabilidad. Yildiz et al.\ [Cita: yildiz2012] mostraron, de hecho, que la ESP puede sostenerse incluso con $\rho(\mat{W}_{\text{res}}) > 1$ para ciertas clases de entradas.
\end{remark}

### Capacidad de Aproximación Universal

La justificación teórica para utilizar un reservorio como extractor
de características proviene del siguiente resultado clave.

\begin{theorem}[Aproximación universal por reservorios
{\cite[Teorema~3.1]{grigoryeva2018universal}}]

Sean $\set{U} \subset \mathbb{R}^d$ compacto y $\set{F}$ el
conjunto de funcionales causales e invariantes en el tiempo
$H: \set{U}^{\mathbb{Z}_{-}} \to \mathbb{R}^m$ que son continuos
respecto a la topología del producto.  Para todo $H \in \set{F}$ y
todo $\varepsilon > 0$, existen un Computador de Reservorio con
la ESP de dimensión $D$ suficientemente grande y una matriz de
lectura $\mat{W}_{\text{out}} \in \mathbb{R}^{m \times D}$ tales
que

$$

    \sup_{\{\vect{u}_t\} \in \set{U}^{\mathbb{Z}_{-}}}
    \|H(\ldots, \vect{u}_{t-1}, \vect{u}_t) -
    \mat{W}_{\text{out}}\,\vect{h}_t\| < \varepsilon.
    

$$

\end{theorem}

Este resultado establece que los computadores de reservorio con la
ESP son **aproximadores universales de funcionales sobre
secuencias**.  A diferencia de los teoremas de aproximación universal
para redes neuronales prealimentadas (que aproximan funciones estáticas),
el Teorema~[Ref: teo:aproximacion_universal] garantiza la aproximación
de mapas *con memoria*: funciones que dependen de toda la
historia pasada de la señal.  Para esta tesis, esto significa que
un reservorio de dimensión suficiente puede, en principio, capturar
cualquier dependencia no lineal entre el historial de precios
$\{v_1, \ldots, v_t\}$ y el precio futuro $v_{t+1}$, siempre que
esta dependencia sea continua y causal.

## TCRA-Markov como Caso Particular del SSRC

Esta sección establece el resultado central del capítulo: la
relación formal de inclusión entre el marco lineal del
Capítulo~[Ref: cap:regimenes_combustibles] y el marco no lineal del
SSRC.

### Reformulación del Marco Lineal

Para establecer la conexión, se reformula el flujo de trabajo
TCRA-Markov como un caso de la
Definición~[Ref: def:sistema_dinamico_entrada].

\begin{definition}[Sistema TCRA-Markov como sistema dinámico]

El marco TCRA-Markov con hiperparámetros $(W, \lambda, K)$ es un
sistema dinámico con entrada
$(\mathbb{R}, F_{\text{TM}}, G_{\text{TM}})$ donde:

    * La entrada es $u_t = \alpha_t \in \mathbb{R}$.
    * El espacio de estados es $\set{H}_{\text{TM}} =
    \{\vect{e}_1, \ldots, \vect{e}_K\} \subset \mathbb{R}^K$
    (los vectores canónicos).
    * El mapa de estado es
    $F_{\text{TM}}(\vect{h}, u) = \vect{e}_{Q(u)}$, donde
    $Q: \mathbb{R} \to \{1, \ldots, K\}$ es la función de
    asignación K-medias.
    * El mapa de lectura es
    $G_{\text{TM}}(\vect{h}) = \hat{\mat{P}}\,\vect{h}$,
    con $\hat{\mat{P}} \in \mathbb{R}^{K \times K}$ la matriz
    de transición estimada.

\end{definition}

Obsérvese que en este sistema: (a) el mapa de estado $F_{\text{TM}}$
no tiene memoria interna (depende solo de $u_t$, no de
$\vect{h}_{t-1}$); (b) la no linealidad está contenida únicamente
en la cuantización $Q$; y (c) el mapa de lectura es estrictamente
lineal.

### El Teorema de Inclusión

\begin{theorem}[TCRA-Markov como reservorio degenerado]

Sea $(\set{U}, F_{\text{RC}}, G_{\text{RC}})$ un Computador de
Reservorio de dimensión $D = K$ con:

    \item[(i)] Matriz de reservorio nula:
    $\mat{W}_{\text{res}} = \mat{0}_{K \times K}$.
    \item[(ii)] Función de activación identidad:
    $\sigma = \mathrm{id}$.
    \item[(iii)] Matriz de entrada definida por la cuantización
    K-medias: $\mat{W}_{\text{in}} = [\vect{e}_{Q(\cdot)}]$,
    es decir, $F_{\text{RC}}(\vect{h}, u) = \vect{e}_{Q(u)}$.
    \item[(iv)] Matriz de lectura
    $\mat{W}_{\text{out}} = \hat{\mat{P}}$.
    \item[(v)] Factor de fuga $a = 1$ (ESN estándar).

Entonces, para toda secuencia de entradas $\{u_t = \alpha_t\}$ y
toda condición inicial $\vect{h}_0$:

$$

    G_{\text{RC}}(\vect{h}_t) = G_{\text{TM}}(\vect{h}_t) =
    \hat{\mat{P}}\,\vect{e}_{S_t} \qquad \forall\, t \ge 1.
    

$$

El sistema satisface trivialmente la ESP: la convergencia
en~\eqref{eq:esp_formal} es instantánea (en un solo paso).
\end{theorem}

\begin{proof}
**Equivalencia de salidas.**  Bajo las condiciones
(i)--(iii) y (v), el mapa de estado~\eqref{eq:leaky_esn} se
reduce a:

$$

    \vect{h}_t = (1-1)\vect{h}_{t-1} + 1 \cdot \mathrm{id}\!\left(
    \mat{W}_{\text{in}}\,u_t + \mat{0}\cdot\vect{h}_{t-1}
    \right) = \vect{e}_{Q(u_t)} = \vect{e}_{S_t}.

$$

Con la condición (iv), la salida es
$G_{\text{RC}}(\vect{h}_t) =
\hat{\mat{P}}\,\vect{e}_{S_t}$, que coincide con
$G_{\text{TM}}$.

**ESP instantánea.**  Para cualesquiera
$\vect{h}_0, \vect{h}_0'$, a partir de $t = 1$:
$\vect{h}_1 = \vect{e}_{Q(u_1)} = \vect{h}_1'$,
independientemente de $\vect{h}_0$ y $\vect{h}_0'$.  Por tanto,
$\|\vect{h}_t - \vect{h}_t'\| = 0$ para todo $t \ge 1$.
\end{proof}

Aunque la prueba analítica del Teorema~[Ref: teo:inclusion] es matemáticamente concluyente y suficiente por sí misma, haciendo redundante cualquier verificación empírica, la Figura~[Ref: fig:ssrc_inclusion] se incluye con fines ilustrativos y didácticos. Su propósito es testificar que la consistencia algebraica se preserva estrictamente bajo codificación computacional sin discrepancias numéricas debidas al truncamiento en precisión de punto flotante.

![Verificación de la identidad algebraica entre TCRA-Markov y SSRC degenerado.](figures/02_inclusion_theorem.png)
*Figura: Verificación de la identidad algebraica entre TCRA-Markov y SSRC degenerado.* (fig:ssrc_inclusion)

La Figura~[Ref: fig:ssrc_inclusion] expone el contraste entre las salidas del modelo lineal de Markov y las del reservorio configurado con matriz nula y activación identidad. El error residual de exactamente $0.0$ en todas las series valida que la codificación del algoritmo implementado respeta de forma fidedigna la jerarquía analítica demostrada.

\begin{remark}[Jerarquía de modelos]

El marco TCRA-Markov del Capítulo~[Ref: cap:regimenes_combustibles] es un caso particular del SSRC (Definición~[Ref: def:ssrc]) obtenido al fijar $\mat{W}_{\text{res}} = \mat{0}$, $\sigma = \mathrm{id}$, $a = 1$ y $D = K$. La extensión a $\mat{W}_{\text{res}} \ne \mat{0}$, $\sigma = \tanh$, $a < 1$ y $D > K$ constituye una generalización estrictamente más expresiva. Esto es consecuencia directa del Teorema~[Ref: teo:inclusion] y del hecho de que el conjunto de funciones realizables por TCRA-Markov (composiciones lineales de indicadoras de pertenencia a clústeres) es un subconjunto propio del conjunto de funcionales continuos sobre $\set{U}^{\mathbb{Z}_{-}}$. Por el Teorema~[Ref: teo:aproximacion_universal], el SSRC puede aproximar cualquier funcional causal continuo sobre secuencias en la topología del producto, mientras que TCRA-Markov se limita a dependencias lineales sobre estados discretos de un paso.
\end{remark}

## Estimación de la Capa de Lectura vía NNLS

La separación entre reservorio fijo y lectura entrenable permite
reducción de la estimación de $\mat{W}_{\text{out}}$ a un problema de
mínimos cuadrados, que en el presente caso se resuelve bajo la restricción de no negatividad para asegurar que se optimiza siempre de forma positiva y no negativa, lo cual es axiomático para que el vector actúe como una suma convexa de propiedades físicas sin cancelar filtros.

### Formulación del Problema

Dada la secuencia de estados del reservorio
$\{\vect{h}_t\}_{t=W}^{T}$ generada por la señal TCRA
$\{\alpha_t\}$, y los valores objetivo
$\{y_t\}_{t=W+1}^{T}$, se busca la matriz de lectura que
minimiza el error de reconstrucción:

$$

    \hat{\mat{W}}_{\text{out}} = \argmin_{\mat{W} \ge \mat{0}}
    \sum_{t=W}^{T-1}
    \|y_{t+1} - \mat{W}\,\vect{h}_t\|^2.
    

$$

Es oportuno precisar que, aunque la estimación de la capa de lectura se formule como una relación lineal respecto al estado del reservorio $\vect{h}_t$ en la Ecuación~\eqref{eq:nnls_ssrc}, esto no implica asumir linealidad en el proceso físico o económico subyacente. La trayectoria $\vect{h}_t$ constituye una proyección no lineal recursiva de la señal de entrada $u_t$ hacia un espacio de alta dimensión, mediada por la función $\tanh$. Por consiguiente, resolver un problema de optimización lineal sobre $\vect{h}_t$ equivale a estimar un operador altamente no lineal sobre el espacio original de entradas, de forma homóloga a la linealización de frontera que efectúan los métodos de soporte vectorial o las funciones de base radial.

Definiendo la matriz de estados acumulados
$\mat{H} = [\vect{h}_W, \vect{h}_{W+1}, \ldots,
\vect{h}_{T-1}] \in \mathbb{R}^{D \times (T-W)}$ y el vector
de objetivos $\vect{y} = [y_{W+1}, \ldots, y_T]^T$, el
problema~\eqref{eq:nnls_ssrc} se reescribe en forma matricial:

$$

    \hat{\vect{w}} = \argmin_{\vect{w} \ge \vect{0}}
    \|\vect{y} - \mat{H}^T\vect{w}\|^2,
    

$$

donde $\vect{w} = \text{vec}(\mat{W}_{\text{out}}^T)$.

### Herencia de las Propiedades NNLS

\begin{theorem}[Existencia y unicidad de la capa de lectura NNLS]

Sea $\mat{H} \in \mathbb{R}^{D \times (T-W)}$ la matriz de estados
del reservorio.  Entonces:

    * **Existencia:** El
    problema~\eqref{eq:nnls_ssrc_matricial} admite al menos una
    solución.
    * **Unicidad:** Si $\mat{H}$ tiene rango completo por
    filas (i.e., $\mathrm{rango}(\mat{H}) = D$), la solución es
    única.

\end{theorem}

\begin{proof}
**(1) Existencia.**  El cono factible
$\set{C} = \{\vect{w} \in \mathbb{R}^{Dm} : \vect{w} \ge \vect{0}\}$
es un cono convexo cerrado no vacío ($\vect{0} \in \set{C}$).  La
función objetivo $f(\vect{w}) = \|\vect{y} - \mat{H}^T\vect{w}\|^2$
es continua y coerciva en $\set{C}$.  Por el teorema de
Weierstrass, $f$ alcanza su mínimo en $\set{C}$.

**(2) Unicidad.**  Si $\mathrm{rango}(\mat{H}) = D$, entonces
$\mat{H}\mat{H}^T$ es definida positiva, lo que implica que
$f(\vect{w}) = \vect{w}^T(\mat{H}\mat{H}^T)\vect{w} -
2\vect{y}^T\mat{H}^T\vect{w} + \|\vect{y}\|^2$ es estrictamente
convexa en $\vect{w}$.  Un funcional estrictamente convexo sobre un
conjunto convexo tiene a lo sumo un minimizador.  Combinado con la
existencia del punto (1), la solución es única.
\end{proof}

\begin{remark}[Condición de rango y condicionamiento numérico]

La condición $\mathrm{rango}(\mat{H}) = D$ requiere $T - W \ge D$ y
que los estados del reservorio sean linealmente independientes.  La no
linealidad de $\tanh$ y la aleatoriedad de
$\mat{W}_{\text{in}}, \mat{W}_{\text{res}}$ garantizan que esta
condición se satisface genéricamente cuando $T - W > D$.  Sin embargo,
la *unicidad teórica* no implica *estabilidad numérica*:
si el número de condición $\kappa(\mat{H}) = \sigma_{\max}/\sigma_{\min}$
es grande, la solución NNLS puede ser sensible a perturbaciones en
los datos.  En la práctica, la restricción de no negatividad del NNLS
actúa como una regularización implícita que mitiga parcialmente este
efecto, y el promediado sobre múltiples realizaciones
(*ensemble*) lo reduce aún más.  El análisis empírico
del condicionamiento se presenta en la
Sección~[Ref: subsec:verificaciones_teoricas].
\end{remark}

\begin{remark}[Alcance de la no negatividad del estimador]

Conviene precisar hasta dónde llega la restricción $\vect{w} \ge \vect{0}$. El estimador NNLS garantiza que los coeficientes de la capa de lectura sean no negativos, pero no garantiza que la predicción lo sea. Los estados internos se calculan con la tangente hiperbólica, de modo que cada componente de $\vect{h}_t$ vive en el intervalo abierto $(-1,1)$ y puede ser negativa. La predicción $\hat{y}_t = \vect{w}^\top \vect{h}_t$ admite, por tanto, valores negativos en teoría, aun cuando $\vect{w} \ge \vect{0}$. Que en las simulaciones sobre precios de combustibles nunca aparezcan valores negativos responde a la escala positiva de los datos observados y a la inercia del operador TCRA de entrada, no a una garantía analítica de la formulación.
\end{remark}

## Arquitectura TCRA-SSRC

La Figura~[Ref: fig:ssrc_architecture] presenta la comparación
esquemática de ambas arquitecturas; los pasos 1--2
(extracción TCRA) y 4 (estimación NNLS) son compartidos; la
diferencia reside exclusivamente en el paso 3: discretización K-medias
en TCRA-Markov vs.\ proyección al reservorio recurrente en
TCRA-SSRC.

![Comparación esquemática de las arquitecturas TCRA-Markov y TCRA-SSRC.](figures/01_architecture_diagram.png)
*Figura: Comparación esquemática de las arquitecturas TCRA-Markov y TCRA-SSRC.* (fig:ssrc_architecture)

Como se detalla en el diagrama de la Figura~[Ref: fig:ssrc_architecture], 
ambos marcos tecnológicos comparten la fase inicial de extracción de 
características y la fase final de estimación por mínimos cuadrados. 
Para facilitar la lectura y comprensión del flujo metodológico ilustrado en la figura, a continuación se detalla el procesamiento paso a paso de la información a través de las cuatro etapas principales:

    * **Paso 1 (Señal Temporal de Entrada):** Se parte de la serie temporal original de precios de combustibles $v_t$, la cual representa el insumo base del sistema.
    * **Paso 2 (Extracción del Operador TCRA):** Se aplica el operador de tasa de cambio relativo acumulada (TCRA) con ventana $W$ y factor de atenuación $\lambda$ para generar la serie de tasas de cambio $\alpha_t$. Este paso actúa como un preprocesamiento físico-financiero que estacionariza la serie y extrae su tendencia local.
    * **Paso 3 (Bifurcación del Espacio de Estados):** 
    
        * En el **modelo híbrido TCRA-Markov**, la serie continua $\alpha_t$ se discretiza en $K$ estados discretos $\{s_1, \dots, s_K\}$ mediante K-medias, y las transiciones se modelan en una matriz estocástica discreta.
        * En el **modelo SSRC**, la serie continua $\alpha_t$ se alimenta como entrada $u_t$ al reservorio dinámico de dimensión $D$. Los estados internos $\vect{h}_t$ evolucionan en un espacio continuo no lineal mediante la ecuación del Leaky-ESN, preservando una memoria analógica de largo alcance.
    
    * **Paso 4 (Capa de Lectura y Predicción):** 
    
        * En TCRA-Markov, la predicción se realiza mediante la multiplicación de la distribución de estados por la matriz de transición estimada vía NNLS.
        * En TCRA-SSRC, la predicción de la tasa de cambio futura $\hat{y}_{t+1}$ se calcula mediante la combinación lineal de los estados internos del reservorio $\vect{h}_t$ multiplicados por los pesos de salida $\mat{W}_{\text{out}}$, los cuales se estiman también por NNLS (Lawson-Hanson).
    

La distinción radica en el procesamiento de la información en el espacio de estados, 
donde el modelo SSRC sustituye la discretización por clústeres por una proyección
 a un espacio continuo recurrente de alta dimensión, permitiendo capturar 
 dependencias temporales no lineales inaccesibles para el modelo lineal.

\begin{definition}[Marco TCRA-SSRC]

El **marco TCRA-SSRC** para pronóstico de series temporales
consiste en el flujo secuencial:

    * **Extracción TCRA:** Dada la serie $\{v_t\}$, se
    computa la señal $\alpha_t = \alpha_t(W, \lambda)$ según la
    ecuación~\eqref{eq:alpha_t_combustibles} del
    Capítulo~[Ref: cap:regimenes_combustibles].
    * **Proyección al reservorio:** Se define la entrada
    $u_t = \alpha_t \in \mathbb{R}$ y se propaga mediante la
    ecuación Leaky-ESN~\eqref{eq:leaky_esn}, con $\vect{h}_0 = \vect{0}$
    y los primeros $W_{\text{wash}}$ pasos descartados (fase de calentamiento
    o *washout*) con el fin de mitigar la influencia del estado inicial
    arbitrario $\vect{h}_0$ y garantizar que el reservorio haya convergido
    a su comportamiento estable de eco.
    * **Estimación de lectura:** Se resuelve el problema
    NNLS~\eqref{eq:nnls_ssrc} para obtener
    $\hat{\mat{W}}_{\text{out}}$.
    * **Pronóstico:** El valor predicho es
    $\hat{y}_{t+1} = \hat{\mat{W}}_{\text{out}}\,\vect{h}_t$.

Los hiperparámetros del marco son: $(W, \lambda)$ heredados de TCRA;
$(D, \rho_{\text{res}}, a, W_{\text{wash}})$ propios del reservorio.
\end{definition}

\begin{proposition}[Consistencia con el caso lineal]

Con los hiperparámetros del reservorio fijados en
$D = K$, $\mat{W}_{\text{res}} = \mat{0}$, $\sigma = \mathrm{id}$,
$a = 1$ y $W_{\text{wash}} = 0$, el marco TCRA-SSRC se reduce
exactamente al marco TCRA-Markov del
Capítulo~[Ref: cap:regimenes_combustibles].
\end{proposition}

\begin{proof}
Consecuencia directa del Teorema~[Ref: teo:inclusion].
\end{proof}

## Propiedades de Estabilidad del Pronóstico

\begin{proposition}[Acotación del error de pronóstico para Leaky-ESN]

Sea $\hat{y}_{t+1} = \hat{\mat{W}}_{\text{out}}\,\vect{h}_t$ el pronóstico del SSRC bajo la dinámica Leaky-ESN. Si la norma de la matriz de conexiones recurrentes satisface $\|\mat{W}_{\text{res}}\| < 1$ (lo cual garantiza asintóticamente la ESP), entonces para toda perturbación acotada en la entrada con $\|\delta\vect{u}_t\| \le \epsilon$ para todo $t$, la perturbación inducida en la salida del sistema satisface la cota:

$$

    \|\delta \hat{y}_{t+1}\| \le
    \|\hat{\mat{W}}_{\text{out}}\|\,
    \frac{\|\mat{W}_{\text{in}}\|\,\epsilon}
    {1 - \|\mat{W}_{\text{res}}\|}.
    

$$

Notablemente, el factor de fuga $a$ se cancela en la cota asintótica de estabilidad.
\end{proposition}

\begin{proof}
Sea $\{\vect{h}_t\}$ la trayectoria nominal del reservorio y $\{\vect{h}'_t = \vect{h}_t + \delta\vect{h}_t\}$ la trayectoria perturbada por una variación persistente en la entrada $\{\vect{u}'_t = \vect{u}_t + \delta\vect{u}_t\}$. La dinámica del Leaky-ESN establece que:

$$

    \vect{h}_t + \delta\vect{h}_t = (1-a)(\vect{h}_{t-1} + \delta\vect{h}_{t-1}) + a\tanh\left(\mat{W}_{\text{in}}(\vect{u}_t + \delta\vect{u}_t) + \mat{W}_{\text{res}}(\vect{h}_{t-1} + \delta\vect{h}_{t-1})\right).

$$

Restando la ecuación nominal $\vect{h}_t = (1-a)\vect{h}_{t-1} + a\tanh(\mat{W}_{\text{in}}\vect{u}_t + \mat{W}_{\text{res}}\vect{h}_{t-1})$ y aplicando la desigualdad triangular:

$$

    \|\delta\vect{h}_t\| \le (1-a)\|\delta\vect{h}_{t-1}\| + a\left\|\tanh\left(\mat{W}_{\text{in}}(\vect{u}_t + \delta\vect{u}_t) + \mat{W}_{\text{res}}(\vect{h}_{t-1} + \delta\vect{h}_{t-1})\right) - \tanh\left(\mat{W}_{\text{in}}\vect{u}_t + \mat{W}_{\text{res}}\vect{h}_{t-1}\right)\right\|.

$$

Por la Lipschitz continuidad de la función tangente hiperbólica con constante $L = 1$:

$$
\begin{aligned}
    \|\delta\vect{h}_t\| &\le (1-a)\|\delta\vect{h}_{t-1}\| + a\left\|\mat{W}_{\text{in}}\delta\vect{u}_t + \mat{W}_{\text{res}}\delta\vect{h}_{t-1}\right\| \\
    &\le (1-a)\|\delta\vect{h}_{t-1}\| + a\|\mat{W}_{\text{in}}\|\,\|\delta\vect{u}_t\| + a\|\mat{W}_{\text{res}}\|\,\|\delta\vect{h}_{t-1}\|.
\end{aligned}
$$

Reagrupando los términos respecto al estado de perturbación anterior $\|\delta\vect{h}_{t-1}\|$:

$$

    \|\delta\vect{h}_t\| \le a\|\mat{W}_{\text{in}}\|\,\|\delta\vect{u}_t\| + \left(1 - a + a\|\mat{W}_{\text{res}}\|\right)\|\delta\vect{h}_{t-1}\|.

$$

Para que la perturbación se disipe y la órbita sea de largo plazo estable, se requiere que el factor recursivo sea menor que la unidad, es decir, $1 - a + a\|\mat{W}_{\text{res}}\| < 1 \iff a\|\mat{W}_{\text{res}}\| < a \iff \|\mat{W}_{\text{res}}\| < 1$ (dado que $a > 0$).
Bajo esta condición, y asumiendo una perturbación de entrada acotada uniformemente por $\|\delta\vect{u}_t\| \le \epsilon$, iteramos la desigualdad desde un estado inicial no perturbado:

$$

    \|\delta\vect{h}_t\| \le a\|\mat{W}_{\text{in}}\|\epsilon \sum_{k=0}^{t-1} \left(1 - a + a\|\mat{W}_{\text{res}}\|\right)^k.

$$

En el límite asintótico $t \to \infty$, evaluando la serie geométrica convergente:

$$

    \sup_t \|\delta\vect{h}_t\| \le \frac{a\|\mat{W}_{\text{in}}\|\epsilon}{1 - (1 - a + a\|\mat{W}_{\text{res}}\|)} = \frac{a\|\mat{W}_{\text{in}}\|\epsilon}{a(1 - \|\mat{W}_{\text{res}}\|)} = \frac{\|\mat{W}_{\text{in}}\|\epsilon}{1 - \|\mat{W}_{\text{res}}\|}.

$$

Donde se aprecia la cancelación exacta del factor de fuga $a$. La cota sobre la salida perturbada sigue directamente por linealidad del operador de lectura:

$$

    \|\delta\hat{y}_{t+1}\| = \|\hat{\mat{W}}_{\text{out}}\delta\vect{h}_t\| \le \|\hat{\mat{W}}_{\text{out}}\|\,\|\delta\vect{h}_t\| \le \|\hat{\mat{W}}_{\text{out}}\|\,\frac{\|\mat{W}_{\text{in}}\|\,\epsilon}{1 - \|\mat{W}_{\text{res}}\|}.

$$

\end{proof}

\begin{remark}[Conservadurismo de la cota]

La cota~\eqref{eq:acotacion_perturbacion} es una *cota en el
peor caso* que puede ser significativamente conservadora.  En la
práctica, $\|\hat{\mat{W}}_{\text{out}}\|$ depende fuertemente
del condicionamiento de $\mat{H}$: números de condición altos
producen normas $\|\hat{\mat{W}}_{\text{out}}\|$ grandes,
inflando la cota sin que esto se traduzca necesariamente en
inestabilidad observable.  La restricción de no negatividad del NNLS
restringe $\hat{\mat{W}}_{\text{out}}$ al ortante positivo,
proporcionando una forma de regularización implícita que la cota
teórica no captura.  El análisis empírico de la
Sección~[Ref: subsec:sensibilidad_hiperparametros] muestra que la
variabilidad real del pronóstico es órdenes de magnitud menor que
la cota teórica.
\end{remark}

## Aplicación Comparativa

### Configuración Experimental

Los hiperparámetros TCRA se fijaron en los valores óptimos identificados en la Tabla~[Ref: tab:hiperparametros_optimos] del capítulo anterior. Para el reservorio, se evaluó una búsqueda exhaustiva en grilla sobre tres hiperparámetros clave:

$$
\begin{aligned}
    D &\in \{20, 30, 40, 50, 60, 75, 100, 150\}, \nonumber\\
    \rho_{\text{res}} &\in \{0.70, 0.80, 0.85, 0.90, 0.95, 0.99\}, \quad
    a \in \{0.1, 0.3, 0.5, 0.7, 1.0\},
    
\end{aligned}
$$

lo que representa un espacio de búsqueda acotado de $8 \times 6 \times 5 = 240$ combinaciones por serie. Cabe destacar que, aunque la búsqueda exhaustiva en grilla suele ser computacionalmente prohibitiva en el entrenamiento de redes recurrentes estándar entrenadas por retropropagación a través del tiempo, la naturaleza lineal del estimador de salida (NNLS) en el SSRC permite evaluar cada combinación en milisegundos. Específicamente, el proceso completo de búsqueda en grilla (evaluando las 240 celdas multiplicadas por 5 realizaciones estocásticas por celda para promediar el ruido de inicialización) requirió un tiempo de cómputo promedio de únicamente $4.2$ segundos por serie de combustible en un computador portátil convencional con procesador Intel i7 y 16~GB de RAM, sin necesidad de aceleración por GPU.

Para la búsqueda se fijaron $\omega = 1.0$ para la escala de $\mat{W}_{\text{in}}$, una densidad de conectividad del 10\% (90\% de dispersión) en la matriz $\mat{W}_{\text{res}}$, y $W_{\text{wash}} = 50$ pasos de calentamiento. La configuración óptima $(D^*, \rho^*, a^*)$ fue elegida basándose en el RMSE promedio en la fase de validación cruzada. Finalmente, para atenuar la dependencia de la inicialización aleatoria de las matrices estocásticas, la estimación definitiva de los parámetros de lectura $\hat{\mat{W}}_{\text{out}}$ y el cálculo de los pronósticos de la serie se realizó mediante un ensemble de 30 realizaciones independientes. Este ensemble permite obtener estimaciones robustas de la media predictiva y la variabilidad, actuando además como mecanismo de validación de robustez al cuantificar la sensibilidad del modelo a la inicialización aleatoria del reservorio.

### Verificaciones Teóricas

Antes de reportar los resultados predictivos, se verificaron las condiciones teóricas establecidas en las secciones precedentes. La Tabla~[Ref: tab:verificaciones_teoricas] resume los resultados cuantitativos obtenidos bajo la configuración óptima para cada serie de combustible. En dicha tabla:

    * $D^*$ denota la dimensión óptima del reservorio (número de variables de estado recurrentes).
    * $\rho^*$ es el radio espectral óptimo de la matriz de conectividad interna $\mat{W}_{\text{res}}$.
    * **ESP** indica el cumplimiento binario de la propiedad de estado de eco, verificando si $\rho^* < 1$.
    * $\mathrm{rango}(\mat{H})$ representa el rango de la matriz de estados históricos $\mat{H} \in \mathbb{R}^{D \times (T - W - W_{\text{wash}})}$.
    * $T_{\text{eff}}$ corresponde al tamaño efectivo del conjunto de entrenamiento tras descartar el washout ($T - W - W_{\text{wash}}$).
    * $\kappa(\mat{H})$ es el número de condición de la matriz de estados $\mat{H}$, definido como el cociente entre su máximo y mínimo valor singular, $\sigma_{\max}(\mat{H}) / \sigma_{\min}(\mat{H})$, que califica la estabilidad numérica de la estimación.

*Tabla: Verificación de las condiciones teóricas del SSRC.* (tab:verificaciones_teoricas)

| lcccccc@{}}
  
  **Serie** | $D^*$ | $\rho^*$ | **ESP** | $\mathrm{rango}(\mat{H})$ | $T_{\text{eff}}$ | $\kappa(\mat{H})$ |
| --- | --- | --- | --- | --- | --- | --- |
| Regular | 150 | 0.85 | Sí | 150 | 340 | $1.47 \times 10^{9}$ |
| Superior | 150 | 0.70 | Sí | 150 | 338 | $5.60 \times 10^{10}$ |
| Diésel | 150 | 0.80 | Sí | 150 | 340 | $3.28 \times 10^{9}$ |
| Kerosene | 60 | 0.70 | Sí | 60 | 338 | $1.14 \times 10^{9}$ |

Las cuatro configuraciones satisfacen la ESP (todos los
$\rho^* < 1$) y la condición de rango completo
($\mathrm{rango}(\mat{H}) = D^*$ en todos los casos).

![Autovalores de $\mat{W](figures/04_eigenvalues_complex_plane.png)
*Figura: Autovalores de $\mat{W* (fig:ssrc_eigenvalues)

La Figura~[Ref: fig:ssrc_eigenvalues] muestra los autovalores de 
$\mat{W}_{\text{res}}$ para la configuración óptima de cada serie. 
El círculo negro punteado representa el círculo unitario ($|z| = 1$), 
mientras que el círculo rojo sólido indica el radio espectral objetivo 
$\rho^*$. Se verifica que todos los autovalores se encuentran estrictamente 
dentro del círculo unitario (línea punteada), cumpliendo con la condición suficiente para la 
ESP detallada en el Teorema~[Ref: teo:esp_suficiente]. Los radios espectrales 
óptimos moderados ($\rho^* \in [0.70, 0.85]$) confirman la condición $\rho < 1$ e indican que el reservorio no 
requiere de memoria máxima para capturar la dinámica de los combustibles.

Los números de condición $\kappa(\mat{H})$ merecen discusión
explícita. Los valores observados, que se sitúan en el orden de $10^9$ a
$10^{10}$, indican que la matriz $\mat{H}\mat{H}^T$ está
pobremente condicionada en sentido numérico.  Esto es una
consecuencia directa de la dimensión $D = 150$ combinada con la
naturaleza de la señal $\alpha_t$, que tiene variabilidad reducida
($|\alpha_t| < 0.02$ típicamente).  Sin embargo, tres factores
mitigan este condicionamiento en la práctica: (i) la restricción
$\vect{w} \ge \vect{0}$ del NNLS actúa como una regularización
implícita, restringiendo la solución al ortante positivo y
eliminando las direcciones negativas que amplifican el error;
(ii) el ensemble de 30 realizaciones promedia sobre diferentes
matrices $\mat{W}_{\text{in}}$ y $\mat{W}_{\text{res}}$, cada
una con condicionamiento diferente; y (iii) los resultados
predictivos (Sección~[Ref: subsec:ssrc_resultados]) muestran
variabilidad baja entre realizaciones, lo que muestra estabilidad
práctica a pesar del condicionamiento teórico adverso.

![Convergencia del RMSE según el período de calentamiento $W_{\text{wash](figures/11_washout_convergence.png)
*Figura: Convergencia del RMSE según el período de calentamiento $W_{\text{wash* (fig:ssrc_washout)

Como se ilustra en la Figura~[Ref: fig:ssrc_washout], el RMSE se estabiliza 
para las cuatro series a partir de un período de $W_{\text{wash}} 
\approx 10$--$20$ pasos, confirmando que la ESP disipa rápidamente la influencia de la condición inicial. 
La línea amarilla punteada señala el valor utilizado de $W_{\text{wash}} = 50$,
el cual se encuentra dentro de la región de convergencia estable, reflejando el
decaimiento exponencial rápido asociado a radios espectrales moderados ($\rho^* \le 0.85$).
El argumento admite una cota cuantitativa directa. La memoria del transitorio decae como $\rho(\mat{W}_{\text{res}})^t$, de modo que para $\rho^* \approx 0.85$ la influencia residual de la condición inicial tras 50 pasos es del orden de $0.85^{50} \approx 2.9 \times 10^{-4}$, esto es, menos del $0.03\%$ de la perturbación de partida. A partir de ese punto el estado interno depende casi por completo de la historia del estímulo de entrada y no de la inicialización arbitraria del reservorio.
Sin embargo, en series temporales relativamente cortas (como las analizadas aquí, que cuentan con alrededor de 400 observaciones semanales), existe un compromiso metodológico crítico: un valor excesivamente alto de $W_{\text{wash}}$ reduce el tamaño efectivo del conjunto de entrenamiento disponible ($T_{\text{eff}} = T - W - W_{\text{wash}}$) para estimar los pesos de salida $\mat{W}_{\text{out}}$, lo que puede redundar en una ligera pérdida de rendimiento predictivo al restringir la muestra útil de calibración. Por consiguiente, $W_{\text{wash}} = 50$ constituye un compromiso óptimo que prioriza la solidez de la disipación del estado transitorio inicial sin comprometer la suficiencia muestral del entrenamiento.

### Optimización de Hiperparámetros

La Figura~[Ref: fig:ssrc_heatmap] presenta los resultados de la
búsqueda en grilla como mapas de calor del RMSE medio en el plano
$(D, \rho)$, donde para cada celda se reporta el mejor valor de
$a$ encontrado.

![Búsqueda en grilla del SSRC sobre el plano $(D, \rho)$.](figures/03_grid_heatmap.png)
*Figura: Búsqueda en grilla del SSRC sobre el plano $(D, \rho)$.* (fig:ssrc_heatmap)

La Figura~[Ref: fig:ssrc_heatmap] ilustra la búsqueda en grilla del SSRC sobre el plano $(D, \rho)$. Los mapas de calor muestran el RMSE medio para cada combinación y el mejor factor de fuga $a^*$ por celda, con el recuadro negro indicando la configuración óptima. Se observa que para Regular, Superior y Diésel el óptimo se alcanza en $D = 150$, mientras que para Kerosene ocurre en $D = 60$. Los radios espectrales óptimos son moderados y la superficie de error es suave, manteniendo el RMSE del SSRC significativamente por debajo del modelo Markoviano en todos los paneles.

La Tabla~[Ref: tab:hiperparametros_ssrc] detalla los hiperparámetros óptimos identificados para cada serie de combustible ($D^*$, $\rho^*$ y $a^*$) y presenta la comparación entre los resolvedores NNLS y Ridge.

*Tabla: Hiperparámetros óptimos del SSRC por serie de combustible.* (tab:hiperparametros_ssrc)

| lcccccc@{}}

**Serie** | $D^*$ | $\rho^*$ | $a^*$ | **NNLS RMSE** | **Ridge RMSE** | $\Delta_{\text{solver}}$ |
| --- | --- | --- | --- | --- | --- | --- |
| Regular | 150 | 0.85 | 0.1 | 0.6595 | 0.6741 | $-2.16\%$ |
| Superior | 150 | 0.70 | 0.3 | 0.7810 | 0.7885 | $-0.95\%$ |
| Diésel | 150 | 0.80 | 0.1 | 0.8299 | 0.8443 | $-1.70\%$ |
| Kerosene | 60 | 0.70 | 1.0 | 0.9156 | 0.9173 | $-0.17\%$ |

De los resultados de optimización resumidos en la Tabla~[Ref: tab:hiperparametros_ssrc], se observa que la restricción de no negatividad supera a la regresión Ridge clásica en todas las series. Específicamente, se extraen las siguientes observaciones fundamentales:
1. **Inercia temporal y volatilidad:** Los valores óptimos $a^* = 0.1$ para Regular y Diésel indican que estas series se benefician de una inercia temporal fuerte en el reservorio: el 90\% del estado previo se preserva en cada paso temporal. Esto actúa como un filtro de suavizado adaptativo muy consistente con la menor volatilidad semanal de estos mercados regulados por el Estado. En contraste, para el Kerosene el óptimo es $a^* = 1.0$ (lo que corresponde a un ESN estándar sin factor de fuga), lo cual es coherente con su mayor volatilidad y complejidad intrínseca de 5 regímenes dinámicos identificados en el capítulo anterior.
2. **Desempeño y Benchmarks de Solvers (NNLS vs. Ridge):** En la Tabla~[Ref: tab:hiperparametros_ssrc], la columna **Ridge RMSE** representa el desempeño del reservorio estimado mediante regresión Ridge clásica (regularización $L_2$ sin restricciones de signo en los pesos de salida). Este modelo actúa como el principal benchmark lineal alternativo para aislar y medir el beneficio específico de la restricción de no negatividad. En las cuatro series, NNLS supera consistentemente a Ridge Regression. Esto valida empíricamente que la restricción de no negatividad ($\mat{W}_{\text{out}} \ge \mat{0}$), al forzar que la predicción de salida sea una combinación estrictamente positiva de los estados internos (actuando como filtros no lineales), aporta una regularización geométrica implícita sumamente útil, previniendo cancelaciones mutuas destructivas entre neuronas y mejorando la generalización fuera de muestra.
3. **Eficiencia y Viabilidad Computacional:** Si bien la búsqueda en grilla exhaustiva de hiperparámetros (que evalúa 240 combinaciones) es ineficiente en términos algorítmicos generales, su viabilidad práctica en este trabajo radica en que el reservorio SSRC prescinde por completo de la retropropagación de gradientes. La estimación de la capa de lectura se reduce a un problema convexo lineal directo de NNLS (Lawson-Hanson), el cual se resuelve de forma determinista en milisegundos. Empíricamente, la evaluación completa de la grilla para las cuatro series (con 5 realizaciones estocásticas por celda para promediar el ruido de inicialización) requirió un coste temporal de apenas $4.2$ segundos en hardware estándar de escritorio. No obstante, para aplicaciones industriales o el monitoreo simultáneo de cientos de series temporales, la búsqueda exhaustiva resulta limitante, por lo que se recomienda la incorporación de Optimización Bayesiana o algoritmos evolutivos en el trabajo futuro para buscar hiperparámetros de forma inteligente.

Las Figuras~[Ref: fig:ssrc_sens_D] y~[Ref: fig:ssrc_sens_rho]
presentan el análisis de sensibilidad marginal a los hiperparámetros
$D$ y $\rho$, respectivamente.

![Sensibilidad del RMSE a la dimensión del reservorio $D$.](figures/08_sensitivity_D.png)
*Figura: Sensibilidad del RMSE a la dimensión del reservorio $D$.* (fig:ssrc_sens_D)

La sensibilidad del RMSE respecto a la dimensión $D$ se detalla en la Figura~[Ref: fig:ssrc_sens_D]. Las curvas, promediadas sobre todos los valores de $\rho$, resultan esencialmente planas, con variaciones mínimas entre $D = 20$ y $D = 150$. Este comportamiento indica que la complejidad no lineal del proceso es baja, permitiendo que incluso dimensiones modestas ($D = 20$) capturen la mayor parte de la estructura, con ganancias de apenas el 0.3\% al aumentar la dimensión a los niveles máximos evaluados.

Específicamente, en lo que respecta a la diferencia en la dimensión óptima entre las series (donde Regular, Superior y Diésel seleccionan $D^* = 150$, mientras que Kerosene selecciona $D^* = 60$), esta discrepancia se encuentra íntimamente acoplada con el factor de fuga óptimo $a^*$. Para las tres primeras series, el factor de fuga es bajo ($a^* \le 0.3$), lo cual implica que el reservorio exhibe una inercia dinámica muy alta y la información fluye con lentitud. En este régimen de mezcla lenta, se requiere un espacio de estados de mayor dimensión ($D = 150$) para evitar colisiones de representación y retener suficientes proyecciones independientes a lo largo del tiempo. En contraste, para el Kerosene la dinámica se actualiza por completo en cada paso ($a^* = 1.0$, sin inercia), lo que reduce drásticamente la redundancia temporal de los estados internos; en consecuencia, una dimensión menor ($D = 60$) es óptima y suficiente para modelar los 5 regímenes dinámicos de volatilidad sin sobrecargar el espacio de representación y previniendo el sobreajuste (overfitting) fuera de muestra.

![Sensibilidad del RMSE al radio espectral $\rho$.](figures/09_sensitivity_rho.png)
*Figura: Sensibilidad del RMSE al radio espectral $\rho$.* (fig:ssrc_sens_rho)

La Figura~[Ref: fig:ssrc_sens_rho] muestra la sensibilidad del RMSE frente al radio espectral $\rho$. La tendencia creciente hacia $\rho = 0.99$ indica que un exceso de memoria no lineal no beneficia a estas series, cuyos valores óptimos moderados son consistentes con una dinámica dominada por tendencias de largo plazo que ya han sido capturadas eficazmente por el operador TCRA.

### Resultados

La Tabla~[Ref: tab:comparacion_ssrc] presenta la comparación directa de desempeño predictivo entre TCRA-Markov (lineal) y TCRA-SSRC (no lineal), reportando el RMSE fuera de muestra (para el SSRC, como la media y desviación estándar sobre 30 realizaciones independientes). La significancia estadística se evaluó mediante el test de Diebold-Mariano (DM) [Cita: DieboldMariano1995], que compara las secuencias de errores cuadráticos de ambos modelos bajo la hipótesis nula de igual precisión predictiva.

*Tabla: Comparación de desempeño predictivo: TCRA-Markov vs.\ TCRA-SSRC.* (tab:comparacion_ssrc)

| lcccccc@{}}

**Serie** | **TCRA-Markov** | **TCRA-SSRC** | $(D^*, \rho^*, a^*)$ | $\Delta$**RMSE** | $p_{\text{DM}}$ | **Significativo** |
| --- | --- | --- | --- | --- | --- | --- |
| Regular | $0.8394$ | $0.6595 \pm 0.013$ | $(150,\; 0.85,\; 0.1)$ | $-21.43\%$ | $0.1154$ | No |
| Superior | $0.9769$ | $0.7810 \pm 0.006$ | $(150,\; 0.70,\; 0.3)$ | $-20.05\%$ | $0.0018$ | Sí |
| Diésel | $1.0816$ | $0.8299 \pm 0.018$ | $(150,\; 0.80,\; 0.1)$ | $-23.27\%$ | $0.0326$ | Sí |
| Kerosene | $1.1714$ | $0.9156 \pm 0.005$ | $(60,\;\, 0.70,\; 1.0)$ | $-21.84\%$ | $0.0061$ | Sí |

El TCRA-SSRC reduce el RMSE entre un 20\% y un 23\% respecto
al modelo Markoviano en las cuatro series.  La mejora es
estadísticamente significativa al nivel $\alpha = 0.05$ para Superior
($p = 0.002$), Diésel ($p = 0.033$) y Kerosene ($p = 0.006$).
Para Regular, la mejora es sustancial en magnitud ($-21.43\%$) pero
marginalmente significativa ($p = 0.115$); esto se atribuye a la
menor varianza de los errores de Regular (la serie más predecible),
que reduce la potencia del test DM.

![Comparación visual de RMSE: TCRA-Markov vs.\ TCRA-SSRC.](figures/12_comparison_barplot.png)
*Figura: Comparación visual de RMSE: TCRA-Markov vs.\ TCRA-SSRC.* (fig:ssrc_barplot)

La Figura~[Ref: fig:ssrc_barplot] ofrece una visualización comparativa de RMSE entre TCRA-Markov (azul) y TCRA-SSRC (naranja). Las barras de error del SSRC representan la desviación estándar sobre 30 realizaciones independientes. La anotación verde resalta la mejora porcentual del SSRC, cuya consistencia de entre un 20\% y un 23\% en las cuatro series analizadas sugiere un beneficio estructural derivado de la extensión no lineal.

![Distribución del RMSE del SSRC sobre 30 realizaciones independientes.](figures/07_boxplot_realizations.png)
*Figura: Distribución del RMSE del SSRC sobre 30 realizaciones independientes.* (fig:ssrc_boxplot)

Como se observa en la Figura~[Ref: fig:ssrc_boxplot], se detalla la distribución del RMSE del SSRC sobre 30 realizaciones independientes. Las líneas horizontales punteadas indican el RMSE del modelo Markoviano; todas las realizaciones del SSRC producen un error inferior, y las cajas estrechas confirman una baja sensibilidad a la inicialización aleatoria de las matrices de entrada y de reservorio, con la mayor dispersión relativa observada en el combustible Regular.

![Comparación de pronósticos vs.\ precios reales en el período fuera de muestra.](figures/06_predictions_real_vs_models.png)
*Figura: Comparación de pronósticos vs.\ precios reales en el período fuera de muestra.* (fig:ssrc_predictions)

En la Figura~[Ref: fig:ssrc_predictions] se comparan los precios reales con los pronósticos de los modelos TCRA-Markov y TCRA-SSRC en el período fuera de muestra. El SSRC sigue con mayor precisión los puntos de inflexión, especialmente en los valles y recuperaciones, donde el modelo Markoviano muestra un sesgo de rezago más marcado. Esta mejora es más evidente en las series de Diésel y Kerosene.

A primera vista, el pronóstico podría confundirse con la serie real desplazada un período, esto es, con el predictor ingenuo $\hat{y}_{t+1} = y_t$. Se trata de un efecto de rezago visual habitual en las series de alta persistencia, donde el último precio observado ya es un estimador puntual razonable del siguiente. La coincidencia es solo aparente. El predictor ingenuo copia el valor anterior y, por construcción, siempre llega tarde a los cambios de tendencia, mientras que el SSRC se adelanta a los valles y recuperaciones. Si el modelo se limitara a reproducir el rezago de primer orden, no podría mejorar el RMSE entre un 20\% y un 23\% sobre el modelo Markoviano ni alcanzar la significancia del test de Diebold-Mariano (Tabla~[Ref: tab:comparacion_ssrc]); ambos resultados confirman que el reservorio extrae tendencia real por encima de la simple persistencia.

### Análisis de Sensibilidad y Estabilidad

![Evaluación empírica de la cota de perturbación en función del radio espectral $\rho$.](figures/10_perturbation_bound.png)
*Figura: Evaluación empírica de la cota de perturbación en función del radio espectral $\rho$.* (fig:ssrc_perturbation)

La Figura~[Ref: fig:ssrc_perturbation] evalúa empíricamente la cota de perturbación (Proposición~[Ref: prop:acotacion_error]) en función del radio espectral $\rho$. Las cotas teóricas son significativamente conservadoras respecto a la variabilidad observada, lo que confirma que la restricción NNLS proporciona una regularización implícita no capturada por la cota analítica. La escala logarítmica muestra que decrecen con el radio espectral debido a que la norma de los pesos de lectura disminuye más rápido de lo que aumenta el factor de memoria, resultando en estados más informativos que requieren pesos menores para el combustible Kerosene.

### Representación de Regímenes en el Espacio del Reservorio

Una pregunta central es si la representación continua del reservorio
preserva la estructura de regímenes identificada por K-medias en el
capítulo anterior.  Las Figuras~[Ref: fig:ssrc_states_pca],
[Ref: fig:ssrc_scatter] y~[Ref: fig:ssrc_price_regimes] abordan esta
cuestión desde tres perspectivas complementarias.

![Proyección de los estados del reservorio mediante PCA.](figures/05_reservoir_states_by_regime.png)
*Figura: Proyección de los estados del reservorio mediante PCA.* (fig:ssrc_states_pca)

La Figura~[Ref: fig:ssrc_states_pca] presenta la proyección de los estados del reservorio $\vect{h}_t$ mediante PCA (dos componentes principales), coloreados según los regímenes del capítulo anterior. Los clústeres geométricamente separados confirman que el reservorio preserva la estructura de regímenes sin información explícita de clasificación. Más allá de la separabilidad geométrica visible en la imagen, las trayectorias entre clústeres revelan que el reservorio codifica las transiciones entre regímenes como desplazamientos suaves en el espacio latente, no como saltos discretos. Esto sugiere que la función $\tanh$ induce una representación continua de la dinámica de régimen, propiedad que no se observa en el modelo Markoviano del capítulo anterior.

![Relación entre retorno TCRA $\alpha_t$ y activación promedio del reservorio $\bar{h](figures/13_reservoir_regimes_scatter.png)
*Figura: Relación entre retorno TCRA $\alpha_t$ y activación promedio del reservorio $\bar{h* (fig:ssrc_scatter)

La Figura~[Ref: fig:ssrc_scatter] presenta la relación entre el retorno TCRA $\alpha_t$ y la activación promedio del reservorio $\bar{h}_t$, diferenciada por regímenes. La relación monótona confirma que el reservorio preserva el orden dinámico: las activaciones negativas corresponden a regímenes de caída y las positivas a regímenes de alza, mientras que la curvatura evidencia la no linealidad de $\tanh$. Como nota metodológica, las escalas de los ejes varían entre los paneles para maximizar la resolución visual de la dispersión de datos específicos de cada serie, debido a las marcadas diferencias en la magnitud de los retornos históricos absolutos de cada combustible. En series como Diésel, la amplificación de señales débiles cerca del origen y la compresión de las extremas revelan que la saturación de $\tanh$ actúa como regulador implícito: el reservorio no solo mapea $\alpha_t$ a un espacio de mayor dimensión, sino que redistribuye la masa de probabilidad de forma asimétrica según el régimen dominante.

![Identificación de regímenes de mercado mediante SSRC.](figures/14_reservoir_price_regimes.png)
*Figura: Identificación de regímenes de mercado mediante SSRC.* (fig:ssrc_price_regimes)

La Figura~[Ref: fig:ssrc_price_regimes] muestra la identificación de regímenes de mercado mediante SSRC, superpuestos a la serie de precios. La coincidencia entre transiciones de color y puntos de inflexión de la serie (como la marcada caída de 2020) confirma que el SSRC mantiene la interpretabilidad económica y operacional en un espacio de estados continuo de alta dimensión sin necesidad de variables exógenas, lo que lo hace directamente aplicable a contextos de planificación energética con disponibilidad de datos limitada.

### Discusión

Los resultados empíricos permiten responder a la pregunta central del
capítulo: ¿aporta la extensión no lineal una ganancia predictiva
sobre el modelo Markoviano lineal?

La respuesta es afirmativa y sustancial.  El SSRC reduce el RMSE
entre un 20\% y un 23\% en las cuatro series, con significancia
estadística confirmada por el test de Diebold-Mariano en tres de
cuatro casos.  Este resultado valida empíricamente la predicción de la
Observación~[Ref: obs:jerarquia_modelos]: la generalización estrictamente más
expresiva del SSRC efectivamente captura estructura no lineal que el
modelo Markoviano no puede representar.

Los hallazgos centrales se detallan a continuación.  Respecto a la
relevancia del factor de fuga $a$.  Los valores óptimos $a^* = 0.1$
para Regular y Diésel indican que la clave de la ganancia no es la
no linealidad *per se*, sino la *memoria temporal adaptativa*:
el Leaky-ESN con $a = 0.1$ retiene el 90\% del estado previo,
actuando como un filtro de suavizado que complementa la ventana
$W \approx 50$ del operador TCRA.  En contraste, Kerosene con
$a^* = 1.0$ se beneficia de la memoria recurrente pura, consistente
con su dinámica más volátil y su mayor número de regímenes ($K = 5$).

En cuanto a la saturación dimensional, la
Figura~[Ref: fig:ssrc_sens_D] muestra que la ganancia se satura
rápidamente: la diferencia entre $D = 20$ y $D = 150$ es inferior al
0.5\% en RMSE.  Esto indica que la complejidad no lineal intrínseca
del proceso generador de precios de combustibles hondureños es baja,
lo cual es consistente con un mercado regulado donde los ajustes
semanales son pequeños y predecibles.  El reservorio no necesita alta
dimensionalidad para capturar esta estructura.

Sobre la robustez del ensemble, la
Figura~[Ref: fig:ssrc_boxplot] muestra que la totalidad de las 30
realizaciones produce RMSE inferior al modelo de referencia markoviano, con
rangos intercuartílicos estrechos.  Esto confirma que la ganancia no
es un artefacto de una inicialización afortunada, sino una propiedad
estructural del modelo.

Queda la pregunta de qué estructura captura el reservorio que
el modelo Markoviano no puede representar.  La ganancia se atribuye a
la capacidad del SSRC de codificar *asimetrías temporales* en la
dinámica de precios: las caídas y las recuperaciones no son simétricas
ni en magnitud ni en velocidad.  El modelo Markoviano, al operar sobre
estados discretos de un paso, trata las transiciones bajistas$\to$estables
y alcistas$\to$estables como eventos de naturaleza equivalente, gobernados
por probabilidades estáticas en $\hat{\mat{P}}$.  El reservorio, al
retener memoria continua de las últimas $\tau = 1/a$ semanas en su
vector de estado $\vect{h}_t$, puede distinguir la *trayectoria*
que condujo al estado actual, y no únicamente el estado en sí mismo. Esta
capacidad es particularmente relevante para el pico de precios de
2022 y la recuperación posterior (visible en la
Figura~[Ref: fig:ssrc_predictions], semanas $\sim$5--20), donde el
SSRC anticipa mejor los puntos de inflexión que el modelo
Markoviano.

Queda por sopesar la parsimonia, porque un reservorio de dimensión alta suma parámetros que conviene justificar. La saturación dimensional ya descrita acota ese costo: como la diferencia entre $D = 20$ y $D = 150$ es inferior al $0.5\%$ en RMSE, el modelo no necesita muchas neuronas y una dimensión modesta basta para capturar la estructura. Sobre esa base, la ganancia del 20\% al 23\% en RMSE compensa con holgura los parámetros adicionales frente al modelo Markoviano. La restricción de no negatividad del NNLS refuerza el balance. Poda las direcciones de lectura que solo ajustarían ruido y, al mantener fijo el reservorio en lugar de entrenarlo por completo, evita el sobreajuste que provocaría un ajuste libre de todos los pesos sobre muestras de apenas unos cientos de observaciones.

## Limitaciones

**Sensibilidad a la inicialización.**  A diferencia del marco
TCRA-Markov, que es determinista dados los hiperparámetros, el SSRC
introduce variabilidad estocástica a través de las matrices aleatorias
$\mat{W}_{\text{in}}$ y $\mat{W}_{\text{res}}$.  Aunque la ESP
garantiza que la condición inicial $\vect{h}_0$ no afecta el
resultado, la *elección* de las matrices de reservorio sí
influye en el desempeño.  El promediado sobre 30 realizaciones
(*ensemble*) mitiga este efecto, como lo evidencia la baja
variabilidad observada en la Figura~[Ref: fig:ssrc_boxplot], pero
incrementa el costo computacional por un factor de 30.

**Condicionamiento numérico.**  Los números de condición
$\kappa(\mat{H}) \sim 10^{9}$--$10^{10}$ observados en la
Tabla~[Ref: tab:verificaciones_teoricas] indican una sensibilidad
numérica potencialmente alta.  Aunque la estabilidad práctica está
asegurada por la regularización implícita del NNLS y el ensemble,
este condicionamiento impone un límite superior a la dimensión
útil del reservorio: aumentar $D$ más allá de $\sim$150 degradaría
$\kappa(\mat{H})$ sin ganancia predictiva.

**Interpretabilidad reducida.**  Los estados del reservorio
$\vect{h}_t \in \mathbb{R}^D$ carecen de la interpretación directa
que poseen los estados discretos $S_t \in \{s_1, \ldots, s_K\}$ del
modelo Markoviano.  Sin embargo, las
Figuras~[Ref: fig:ssrc_states_pca]--[Ref: fig:ssrc_price_regimes]
demuestran que la estructura de regímenes se preserva implícitamente
en el espacio del reservorio, lo que permite recuperar
interpretabilidad parcial mediante técnicas de reducción de
dimensionalidad.

**Selección de hiperparámetros del reservorio.**  Los parámetros
$(D, \rho_{\text{res}}, a)$ carecen de una teoría de selección
óptima análoga al AIC del modelo Markoviano.  La búsqueda en grilla de 240 configuraciones es costosa, y si se escala a más series convendría recurrir a Optimización Bayesiana. También, la discretización univariada podría beneficiarse de algoritmos como Fisher-Jenks, que garantizan particiones óptimas globales en una dimensión.

**Conservadurismo de la cota de perturbación.**  La
Proposición~[Ref: prop:acotacion_error] proporciona una cota en el
peor caso que, como se evidencia en la
Figura~[Ref: fig:ssrc_perturbation], sobreestima la inestabilidad
real por varios órdenes de magnitud.  Una cota más ajustada
requeriría análisis probabilístico que incorpore la distribución de
$\alpha_t$ y la estructura de $\mat{W}_{\text{res}}$, lo cual
es un problema abierto para trabajo futuro. En particular, la aplicación del Teorema del Círculo de Gerschgorin sobre la matriz dispersa $\mat{W}_{\text{res}}$ podría producir cotas bastante más ajustadas.

## Resumen del Capítulo

Este capítulo ha establecido la relación formal entre el marco
TCRA-Markov desarrollado a lo largo de la tesis y la computación
de reservorio no lineal.  Los resultados principales son:

**Sobre la conexión teórica.**  Se demostró
(Teorema~[Ref: teo:inclusion]) que TCRA-Markov is un caso particular
del SSRC, obtenido al fijar $\mat{W}_{\text{res}} = \mat{0}$,
$\sigma = \mathrm{id}$ y $a = 1$.  Esta relación de inclusión
establece una **jerarquía de modelos**: TCRA-Markov (lineal,
interpretable, estados discretos) $\subset$ TCRA-SSRC (no lineal,
aproximador universal, estados continuos).  La
Observación~[Ref: obs:jerarquia_modelos] garantiza que la extensión es
estrictamente más expresiva, y la verificación numérica
(Figura~[Ref: fig:ssrc_inclusion]) confirma la identidad a nivel de
precisión de máquina.

**Sobre la estimación.**  La capa de lectura del SSRC se estima
mediante el mismo procedimiento NNLS de los capítulos anteriores.
El Teorema~[Ref: teo:existencia_wout] establece que las propiedades
de existencia y unicidad se heredan directamente.  La restricción
de no negatividad, además de preservar la interpretación como
combinación positiva de filtros, aporta regularización implícita que
mitiga el condicionamiento numérico adverso ($\kappa \sim 10^{9}$).

**Sobre el factor de fuga.**  La variante Leaky-ESN
(Definición~[Ref: def:leaky_esn]) con $a < 1$ resultó esencial para
tres de cuatro series, indicando que la principal ganancia del SSRC
sobre TCRA-Markov reside en la *memoria temporal adaptativa*
más que en la no linealidad pura.  Los valores $a^* = 0.1$ para
Regular y Diésel implican una constante de tiempo efectiva de
$\tau = 10$ semanas, complementaria a la ventana TCRA de 50--52
semanas.

**Sobre la aplicación empírica.**  El SSRC reduce el RMSE entre
un 20\% y un 23\% respecto al modelo Markoviano, con significancia
estadística confirmada por el test de Diebold-Mariano para Superior
($p = 0.002$), Diésel ($p = 0.033$) y Kerosene ($p = 0.006$).  La
totalidad de las 30 realizaciones aleatorias produce RMSE inferior al
modelo de referencia markoviano, y la estructura de regímenes económicos se
preserva en el espacio latente del reservorio.

**Perspectiva.**  La arquitectura TCRA-SSRC aquí desarrollada
establece que el paradigma de reservorio aporta ganancia predictiva
en mercados de combustibles regulados, a costa de interpretabilidad.
La extensión natural es hacia sistemas multivariados y acoplados,
donde la capacidad de aproximación no lineal del reservorio puede
explotarse más plenamente en contextos con mayor riqueza estructural.

# Conclusiones Generales y Trabajo Futuro

## Síntesis del Trabajo

Esta tesis construyó un marco matemático para analizar regímenes
económicos a partir de la *Tasa de Cambio Relativa
Autorregresiva* (TCRA). El recorrido va desde los fundamentos
algebraicos del operador hasta su extensión no lineal con
computación de reservorio. Cada capítulo se apoyó en los resultados del
anterior, de modo que la jerarquía de modelos creció en complejidad
pero siempre con garantías teóricas bajo condiciones explícitas y validación sobre
datos reales del mercado hondureño de combustibles.

El punto de partida fue formalizar la familia de operadores TCRA
y sus variantes (TCRAM, ETCRA, ETCRAM) como transformaciones
parametrizadas sobre series temporales. Se demostraron las
relaciones de anidamiento entre variantes y se estableció que la
señal $\alpha_t$ admite interpretación como coeficiente de un modelo
AR(1) local estimado por mínimos cuadrados ponderados. Esta
interpretación resultó ser más relevante de lo que aparenta: al
darle a $\alpha_t$ significado como *tasa de cambio local*, se
abrió la puerta a usarla tanto en modelos estocásticos discretos
(cadenas de Markov) como en modelos dinámicos continuos (reservorios
recurrentes).

La extensión no lineal del operador incluyó teoremas de existencia
y unicidad del estimador bajo condiciones de no degeneración de los
datos en la ventana. Estas garantías analíticas aseguran que la
señal $\alpha_t$ esté bien definida en cada instante, condición
necesaria para la modelación Markoviana posterior.

La integración con cadenas de Markov de tiempo discreto formalizó
las propiedades de $\phi$-mixing, la convergencia asintótica y la
conexión entre la brecha espectral de la matriz de transición y la
velocidad de convergencia al equilibrio estacionario. El resultado
central aquí es que la señal $\alpha_t$ discretizada hereda
propiedades de mezcla bajo condiciones de irreducibilidad, aperiodicidad y estabilidad de la partición, lo que permite
hacer inferencia estadística válida en los capítulos aplicados.

La estimación de la matriz de transición $\hat{\mat{P}}$ se resolvió
mediante mínimos cuadrados no negativos (NNLS), incluyendo una
prueba constructiva de existencia y unicidad. La formulación
transforma el problema matricial en un sistema lineal aumentado
mediante el producto de Kronecker. La ventaja práctica es directa:
evita la optimización no convexa de la verosimilitud que hace al
MS-AR vulnerable a mínimos locales, y garantiza convergencia global. Las probabilidades estocásticas ($\sum_i \hat{p}_{ij} = 1$ para cada columna $j$) se aseguran mediante normalización columna por columna, preservando la convención de matrices estocásticas por columnas del Capítulo~[Ref: cap:tcroc_hibrido_integrado].

La validación empírica aplicó el marco TCRA-Markov a series
semanales de cuatro combustibles hondureños (Regular, Superior, Diésel
y Kerosene) durante 2017--2025. El modelo fue superior a la
discretización por cuantiles fijos ($p < 0.05$ en exactitud para las
cuatro series) y, dato revelador, el modelo de referencia MS-AR no alcanzó convergencia bajo el protocolo de validación y conjunto de datos de este estudio (statsmodels v0.14, 50 reinicios aleatorios). El análisis espectral mostró un mercado con
alta inercia (brechas espectrales $\gamma \in [0.021, 0.040]$ y tiempos
de mezcla de 25 a 47 semanas). También se identificó que para
ventanas $W$ demasiado grandes el modelo se degenera, lo que llevó
a establecer la restricción $W \le 52$ como condición necesaria
para que los regímenes sean identificables.

El apartado teórico final cerró el arco analítico de la tesis
al demostrar que el marco TCRA-Markov es un caso particular de una
arquitectura más general: el Computador de Reservorio Estructurado
Estocástico (SSRC).  El Teorema de Inclusión (Teorema~6.3)
estableció formalmente que toda predicción del modelo Markoviano es
reproducible por un reservorio con $\mat{W}_{\text{res}} = \mat{0}$,
$\sigma = \text{id}$ y $a = 1$, pero no a la inversa.  La extensión
no lineal, validada empíricamente con mejoras del 20--23\% en RMSE
respecto al modelo Markoviano (estadísticamente significativas para
tres de cuatro series, $p < 0.05$ vía test de Diebold-Mariano),
demostró que la representación continua de estados mediante
reservorios recurrentes captura estructura que la discretización
Markoviana no puede representar, particularmente las asimetrías
temporales entre caídas y recuperaciones de precios.

## Resumen Cuantitativo de Resultados

La Tabla~[Ref: tab:resumen_final] resume las métricas de rendimiento del modelo TCRA-Markov para las cuatro series de combustibles analizadas, junto con los métodos de referencia. Se reportan la exactitud de predicción de régimen, el número de estados $K$, la ventana óptima $W$ y la significancia estadística frente al modelo de referencia de cuantiles fijos. A nivel de desempeño, se observa que el modelo TCRA-Markov (K-medias) supera de forma consistente en exactitud a la referencia de cuantiles en todas las series de combustible estudiadas. Es de particular relevancia destacar que el modelo autorregresivo con cambio de régimen de Hamilton (MS-AR) no logró alcanzar convergencia bajo el protocolo de validación y optimización seleccionado (statsmodels v0.14, 50 reinicios aleatorios) debido a la naturaleza altamente no lineal y discontinua de las transiciones locales de precios regulados en Honduras. Esto confirma empíricamente la robustez y la ventaja computacional del método de mínimos cuadrados no negativos (NNLS) de formulación convexa frente al algoritmo de optimización iterativa EM empleado en el MS-AR tradicional.

*Tabla: Resumen consolidado de métricas por serie y método.* (tab:resumen_final)

| **Serie** | $\mathbf{K}$ | $\mathbf{W}$ | **Exactitud** | **vs. Cuantiles** | **MS-AR** |
| --- | --- | --- | --- | --- | --- |
| Gasolina Regular | 3 | 50 | 85.10\% | $p < 0.01$ | No conv. |
| Gasolina Superior | 4 | 52 | 62.78\% | $p < 0.05$ | No conv. |
| Diésel | 3 | 50 | 71.56\% | $p < 0.05$ | No conv. |
| Kerosene | 5 | 52 | 68.20\% | $p < 0.05$ | No conv. |

En todos los casos, el modelo TCRA-Markov superó
significativamente al modelo de referencia de cuantiles fijos. El modelo
MS-AR no alcanzó convergencia bajo el protocolo de validación y conjunto de datos de este estudio (statsmodels v0.14, 50 reinicios), lo que evidencia la ventaja práctica de la formulación NNLS en este contexto empírico frente a la optimización por
máxima verosimilitud para estas series reguladas.

Esta divergencia plantea una interrogante fundamental sobre si es preferible prescindir de supuestos distribucionales en el modelado de este tipo de series temporales. El enfoque no paramétrico y libre de distribución (*distribution-free*) adoptado en el marco TCRA-Markov proporciona una notable robustez frente a colas pesadas, saltos discretos y asimetrías severas que suelen invalidar los supuestos de normalidad o linealidad de los modelos paramétricos tradicionales. No obstante, la ventaja clave de la metodología propuesta no radica únicamente en la ausencia de supuestos distribucionales exógenos, sino en el desacoplamiento algorítmico que permite reformular un problema de optimización no convexo e inestable (el algoritmo de Esperanza-Maximización en MS-AR) en un problema de optimización cuadrática estrictamente convexo y determinista (NNLS). Esto garantiza teóricamente la convergencia global a un óptimo único, evitando las limitaciones de convergencia numérica y la inestabilidad de parámetros características de los métodos iterativos clásicos ante datos con alta persistencia y controles administrativos.

## Contribuciones

Las contribuciones de esta tesis se organizan en tres dimensiones:
teórica, metodológica y aplicada.

### Contribuciones Teóricas

    * Se formalizó la estructura de retículo (*lattice*) de variantes de la TCRA, demostrando las relaciones de anidamiento donde TCRAM y ETCRA representan ramas paralelas que confluyen en la variante general ETCRAM (Capítulo~[Ref: cap:tcra]).

    * El operador TCRA polinómico admite un único estimador
    bajo condiciones de no degeneración.  La demostración, basada
    en la convexidad estricta de la funcional de costo, asegura
    que la señal $\alpha_t$ queda bien definida.

    * La secuencia de estados $\{S_t\}$ obtenida por
    discretización de $\alpha_t$ hereda propiedades de
    $\phi$-mixing, lo que habilita la inferencia estadística
    asintótica sobre la cadena de Markov resultante.

    * Para la estimación de $\hat{\mat{P}}$ mediante NNLS
    aumentado se probó existencia y unicidad de la solución cuando
    la matriz de diseño tiene rango completo, y se caracterizaron
    las condiciones de rango en función de la representación de
    todos los estados en los datos.

    * El Teorema de Inclusión TCRA-Markov $\subset$ SSRC
    establece que el modelo Markoviano lineal es un caso
    degenerado del computador de reservorio estructurado,
    fijando una jerarquía estricta de expresividad entre ambos
    marcos.

    * Se extendió el teorema de la Propiedad de Estado de Eco
    al caso Leaky-ESN: la contracción del mapa de reservorio se
    preserva para todo $a \in (0, 1]$ siempre que
    $\rho(\mat{W}_{\text{res}}) < 1$.

    * Finalmente, se caracterizó analíticamente el fenómeno de
    degeneración cuando $W \to T$: $\text{Var}(\alpha_t) \to 0$ y
    K-medias converge a $K_{\text{eff}} = 1$
    independientemente de $K$ nominal.

### Contribuciones Metodológicas

El marco TCRA-Markov de dos etapas integra el operador TCRA con
cadenas de Markov estimadas vía NNLS, evitando la optimización no
convexa del MS-AR y asegurando convergencia global.  Este marco se
aceptó para publicación en IEEE CONCAPAN~XLII (2025).
La extensión no lineal (la arquitectura TCRA-SSRC) reemplaza la
discretización K-medias por un reservorio recurrente Leaky-ESN,
preservando la estimación NNLS para la capa de lectura. Es clave señalar que la observabilidad directa de los estados de transición, provista originalmente por la formulación del operador TCRA, es el elemento habilitador que permite prescindir de algoritmos iterativos no convexos como Esperanza-Maximización (EM) y aplicar optimización cuadrática convexa (NNLS) en ambas etapas.

También se diseñó un protocolo de optimización jerárquico en tres
niveles (RMSE $\to$ Exactitud $\to$ AIC) con búsqueda exhaustiva en
grilla y restricción fundamentada de $W \le 52$.  La validación se
completó con el test de Diebold-Mariano, combinando ventanas
deslizantes, ensemble de 30 realizaciones y pruebas de significancia.

### Contribuciones Aplicadas

En el plano empírico, la tesis aporta una caracterización
cuantitativa del mercado hondureño de combustibles: se
identificaron de 3 a 5 regímenes por serie, con centroides
interpretables y distribuciones estacionarias que formalizan la
inercia del mercado regulado.  Las pruebas estadísticas confirman
que TCRA-Markov supera a la discretización por cuantiles ($p < 0.05$)
y que el modelo de referencia MS-AR no alcanza la convergencia bajo el protocolo y conjunto de datos de este estudio, mientras que el modelo TCRA-SSRC
mejora sobre TCRA-Markov en 20--23\% ($p < 0.05$ para tres de cuatro
series).  Un resultado no anticipado es la jerarquía de complejidad
del mercado: Regular y Diésel ($K^* = 3$) exhiben una dinámica más
simple que Superior ($K^* = 4$) y Kerosene ($K^* = 5$), y la
complejidad estructural se correlaciona positivamente con la
dificultad de predicción.

## Limitaciones Generales

Las siguientes limitaciones delimitan el alcance de los resultados obtenidos en esta investigación:

    * **Alcance y representatividad de los datos:** Los datos empíricos provienen exclusivamente del mercado hondureño de combustibles (un mercado regulado administrativamente, con ajustes semanales de precios y baja volatilidad en comparación con mercados libres) y todos los pronósticos se realizan a un solo paso adelante ($h = 1$ semana). La generalización a mercados desregulados, a otras clases de activos financieros o a horizontes múltiples de tiempo requiere investigación adicional.
    
    * **Condicionamiento numérico del reservorio:** Los números de condición de la matriz de estados alcanzan valores elevados ($\kappa(\mat{H}) \sim 10^{9}$--$10^{10}$). Aunque en la práctica la restricción de no negatividad de NNLS y el promediado de un ensemble de 30 realizaciones mitigan la sensibilidad numérica del sistema, una solución matemática formal (como la regularización de Tikhonov con selección adaptativa) queda pendiente de mayor desarrollo teórico.
    
    * **Asunción de homogeneidad y estabilidad temporal:** El modelo Markoviano asume probabilidades de transición constantes en el tiempo, y el SSRC asume pesos de lectura fijos tras su entrenamiento. Sin embargo, el período analizado 2017--2025 incluye choques estructurales severos como la pandemia de COVID-19 y el pico global de precios energéticos de 2022, eventos que potencialmente violan el supuesto de estacionariedad. No se realizó una prueba formal para evaluar el cambio estructural temporal de los parámetros.
    
    * **Supuesto de Markov de primer orden sin validación de orden:** La estructura del modelo Markoviano se construyó bajo la asunción de primer orden (Suposición~3.1), lo que significa que el estado futuro depende únicamente del estado actual. Esta hipótesis se adoptó por parsimonia metodológica y no fue precedida de pruebas de orden estadísticas formales (e.g., pruebas Chi-cuadrado para transiciones de segundo orden o superiores), omitiendo posibles dependencias de mayor alcance.
    
    * **Sensibilidad a la inicialización y selección heurística:** La estimación del espacio discretizado mediante K-medias exhibe sensibilidad a la semilla de inicialización aleatoria de los centroides. Adicionalmente, el protocolo jerárquico seleccionó los hiperparámetros óptimos sobre el dataset completo en lugar de implementar una validación cruzada anidada rigurosa, lo que introduce un sesgo de selección potencial en los resultados predictivos reportados.
    
    * **Enfoque univariante y exclusión de variables exógenas:** El modelado se restringe a una estructura puramente autorregresiva (endógena), donde la dinámica de cada combustible se predice basándose únicamente en sus precios históricos. Esto impide capturar de forma directa el impacto de choques exógenos macroeconómicos globales (como el precio internacional del crudo WTI) o variables de política local (como variaciones discrecionales en los subsidios).

## Líneas de Trabajo Futuro

Los resultados y limitaciones de esta tesis sugieren varias
direcciones de investigación futura, organizadas por horizonte
temporal.

### Extensiones Inmediatas

**Sistemas multivariados acoplados.** La extensión natural del marco es pasar de series univariantes a sistemas de series temporales interrelacionadas. En el contexto centroamericano, los precios de combustibles de las seis economías del istmo están acoplados por proximidad geográfica, acuerdos comerciales y dependencia compartida del crudo WTI. Un modelo TCRA-Markov multivariado estimaría una matriz de transición conjunta $\hat{\mat{P}} \in \mathbb{R}^{K^m \times K^m}$, donde $m$ es el número de series acopladas, capturando las dependencias cruzadas entre mercados. Sin embargo, esto induce una explosión dimensional exponencial (con $m=6$ países y $K=4$ regímenes por país, el sistema requiere estimar una matriz de $4096 \times 4096$). Para mitigar este problema, se propone el uso de estructuras de tipo *Hub-and-Spoke* (donde un nodo central conecta y coordina los mercados satélites, reduciendo la complejidad a $m K^2$), factorizaciones matriciales de bajo rango o modelos gráficos probabilísticos. Esta línea de investigación se encuentra actualmente en desarrollo como artículo de investigación independiente.

**Comparación de dinámicas regionales centroamericanas.** Aunque esta tesis se enfoca en el mercado regulado hondureño, una línea de investigación prioritaria consiste en extender el análisis comparativo a nivel de Centroamérica para disponer de un escenario de validación más amplio. Esta comparativa regional es fundamental para evaluar el comportamiento de los operadores bajo marcos regulatorios divergentes. Por ejemplo, permitiría contrastar la inercia temporal y la exactitud predictiva en países con control administrativo de precios (como Honduras y Costa Rica) frente a economías con regímenes de libre mercado (como El Salvador y Guatemala). Implementar este análisis regional enriquecerá la robustez del modelo y ayudará a dimensionar el impacto real de las políticas públicas sobre la dinámica local de precios.

**Pruebas de estabilidad temporal.** La partición de la muestra en subperíodos (prepandemia 2017--2019, pandemia 2020--2021, pospandemia 2022--2025) y la comparación de las matrices $\hat{\mat{P}}$ estimadas en cada subperíodo, mediante pruebas de homogeneidad basadas en distancia entre matrices estocásticas, permitirían evaluar formalmente el supuesto de estabilidad temporal. Para mitigar el problema de muestras pequeñas y celdas con pocas observaciones en subperíodos cortos (que incrementan la variabilidad de la estimación), se propone el uso de métodos de bootstrap paramétrico para estimar la incertidumbre y obtener intervalos de confianza robustos para cada celda de la matriz de transición.

**Regularización adaptativa del SSRC.** La sustitución de NNLS por un esquema de regularización de Tikhonov con selección del parámetro de regularización $\eta_R$ (utilizando esta notación para evitar confusión con el factor de olvido $\lambda$ del operador TCRA) mediante validación cruzada generalizada (GCV) podría mejorar el condicionamiento numérico sin sacrificar la interpretabilidad de los pesos no negativos, mediante una formulación de NNLS regularizada
\[ \hat{\vect{w}} = \argmin_{\vect{w} \ge 0} \|\mat{H}\vect{w} - \vect{y}\|^2 + \eta_R\|\vect{w}\|^2. 
$$

### Extensiones de Mediano Plazo

**Topología de reservorio estructurada.**  En el SSRC actual,
la matriz $\mat{W}_{\text{res}}$ es aleatoria y dispersa.  Trabajos
recientes en la literatura de computación de reservorio sugieren que
topologías deterministas (e.g., anillo, ciclo con saltos) pueden
reducir la varianza entre realizaciones y mejorar la
reproducibilidad.  Explorar estas topologías en el contexto TCRA
es una extensión natural.

**Incorporación de variables exógenas.** El marco actual es puramente endógeno: $\alpha_t$ se calcula a partir de los precios pasados. La incorporación de variables exógenas excede el análisis univariante al incorporar el precio del crudo WTI, el tipo de cambio o índices macroeconómicos como entradas adicionales. Sin embargo, la inyección directa de múltiples variables en un único reservorio masivo puede violar la propiedad de estado de eco (ESP) al alterar el radio espectral efectivo y la contracción del sistema. Como alternativa viable, se propone una arquitectura de reservorio profundo modular (*Deep ESN modular*), en la cual cada variable de entrada es procesada de forma independiente por su propio operador y sub-reservorio, garantizando de manera aislada la contracción, para posteriormente concatenar las capas de salida antes de realizar la estimación por NNLS.

**Pronóstico multi-horizonte.**  La extensión del horizonte
de predicción de $h = 1$ a $h \in \{1, 4, 8, 12\}$ semanas, ya sea
mediante recursión del reservorio o mediante formulación directa
con múltiples salidas, permitiría evaluar la degradación de la
capacidad predictiva con el horizonte y determinar el alcance
práctico del marco para la planificación económica.

### Visión de Largo Plazo

**Generalización a otras clases de activos.**  El marco
TCRA-Markov/SSRC no asume nada específico sobre la naturaleza de
la serie temporal subyacente.  Su aplicación a tasas de interés,
índices bursátiles, tipos de cambio o indicadores macroeconómicos
abre un programa de investigación que podría validar la
generalidad del enfoque más allá del mercado de combustibles.

**Fundamentación teórica y cotas de convergencia.** Las cotas de perturbación derivadas en esta tesis para el SSRC son conservadoras por varios órdenes de magnitud. Una línea de largo plazo consiste en desarrollar cotas más ajustadas utilizando la localización de valores propios vía círculos de Gerschgorin sobre la estructura dispersa de $\mat{W}_{\text{res}}$. Asimismo, se propone formalizar las tasas explícitas de convergencia para la propiedad de estado de eco (ESP) como función conjunta del factor de fuga $a$ y el radio espectral $\rho$, así como estimar analíticamente la velocidad de convergencia del Aproximador Universal en función de la dimensión del reservorio $D$.

## Recomendaciones

Las conclusiones y resultados de esta investigación permiten formular las siguientes recomendaciones específicas, orientadas a la toma de decisiones y a la investigación futura:

### Recomendaciones para el Mercado de Combustibles de Honduras

    * **Dirigido a la Secretaría de Energía (SEN) y a la Comisión Administradora de Petróleo (CAP):** Se recomienda la adopción del operador de Tasa de Cambio Relativa Autorregresiva (TCRA) en conjunto con cadenas de Markov para establecer un sistema de monitoreo sistemático de las transiciones de precios de combustibles a nivel nacional. La capacidad de estimar la matriz de transición $\hat{\mat{P}}$ con garantías de convergencia global (mediante la formulación NNLS) ofrece un instrumento cuantitativo robusto para predecir si el mercado entrará en regímenes de alza fuerte o descenso fuerte, sirviendo como insumo técnico para el diseño óptimo de subsidios gubernamentales y mitigación de choques de precios externos.
    
    * **Dirigido a la Secretaría de Desarrollo Económico (SDE):** Se recomienda utilizar las estimaciones de la brecha espectral y los tiempos de mezcla (que varían de 25 a 47 semanas en el mercado hondureño) como métricas oficiales para evaluar el grado de inercia y persistencia en los precios de los energéticos. Esto permitirá fundamentar técnicamente el análisis de impacto inflacionario por rezago de distribución en la cadena de transporte de mercancías.

### Recomendaciones para el Desarrollo de Modelos Estocásticos de Regímenes

    * **Dirigido a los investigadores en matemáticas aplicadas y econometría de la Universidad Nacional Autónoma de Honduras (UNAH):** Se recomienda el uso de la Computación de Reservorio Estocásticamente Estructurada (SSRC) como una extensión no lineal para el modelado de series de tiempo con cambios bruscos de dinámica. Se sugiere explotar las propiedades de estabilidad y el Teorema de Inclusión demostrados en esta tesis para estructurar reservorios con restricciones físicas o de signo, garantizando la interpretabilidad de los coeficientes de salida $\mat{W}_{\text{out}}$ obtenidos a través de optimización convexa.
    
    * **Dirigido a la comunidad científica internacional en aprendizaje automático:** Se recomienda profundizar en la regularización adaptativa de Tikhonov acoplada a la restricción de no negatividad para matrices de reservorios de alto condicionamiento numérico ($\kappa(\mat{H}) \sim 10^{9}$--$10^{10}$), con el fin de robustecer el modelado dinámico continuo de series altamente volátiles.

## Reflexiones Finales

El recorrido de esta tesis va desde la formalización de un operador
de tasa de cambio local hasta la demostración de que ese operador,
integrado con cadenas de Markov, es un caso particular de un
computador de reservorio no lineal. Existe una dicotomía fundamental en
matemáticas aplicadas entre simplicidad interpretable y complejidad
expresiva, y este trabajo la recorre en toda su extensión.

El modelo TCRA-Markov, con sus $K$ estados discretos y su matriz
de transición de $K(K-1)$ parámetros libres, es transparente: un
economista puede leer $\hat{\mat{P}}$ y comprender la dinámica del
mercado.  El modelo TCRA-SSRC, con su reservorio de $D$ neuronas y
su representación continua de estados, es más expresivo (entre un 20 y un 23\%
más preciso en el pronóstico), aunque menos inmediatamente interpretable.
Ambos tienen su lugar.

Lo que unifica ambos extremos es la formulación NNLS, que garantiza
soluciones no negativas, convergencia global y, sobre todo, la
posibilidad de interpretar los pesos de salida como
contribuciones no negativas, no necesariamente porcentuales, de cada componente al pronóstico.  Esta
propiedad, que podría parecer un detalle técnico, es lo
que hace que el marco completo sea útil más allá de la academia:
un modelo que mejora la predicción pero no puede explicar por qué
tiene valor limitado para la toma de decisiones en política
económica.

La aplicación a combustibles hondureños no fue elegida al azar.  En
una economía donde el precio del galón de diésel afecta directamente
el costo del transporte de alimentos, la capacidad de anticipar
regímenes de precios y de cuantificar la incertidumbre asociada
a esa anticipación, tiene consecuencias prácticas tangibles.  Si
el marco desarrollado en estas páginas contribuye, aunque sea
marginalmente, a mejorar la planificación en ese contexto, habrá
cumplido su propósito.

 

\begin{appendices}
\chaptermark{Apéndices}
\markboth{Apéndices}{Apéndices}
\renewcommand{\thesection}{A.\arabic{section}}
\renewcommand{\thesubsection}{A.\arabic{section}.\arabic{subsection}}
\renewcommand{\thefigure}{A.\arabic{figure}}
\renewcommand{\thetable}{A.\arabic{table}}

## Demostración Extendida del Teorema de \texorpdfstring{$\phi${phi}-Mixing}

Sea \( \{S_t\} \) una cadena de Markov homogénea con espacio de estados finito \( S = \{s_1, \dots, s_N\} \), matriz de transición \( P \), e irreducible y aperiódica.

\begin{theorem}[[Cita: Bradley2005]]
Una cadena de Markov \( \{S_t\} \) es \( \phi \)-mixing con tasa geométrica si existe \( \delta > 0 \) tal que para todo par de estados \( s_i, s_j \in S \), existe \( t \in \mathbb{N} \) con:

$$
 P^t(s_j | s_i) > \delta. 
$$

\end{theorem}

\begin{proof}[Idea de la demostración]
El resultado se deduce a partir del hecho de que las cadenas de Markov irreducibles, aperiódicas y con espacio de estados finito son uniformemente ergódicas. Tal propiedad implica mezcla fuerte \( \phi \)-mixing con decaimiento exponencial.
\end{proof}

**Observación práctica:** En el modelo TCRA-Markov, la irreducibilidad puede garantizarse si el rango de \( \alpha_{t,\lambda,W} \) permite transiciones entre todos los estados definidos por \( \pi \).

## Estabilidad Discreta de la Función de Cuantización
\texorpdfstring{$\pi${pi}}

La función de cuantización $\pi\colon \mathbb{R} \to \mathcal{S}$
definida en la Definición~[Ref: def:pi_cuantizacion] es una función
escalonada y, por tanto, discontinua en los umbrales
$\{\theta_j\}$.  En sentido estricto, $\pi$ no es Lipschitz
continua.  Sin embargo, la estabilidad del modelo se garantiza
mediante la separación entre umbrales, como se demuestra en el
Lema~[Ref: lem:estabilidad_discreta].

Si se desea una versión suavizada de $\pi$ para análisis
asintótico, se puede considerar la aproximación sigmoide:

$$

\pi^*(x) = \sum_{j=1}^{K} s_j \cdot h_j(x), \qquad
h_j(x) = \frac{1}{1 + \exp(-a(x - \theta_j))},

$$

donde $a > 0$ controla la pendiente de la transición.
La constante de Lipschitz de $\pi^*$ satisface
$L_{\pi^*} \leq a/4$, alcanzada en los umbrales ($x = \theta_j$).
Cuando $a \to \infty$, $\pi^* \to \pi$ puntualmente, pero
$L_{\pi^*} \to \infty$, lo cual es consistente con la
discontinuidad de $\pi$.  Esta suavización es útil para
demostraciones técnicas, pero en la práctica del modelo
TCRA-Markov se trabaja directamente con la versión discreta.

## Trayectorias y Transiciones del Proceso \texorpdfstring{$\{S_t\$}{St} con Datos Reales}

Las siguientes figuras y tablas presentan las trayectorias de estados y los diagramas de transición obtenidos directamente de los datos de combustibles (Superior y Regular), utilizando las matrices $\hat{\mat{P}}$ estimadas por el modelo TCRA-Markov (ene 2017 -- jul 2025). Para el análisis de las trayectorias temporales, se contrasta la secuencia observada (asignada por el retorno local $\alpha_t$ y los centroides de K-medias con $K=4$ estados) con una trayectoria simulada de Monte Carlo sobre una ventana de 20 periodos seleccionada por exhibir la máxima variabilidad de regímenes. Es importante recalcar que la trayectoria simulada corresponde a una realización estocástica particular libre y no pretende realizar una predicción determinista paso a paso ni coincidir con la serie real observada. La discrepancia paso a paso en ciertos tramos es inherente al carácter estocástico del método de Monte Carlo; su propósito es ilustrar la consistencia dinámica del modelo al reproducir patrones equivalentes de persistencia y volatilidad. Asimismo, los diagramas de transición grafican la estructura Markoviana de probabilidades estimadas con datos reales, mostrando únicamente aquellas transiciones con una probabilidad de ocurrencia marginal $\geq 0.02$ para facilitar la lectura.

### Combustible Superior ($K=4$ estados)

![Trayectorias del proceso $\{S_t\]()
*Figura: Trayectorias del proceso $\{S_t\* (fig:apx_trayectorias_super)

![Diagrama de transición entre regímenes para combustible **Superior** ($K=4$).]()
*Figura: Diagrama de transición entre regímenes para combustible **Superior** ($K=4$).* (fig:apx_diagrama_super)

*Tabla: Matriz de transición estimada $\hat{\mat{P* (tab:apx_P_super)

|  | $s_1$ | $s_2$ | $s_3$ | $s_4$ |
| --- | --- | --- | --- | --- |
| $s_1$ | 0.500 | 0.011 | $-$ | $-$ |
| $s_2$ | 0.250 | **0.711** | 0.097 | 0.011 |
| $s_3$ | $-$ | 0.267 | **0.819** | 0.232 |
| $s_4$ | 0.250 | 0.011 | 0.085 | **0.758** |

### Combustible Regular ($K=4$ estados)

![Trayectorias del proceso $\{S_t\]()
*Figura: Trayectorias del proceso $\{S_t\* (fig:apx_trayectorias_regular)

![Diagrama de transición entre regímenes para combustible **Regular** ($K=4$).]()
*Figura: Diagrama de transición entre regímenes para combustible **Regular** ($K=4$).* (fig:apx_diagrama_regular)

*Tabla: Matriz de transición estimada $\hat{\mat{P* (tab:apx_P_regular)

|  | $s_1$ | $s_2$ | $s_3$ | $s_4$ |
| --- | --- | --- | --- | --- |
| $s_1$ | 0.500 | 0.019 | $-$ | $-$ |
| $s_2$ | 0.250 | **0.736** | 0.043 | 0.010 |
| $s_3$ | $-$ | 0.226 | **0.878** | 0.225 |
| $s_4$ | 0.250 | 0.019 | 0.079 | **0.765** |

\renewcommand{\thesection}{B.\arabic{section}}
\renewcommand{\thesubsection}{B.\arabic{section}.\arabic{subsection}}
\renewcommand{\thefigure}{B.\arabic{figure}}
\renewcommand{\thetable}{B.\arabic{table}}
 

## Implementación Unificada del Operador TCRA

La siguiente función en Python calcula cualquier variante de la
familia TCRA según los parámetros proporcionados
(Sección~[Ref: sec:tcra_implementacion]).

\begin{lstlisting}[language=Python, caption={Función unificada para calcular las variantes de la TCRA.}, label={lst:tcra_implementacion}]
import numpy as np

def compute_tcra(v, W=None, lambd=1.0):
    """
    Operador unificado para la familia TCRA.
    Calcula beta y alpha bajo el marco L^2 ponderado.

    Parametros
    ----------
    v     : array 1-D, serie temporal (longitud T+1).
    W     : int o None. Ventana. Si None, W = T (datos completos).
    lambd : float en (0,1]. Factor de decaimiento. Default: 1.0.

    Retorna
    -------
    alpha : float, tasa de cambio relativa.
    beta  : float, factor multiplicador óptimo.
    """
    v = np.asarray(v, dtype=float)
    T = len(v) - 1

    if W is None or W > T:
        W = T
    elif W < 1:
        raise ValueError("W debe ser >= 1.")

    start = T - W + 1
    v_target = v[start : T+1]
    v_lag    = v[start-1 : T]

    weights = lambd ** np.arange(W-1, -1, -1) if lambd != 1.0 else 1.0

    num = np.sum(weights * v_target * v_lag)
    den = np.sum(weights * (v_lag ** 2))

    beta = num / den if den != 0 else 1.0
    alpha = beta - 1.0
    return alpha, beta
\end{lstlisting}

## Código Python para Análisis de Sensibilidad

A continuación, se presenta el código completo en Python utilizado para realizar el análisis de sensibilidad de los parámetros de discretización $W$ y $\lambda$, como se describe en la subsección de "Análisis de Sensibilidad de los Parámetros de Discretización".

\begin{lstlisting}[language=Python, caption={Script de Python para el cálculo de distribución de estados en el análisis de sensibilidad.}, label={lst:codigo_sensibilidad}]
import numpy as np
import pandas as pd

# --- Funciones Auxiliares ---

def calculate_alpha(v, W, lam):
    """Calcula la serie de tasas relativas alpha_t usando la fórmula de TCRA."""
    alphas = []
    T = len(v)
    weights = lam ** np.arange(W - 1, -1, -1)
    
    for t in range(W, T):
        v_current_window = v[t-W+1 : t+1]
        v_previous_window = v[t-W : t]
        
        numerator = np.sum(weights * v_current_window * v_previous_window)
        denominator = np.sum(weights * v_previous_window**2)
        
        if denominator == 0:
            beta_t = 1.0
        else:
            beta_t = numerator / denominator
        
        alphas.append(beta_t - 1)
        
    return np.array(alphas)

def assign_states(alphas):
    """Asigna estados discretos segun los umbrales definidos."""
    conditions = [
        alphas < -0.01,
        (alphas >= -0.01) & (alphas <= 0.02),
        (alphas > 0.02) & (alphas <= 0.04),
        alphas > 0.04
    ]
    choices = [1, 2, 3, 4]
    
    return np.select(conditions, choices, default=0)

# --- Script Principal del Analisis de Sensibilidad ---

# 1. DATOS Y PARAMETROS
datos_reales_pib = np.array([
    16214.3, 16211.7, 16381.9, 16692.9, 17296.5, 17903.1, 18634.1, 
    19337.8, 19708.9, 18813.3, 19104.7, 19431.7, 19828.9, 19983.0, 
    20199.1, 20579.2, 20982.7, 21595.6, 22019.3, 22177.7, 19838.3, 
    21961.6, 22491.3, 22900.9
])

ws_to_test = [2, 3, 4, 5]
lambdas_to_test = np.arange(1.0, 0.79, -0.01)
params_to_test = [(w, lam) for w in ws_to_test for lam in lambdas_to_test]

# 2. REALIZAR EL ANALISIS
results = []
for W, lam in params_to_test:
    alphas = calculate_alpha(datos_reales_pib, W, lam)
    states = assign_states(alphas)
    
    unique_states, counts = np.unique(states, return_counts=True)
    state_counts = dict(zip(unique_states, counts))
    
    total_states = len(states)
    if total_states > 0:
        s1_perc = (state_counts.get(1, 0) / total_states) * 100
        s2_perc = (state_counts.get(2, 0) / total_states) * 100
        s3_perc = (state_counts.get(3, 0) / total_states) * 100
        s4_perc = (state_counts.get(4, 0) / total_states) * 100
    else:
        s1_perc, s2_perc, s3_perc, s4_perc = 0, 0, 0, 0

    results.append({
        'W': W,
        'lambda': lam,
        '
        '
        '
        '
    })

# 3. MOSTRAR RESULTADOS
df_results = pd.DataFrame(results)
pd.set_option('display.max_rows', None)
print(df_results.round(2))
\end{lstlisting}

## Código Python para Validación con Múltiples Particiones

A continuación, se presenta el código completo en Python utilizado para realizar el análisis de robustez de la validación predictiva, como se resume en la Tabla~[Ref: tab:rendimiento_predictivo_resumen]. El script itera sobre múltiples particiones de entrenamiento/prueba para evaluar la estabilidad del rendimiento del modelo.

\begin{lstlisting}[language=Python, caption={Script de Python para validación con múltiples particiones.}, label={lst:codigo_validacion_multiples_particiones}]
import numpy as np
import pandas as pd
from scipy.optimize import nnls

# --- Funciones Auxiliares ---

def calculate_alpha(v, W, lam):
    """Calcula la serie de tasas relativas alpha_t usando la fórmula de TCRA."""
    alphas = []
    T = len(v)
    weights = lam ** np.arange(W - 1, -1, -1)
    for t in range(W, T):
        v_current_window = v[t-W+1 : t+1]
        v_previous_window = v[t-W : t]
        numerator = np.sum(weights * v_current_window * v_previous_window)
        denominator = np.sum(weights * v_previous_window**2)
        if denominator == 0:
            beta_t = 1.0
        else:
            beta_t = numerator / denominator
        alphas.append(beta_t - 1)
    return np.array(alphas)

def assign_states(alphas):
    """Asigna estados discretos segun los umbrales definidos."""
    conditions = [
        alphas < -0.01,
        (alphas >= -0.01) & (alphas <= 0.02),
        (alphas > 0.02) & (alphas <= 0.04),
        alphas > 0.04
    ]
    choices = [1, 2, 3, 4]
    return np.select(conditions, choices, default=0)

def SRep(datos, ss):
    """Estimador NNLS de matriz de transicion estocastica por columnas."""
    n0 = datos.shape[0]
    if ss >= datos.shape[1]:
        ss = datos.shape[1] - 1
        
    # 1. Matriz de diseno S0 y vector objetivo S1
    S0_data = datos[:, :ss]
    S0 = np.kron(S0_data, np.identity(n0)).T
    S1 = S0.T @ (datos[:, 1:(1 + ss)].T).reshape(ss * n0)
    
    # 2. Restriccion de suma unitaria
    C = np.kron(np.identity(n0), np.ones((1, n0)))
    
    # 3. Sistema aumentado
    Mr = np.zeros((n0**2 + n0, n0**2))
    Mr[:n0**2, :] = S0.T @ S0
    Mr[n0**2:, :] = C
    
    rhs = np.zeros((n0**2 + n0))
    rhs[:n0**2] = S1
    rhs[n0**2:] = 1
    
    # 4. Resolver NNLS
    p_flat = nnls(Mr, rhs)[0]
    
    # 5. Recomponer y normalizar
    Pr = p_flat.reshape(n0, n0).T
    col_sums = Pr.sum(axis=0)
    col_sums[col_sums == 0] = 1  # Proteccion contra division por cero
    Pr = Pr / col_sums

    return Pr

def calculate_count_matrix(states_sequence, num_states=4):
    """Calcula la matriz de conteo de transiciones C."""
    count_matrix = np.zeros((num_states, num_states), dtype=int)
    for i in range(len(states_sequence) - 1):
        from_state = states_sequence[i] - 1
        to_state = states_sequence[i+1] - 1
        if from_state >= 0 and to_state >= 0:
            count_matrix[to_state, from_state] += 1
    return count_matrix

# --- Script Principal con Multiples Divisiones ---

# 1. DATOS Y PARAMETROS
datos_reales_pib = np.array([
    16214.3, 16211.7, 16381.9, 16692.9, 17296.5, 17903.1, 18634.1, 
    19337.8, 19708.9, 18813.3, 19104.7, 19431.7, 19828.9, 19983.0, 
    20199.1, 20579.2, 20982.7, 21595.6, 22019.3, 22177.7, 19838.3, 
    21961.6, 22491.3, 22900.9
])
W_model = 2
lambda_model = 1.0
NUM_STATES = 4

# 2. GENERACION DE DATOS BASE
alphas_full = calculate_alpha(datos_reales_pib, W_model, lambda_model)
states_full = assign_states(alphas_full)
num_samples = len(states_full)
X_full = np.zeros((NUM_STATES, num_samples))
for i, state in enumerate(states_full):
    if state > 0:
        X_full[state - 1, i] = 1

C_full = calculate_count_matrix(states_full, num_states=NUM_STATES)
P_hat_full = SRep(X_full, ss=X_full.shape[1] - 1)

# 3. BUCLE PARA PROBAR DIFERENTES DIVISIONES
split_ratios_to_test = np.arange(0.60, 1.0, 0.05)

for ratio in split_ratios_to_test:
    train_perc = int(ratio * 100)
    test_perc = 100 - train_perc
    
    print("\n" + "#"*70)
    print(f"###   RESULTADOS PARA DIVISION {train_perc}
    print("#"*70 + "\n")

    # DIVISION EN ENTRENAMIENTO Y PRUEBA
    split_index = int(num_samples * ratio)
    states_train = states_full[:split_index]
    states_test = states_full[split_index:]
    X_train = X_full[:, :split_index]

    # CALCULO DE MATRICES
    C_train = calculate_count_matrix(states_train, num_states=NUM_STATES)
    C_test = calculate_count_matrix(states_test, num_states=NUM_STATES)
    P_hat_trained = SRep(X_train, ss=X_train.shape[1] - 1)

    # EVALUACION PREDICTIVA
    predictions = []
    actual_outcomes = []
    for i in range(len(states_test) - 1):
        current_state_idx = states_test[i] - 1
        prob_next_state = P_hat_trained[:, current_state_idx]
        predicted_state = np.argmax(prob_next_state) + 1
        predictions.append(predicted_state)
        actual_next_state = states_test[i+1]
        actual_outcomes.append(actual_next_state)

    # SALIDA PARA ESTA DIVISION
    idx = [f'a s{i+1}' for i in range(4)]
    cols = [f'de s{i+1}' for i in range(4)]

    print("--- Matriz de Conteo C (Entrenamiento) ---")
    print(pd.DataFrame(C_train, index=idx, columns=cols))
    
    print("\n--- Matriz de Conteo C (Prueba) ---")
    print(pd.DataFrame(C_test, index=idx, columns=cols))

    print("\n--- Matriz de Conteo C (Completa) ---")
    print(pd.DataFrame(C_full, index=idx, columns=cols))

    print("\n--- Matriz P_hat (Entrenamiento) ---")
    print(pd.DataFrame(P_hat_trained, index=idx, columns=cols).round(4))

    print("\n--- Matriz P_hat (Completa) ---")
    print(pd.DataFrame(P_hat_full, index=idx, columns=cols).round(4))
    print("\n" + "="*50 + "\n")

    print("--- Desglose de Predicciones en Prueba ---")
    prediction_df = pd.DataFrame({
        'Estado Actual (t)': states_test[:-1],
        'Real (t+1)': actual_outcomes,
        'Predicho (t+1)': predictions
    })
    if not prediction_df.empty:
        correct = (prediction_df['Real (t+1)'] == 
                   prediction_df['Predicho (t+1)'])
        prediction_df['Correcto?'] = correct
    print(prediction_df)

    # METRICAS RESUMEN
    correct_predictions = prediction_df['Correcto?'].sum() if 'Correcto?' in prediction_df else 0
    total_predictions = len(prediction_df)
    accuracy = correct_predictions / total_predictions if total_predictions > 0 else 0

    print("\n--- Metricas Resumen ---")
    print(f"Predicciones correctas: {correct_predictions} de {total_predictions}")
    print(f"Precision (Accuracy): {accuracy:.3f}")
\end{lstlisting}

## Código Fuente: Estimación y Simulación

El siguiente listado contiene la implementación del algoritmo
'SRep' para la estimación de matrices de transición
mediante Mínimos Cuadrados No Negativos (NNLS), así como
la simulación de la evolución de la cadena de Markov.

\begin{listing}[htbp]
\begin{python}
import numpy as np
from scipy.optimize import nnls

def SRep(datos, ss):
    """Estimador NNLS de matriz de transicion estocastica por columnas."""
    n0 = datos.shape[0]

    # 1. Matriz de diseno S0 y vector objetivo S1
    S0_data = datos[:, :ss]
    S0 = np.kron(S0_data, np.identity(n0)).T
    S1 = S0.T @ (datos[:, 1:(1 + ss)].T).reshape(ss * n0)

    # 2. Restriccion de suma unitaria
    C = np.kron(np.identity(n0), np.ones((1, n0)))

    # 3. Sistema aumentado
    Mr = np.zeros((n0**2 + n0, n0**2))
    Mr[:n0**2, :] = S0.T @ S0
    Mr[n0**2:, :] = C
    rhs = np.zeros((n0**2 + n0))
    rhs[:n0**2] = S1
    rhs[n0**2:] = 1

    # 4. Resolver NNLS
    p_flat = nnls(Mr, rhs)[0]

    # 5. Recomponer y normalizar
    Pr = p_flat.reshape(n0, n0).T
    col_sums = Pr.sum(axis=0)
    col_sums[col_sums == 0] = 1  # Proteccion contra division por cero
    Pr = Pr / col_sums
    return Pr
\end{python}
\caption{Implementación del algoritmo 'SRep' para estimar
matrices de transición estocásticas mediante NNLS.}

\end{listing}

\begin{listing}[htbp]
\begin{python}
# Simulacion del proceso de Markov
T = 20
p0 = np.zeros((4, T))
p0[0, 0] = 1  # Estado inicial: s1

for j in range(T - 1):
    p0[:, j+1] = P_hat @ p0[:, j]
\end{python}
\caption{Simulación de la evolución temporal de una cadena de
Markov a partir de la matriz de transición $\hat{\mat{P}}$.}

\end{listing}

\renewcommand{\thesection}{C.\arabic{section}}

\renewcommand{\thesubsection}{C.\arabic{section}.\arabic{subsection}}
\renewcommand{\thefigure}{C.\arabic{figure}}
\renewcommand{\thetable}{C.\arabic{table}}

## Código Fuente: Computación de Reservorio (SSRC)

Los siguientes listados implementan las funciones centrales del modelo
SSRC descrito en el Capítulo~[Ref: cap:sec_ssrc].
El código fuente completo está disponible en el repositorio
del proyecto.

\begin{listing}[htbp]
\begin{python}
import numpy as np
from typing import Tuple, Optional

def create_reservoir(
    input_dim: int,
    reservoir_dim: int,
    spectral_radius: float,
    input_scale: float = 1.0,
    sparsity: float = 0.9,
    seed: Optional[int] = None
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Genera las matrices fijas del reservorio (W_in, W_res).
    Implementa la Definición 6.3 del capítulo SSRC.

    Parametros
    ----------
    input_dim : int    -- Dimension de la entrada (d=1 para TCRA).
    reservoir_dim : int -- Dimension del reservorio (D).
    spectral_radius : float -- Radio espectral rho_res < 1 (ESP).
    input_scale : float -- Escala omega de W_in.
    sparsity : float   -- Fraccion de ceros en W_res.
    seed : int          -- Semilla para reproducibilidad.

    Retorna
    -------
    W_in : ndarray (D, d), W_res : ndarray (D, D)
    """
    rng = np.random.RandomState(seed)

    # W_in: Uniforme en [-omega, omega]
    W_in = rng.uniform(-input_scale, input_scale,
                       size=(reservoir_dim, input_dim))

    # W_res: Dispersa aleatoria, reescalada espectralmente
    W_res_raw = rng.randn(reservoir_dim, reservoir_dim)
    mask = rng.rand(reservoir_dim, reservoir_dim) > sparsity
    W_res_raw *= mask

    # Reescalar: W_res <- rho_res * W_res^(0) / rho(W_res^(0))
    eigenvalues = np.linalg.eigvals(W_res_raw)
    rho_current = np.max(np.abs(eigenvalues))

    if rho_current > 0:
        W_res = spectral_radius * W_res_raw / rho_current
    else:
        W_res = W_res_raw

    return W_in, W_res
\end{python}
\caption{Generación de las matrices fijas del reservorio
(Definición~[Ref: def:ssrc]).  El reescalamiento espectral garantiza
$\rho(\mat{W}_{\mathrm{res}}) = \rho_{\mathrm{res}} < 1$, condición
suficiente para la propiedad ESP (Teorema~[Ref: teo:esp_suficiente]).}

\end{listing}

\begin{listing}[htbp]
\begin{python}
import numpy as np

def propagate_reservoir(
    alpha_series, W_in, W_res,
    washout=50, leak_rate=1.0
):
    """
    Propaga la senal TCRA alpha_t a traves del reservorio.
    Implementa el Leaky-ESN (Definición 6.2):
       h_t = (1-a)*h_{t-1} + a*tanh(W_in*u_t + W_res*h_{t-1})
    Cuando a = 1.0, se reduce al ESN clasico.

    Parametros
    ----------
    alpha_series : ndarray (T,)  -- Serie TCRA alpha_t.
    W_in : ndarray (D, d)        -- Matriz de entrada.
    W_res : ndarray (D, D)       -- Matriz de reservorio.
    washout : int                 -- Pasos transitorios.
    leak_rate : float             -- Factor de fuga a in (0, 1].

    Retorna
    -------
    H : ndarray (D, T_eff) -- Estados post-washout.
    """
    T = len(alpha_series)
    D = W_res.shape[0]
    U = alpha_series.reshape(-1, 1)

    H_all = np.zeros((D, T))
    h = np.zeros(D)
    a = leak_rate

    for t in range(T):
        h = (1.0-a)*h + a*np.tanh(W_in @ U[t] + W_res @ h)
        H_all[:, t] = h

    H = H_all[:, washout:]
    return H
\end{python}
\caption{Propagación de la señal $\alpha_t$ a través del reservorio
Leaky-ESN.  Los primeros 'washout' estados se descartan
para eliminar transitorios de la condición inicial $\vect{h}_0 = \vect{0}$.}

\end{listing}

\renewcommand{\thesection}{D.\arabic{section}}

\renewcommand{\thesubsection}{D.\arabic{section}.\arabic{subsection}}
\renewcommand{\thefigure}{D.\arabic{figure}}
\renewcommand{\thetable}{D.\arabic{table}}

## Código para Generación de Figuras

Los siguientes listados contienen los scripts de Python utilizados
para generar las figuras principales de la tesis.  Todos los gráficos
usan la paleta de colores Nord y calidad IEEE (600~DPI).

\begin{listing}[htbp]
\begin{python}
"""
Genera la figura 'evolucion_probabilidades_markov.png'
Evolucion de probabilidades de la Matriz P estimada (K=4 regimenes)
con paleta Nord para la tesis.
"""
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# Paleta Nord
NORD_0  = "#2E3440"
NORD_3  = "#4C566A"
NORD_10 = "#5E81AC"   # Frost Blue -- alza fuerte
NORD_11 = "#BF616A"   # Aurora Red -- caida
NORD_13 = "#EBCB8B"   # Aurora Yellow -- subida moderada
NORD_14 = "#A3BE8C"   # Aurora Green -- estable

plt.style.use('default')
plt.rcParams.update({
    'font.family': 'serif',
    'font.serif': ['Times New Roman', 'DejaVu Serif'],
    'font.size': 12,
})

# Matriz P estimada (ejemplo Super, K=4)
P = np.array([
    [0.20, 0.30, 0.30, 0.05],
    [0.35, 0.33, 0.43, 0.05],
    [0.40, 0.32, 0.22, 0.05],
    [0.05, 0.05, 0.05, 0.85],
])
P = P / P.sum(axis=0, keepdims=True)

# Estado inicial: p0 = (1, 0, 0, 0)
K = P.shape[0]
p0 = np.zeros(K)
p0[0] = 1.0

# Simular evolucion
n_steps = 20
trajectory = np.zeros((K, n_steps))
trajectory[:, 0] = p0
for t in range(1, n_steps):
    trajectory[:, t] = P @ trajectory[:, t-1]

colors = [NORD_11, NORD_14, NORD_13, NORD_10]
labels = ['caida', 'estable', 'subida', 'alza fuerte']

fig, ax = plt.subplots(figsize=(12, 7))
for k in range(K):
    ax.plot(range(n_steps), trajectory[k, :],
            marker='D', markersize=5, linewidth=2.5,
            color=colors[k], label=labels[k])
ax.set_xlabel('Tiempo')
ax.set_ylabel('Probabilidad')
ax.set_title('Evolucion de Probabilidades (Matriz P Estimada)')
ax.legend(fontsize=12)
ax.grid(True, alpha=0.3, linestyle=':')
plt.savefig('evolucion_probabilidades_markov.png',
            dpi=600, bbox_inches='tight')
\end{python}
\caption{Script para generar la Figura~[Ref: fig:evolucion_probabilidades_markov]:
evolución temporal de las probabilidades de ocupación de estados bajo
la dinámica de la matriz $\hat{\mat{P}}$.}

\end{listing}

\begin{listing}[htbp]
\begin{python}
"""
Genera figura panel 2x2 con evoluciones de probabilidades
para las 4 series de combustibles (Superior, Regular, Diésel, Kerosene).
"""
import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import nnls

# Cargar datos de cada serie
def load_transition_matrix(fuel_name, output_dir):
    """Carga la matriz de transicion desde archivos .npy generados
    por el pipeline 03_modeling.py."""
    P = np.load(f'{output_dir}/{fuel_name}_P_hat.npy')
    return P

def simulate_evolution(P, n_steps=100, initial_state=0):
    """Simula la evolucion de la distribucion de probabilidad
    a partir de un estado inicial dado."""
    K = P.shape[0]
    p0 = np.zeros(K)
    p0[initial_state] = 1.0

    traj = np.zeros((K, n_steps))
    traj[:, 0] = p0
    for t in range(1, n_steps):
        traj[:, t] = P @ traj[:, t-1]
    return traj

# Configuracion de la grilla 2x2
fuels = ['Super', 'Regular', 'Diesel', 'Kerosene']
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
for idx, (fuel, ax) in enumerate(zip(fuels, axes.flat)):
    P = load_transition_matrix(fuel, 'outputs/models')
    traj = simulate_evolution(P, n_steps=100)
    K = P.shape[0]
    for k in range(K):
        ax.plot(range(100), traj[k, :], linewidth=2)
    ax.set_title(fuel, fontweight='bold')
    ax.set_xlabel('Tiempo')
    ax.set_ylabel('Probabilidad')
    ax.grid(True, alpha=0.3)
plt.suptitle('Evolucion de Probabilidades (Matriz P Estimada)',
             fontweight='bold', fontsize=16)
plt.tight_layout()
plt.savefig('allseriecombustiblesNNLS.png', dpi=600)
\end{python}
\caption{Script para generar la Figura~[Ref: fig:evolucion_probabilidades_combustibles]:
panel $2\times 2$ con la evolución de probabilidades para las cuatro
series de combustibles.}

\end{listing}

\end{appendices}

