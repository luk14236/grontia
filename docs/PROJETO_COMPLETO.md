# Grontia — Documentação Completa do Projeto

> Última atualização: 02/10/2026

---

## Índice

1. [O que é o Grontia](#1-o-que-é-o-grontia)
2. [Stack tecnológica](#2-stack-tecnológica)
3. [Arquitetura geral](#3-arquitetura-geral)
4. [Estrutura de pastas](#4-estrutura-de-pastas)
5. [Camada de Orquestração — Airflow](#5-camada-de-orquestração--airflow)
6. [Camada de Storage — SeaweedFS (S3)](#6-camada-de-storage--seaweedfs-s3)
7. [Camada de Processamento — PySpark](#7-camada-de-processamento--pyspark)
8. [Camada de Analytics — dbt](#8-camada-de-analytics--dbt)
9. [Data Warehouse — DuckDB + MotherDuck](#9-data-warehouse--duckdb--motherduck)
10. [Fontes de dados](#10-fontes-de-dados)
11. [CI/CD — GitHub Actions](#11-cicd--github-actions)
12. [Como subir o ambiente local](#12-como-subir-o-ambiente-local)
13. [Como rodar o pipeline completo](#13-como-rodar-o-pipeline-completo)
14. [O que está funcional](#14-o-que-está-funcional)
15. [O que ainda falta](#15-o-que-ainda-falta)
16. [Modelo de dados (ERD)](#16-modelo-de-dados-erd)
17. [Glossário](#17-glossário)

---

## 1. O que é o Grontia

O Grontia é um projeto de **engenharia de dados educacional** construído do zero com ferramentas modernas e open source. O objetivo é aprender na prática como funciona um pipeline de dados de ponta a ponta, usando dados públicos reais do governo holandês como fonte.

**Princípios do projeto:**
- Custo zero — nenhum serviço pago obrigatório
- Open source em toda a stack
- Dados reais, não simulados
- Código limpo, documentado e versionado
- Somente dados abertos — o Grontia nunca faz scraping de portais de anúncios (Funda, Pararius etc.)

**Fora do escopo:** Azure, Databricks, Snowflake e qualquer serviço pago. Versões anteriores do projeto usavam Azure + Databricks; esse código foi removido e continua disponível apenas no histórico do git.

---

## 2. Stack tecnológica

| Camada | Tecnologia | Versão | Por que foi escolhida |
|---|---|---|---|
| Orquestração | Apache Airflow | 3.0.0 | Padrão de mercado, gratuito, Docker-friendly |
| Storage bronze/silver | SeaweedFS | latest | S3-compatível, roda local no Docker (substituiu o MinIO, cujas imagens deixaram de ser publicadas) |
| Processamento | PySpark | 3.x | Transformações distribuídas, lê S3 nativamente |
| Analytics SQL | dbt-duckdb | 1.10.x | SQL declarativo, lineage automático, gratuito |
| Data Warehouse local | DuckDB | 1.5.x | Analítico, embutido, zero configuração |
| Data Warehouse nuvem | MotherDuck | cloud | DuckDB na nuvem, free tier permanente |
| CI/CD | GitHub Actions | — | Integrado ao repositório, gratuito |

---

## 3. Arquitetura geral

O pipeline segue o padrão **Medallion Architecture** (Bronze → Silver → Gold):

```
╔══════════════════════════════════════════════════════════════════╗
║                        FONTES DE DADOS                          ║
║   CBS (OData)   PDOK (WFS)   KNMI (CSV)   NDW (XML.gz)         ║
╚══════════════════╦═══════════════════════════════════════════════╝
                   ║ HTTP requests diários
                   ▼
╔══════════════════════════════════════════════════════════════════╗
║              AIRFLOW  (orquestração)                            ║
║   DAG: 30_multi_source_daily_ingestion                          ║
║   Schedulado: @daily                                             ║
╚══════════════════╦═══════════════════════════════════════════════╝
                   ║ boto3 / S3 API
                   ▼
╔══════════════════════════════════════════════════════════════════╗
║        SeaweedFS (S3) — BRONZE LAYER  (s3://bronze/)            ║
║   Dados crus: JSON, CSV, XML                                    ║
║   Particionado por: source/dataset/ingestion_date=YYYY-MM-DD/   ║
╚══════════════════╦═══════════════════════════════════════════════╝
                   ║ PySpark lê via s3a://
                   ▼
╔══════════════════════════════════════════════════════════════════╗
║         PySpark — bronze_to_silver.py                           ║
║   - Desaninha JSON                                              ║
║   - Remove duplicatas (dedup por chave + ingestion_timestamp)   ║
║   - Adiciona metadados (source, dataset_name, ingestion_date)   ║
║   - Escreve Parquet no silver                                   ║
╚══════════════════╦═══════════════════════════════════════════════╝
                   ║ escreve Parquet via s3a://
                   ▼
╔══════════════════════════════════════════════════════════════════╗
║        SeaweedFS (S3) — SILVER LAYER  (s3://silver/)            ║
║   Dados limpos e normalizados em Parquet                        ║
║   Estrutura: source/dataset_name/**/*.parquet                   ║
╚══════════════════╦═══════════════════════════════════════════════╝
                   ║ dbt lê via httpfs (read_parquet)
                   ▼
╔══════════════════════════════════════════════════════════════════╗
║              dbt-duckdb — camada analytics                      ║
║   STAGING (views): renomeia colunas, casteia tipos              ║
║   MARTS (tables): joins analíticos, agregações                  ║
╚══════════════════╦═══════════════════════════════════════════════╝
                   ║ materializa tabelas
                   ▼
╔══════════════════════════════════════════════════════════════════╗
║    MotherDuck / DuckDB  — DATA WAREHOUSE                        ║
║   Schemas: STAGING, MARTS                                       ║
║   Pronto para consumo por BI tools                              ║
╚══════════════════════════════════════════════════════════════════╝
```

---

## 4. Estrutura de pastas

```
grontia/
│
├── orchestration/airflow/          # Tudo relacionado ao Airflow
│   ├── dags/                       # DAGs (pipelines agendados)
│   │   ├── 00_smoke_test.py        # Teste de saúde do Airflow
│   │   ├── 01_read_datasets_catalog.py  # Valida o catalog YAML
│   │   ├── 11_cbs_bronze_ingestion_local.py  # Ingestão CBS em disco (legado)
│   │   └── 30_multi_source_daily_ingestion.py  ← DAG PRINCIPAL
│   │
│   ├── include/configs/
│   │   └── datasets.yml            # Catálogo centralizado de datasets
│   │
│   ├── Dockerfile                  # Imagem Airflow customizada
│   ├── docker-compose.yml          # Stack local completa
│   └── requirements.txt            # Dependências Python do Airflow
│
├── processing/local/               # Scripts PySpark
│   └── bronze_to_silver.py         # Transforma bronze → silver (Parquet); o gold é feito no dbt
│
├── analytics/dbt/                  # Projeto dbt
│   ├── models/
│   │   ├── staging/                # Views que limpam o silver
│   │   │   ├── cbs/                # 8 modelos CBS
│   │   │   ├── pdok/               # 2 modelos PDOK
│   │   │   ├── knmi/               # 1 modelo KNMI
│   │   │   └── ndw/                # 1 modelo NDW
│   │   └── marts/                  # Tabelas analíticas finais
│   │       ├── mart_regional_dashboard.sql
│   │       ├── mart_housing_market.sql
│   │       ├── mart_weather_summary.sql
│   │       └── mart_neighbourhood_reference.sql
│   ├── dbt_project.yml             # Configuração do projeto dbt
│   └── profiles.yml                # Conexões (motherduck + dev)
│
├── .github/workflows/              # CI/CD
│   ├── ci-cd.yml                   # Pipeline principal
│   ├── pr-validation.yml           # Validação de PRs
│   └── security-scan.yml           # Scan semanal de segurança
│
├── docs/                           # Documentação
│   ├── PROJETO_COMPLETO.md         ← este arquivo
│   └── architecture/               # Decisões e diagramas (Mermaid, ERD)
│
└── .env.example                    # Template de variáveis de ambiente
```

---

## 5. Camada de Orquestração — Airflow

### Como funciona

O Airflow é o "maestro" do pipeline. Ele agenda e executa as tarefas de ingestão de dados todos os dias automaticamente.

### Como está configurado

O Airflow roda em **Docker Compose** junto com o storage S3:

| Serviço | Função |
|---|---|
| `postgres` | Banco de metadados do Airflow |
| `airflow-api-server` | Interface web (porta 8080) |
| `airflow-scheduler` | Dispara as tarefas no horário |
| `airflow-dag-processor` | Processa e valida os DAGs |
| `s3` / `s3-init` | SeaweedFS (API S3 na porta 9000, filer na 8888) e criação dos buckets |

### O DAG principal: `30_multi_source_daily_ingestion`

É o coração do pipeline de ingestão. Roda **uma vez por dia** e ingere dados de todas as 4 fontes em paralelo. As tasks são criadas a partir do `datasets.yml`; o Airflow numera as repetidas (`ingest_cbs_dataset__1`, `__2`…).

**Fluxo do DAG:**
```
start
  ├── ingest_cbs_dataset   ×8 (um por dataset CBS do catálogo) → validate_ingestion
  ├── ingest_pdok_dataset  ×2 (bag_pand, bag_verblijfsobject)  → validate_ingestion
  ├── ingest_knmi_dataset  ×1 (daily_weather_all_stations)     → validate_ingestion
  └── ingest_ndw_dataset   ×1 (current_traffic_flow)           → validate_ingestion
end
```

O DAG `11_cbs_bronze_ingestion_local` é anterior: grava CBS em disco (`include/output/`) e foi substituído pelo DAG 30.

Cada fonte tem sua própria lógica de ingestão (detalhada na seção 10).

### Como cada ingestão funciona

1. Airflow faz request HTTP para a API da fonte
2. Salva o arquivo cru (JSON, CSV ou XML) no bucket bronze
3. Salva um `_summary.json` com metadados da ingestão (status, records, data)
4. Task `validate_ingestion` checa se o status foi `success`

### Catálogo de datasets (`datasets.yml`)

Arquivo YAML centralizado que define todas as fontes, endpoints, colunas e tabelas silver. O DAG lê esse arquivo em runtime para saber quais datasets ingerir. Isso significa que **para adicionar um novo dataset, basta editar o YAML** sem tocar no código Python.

---

## 6. Camada de Storage — SeaweedFS (S3)

### O que é

SeaweedFS é um servidor de objetos S3-compatível que roda em Docker. Ele substituiu o MinIO (cujas imagens oficiais deixaram de ser publicadas); por isso as variáveis continuam se chamando `MINIO_*` e as credenciais continuam `minioadmin`/`minioadmin`. Qualquer código escrito para S3 funciona sem mudanças.

### Estrutura dos buckets

```
bronze/                             ← dados crus
├── cbs/cbs_bronze/
│   └── neighbourhood_key_figures/
│       └── ingestion_date=2026-06-28/
│           ├── tableinfos.json
│           ├── dataproperties.json
│           ├── typeddataset.json   ← dados principais
│           └── _summary.json
├── pdok/pdok_bronze/
│   └── bag_pand/
│       └── ingestion_date=2026-06-28/
│           ├── page_0000.json      ← paginação WFS
│           ├── page_0001.json
│           └── _summary.json
├── knmi/knmi_bronze/
│   └── daily_weather_all_stations/
│       └── ingestion_date=2026-06-28/
│           ├── data.csv            ← CSV original (header "# STN,..." recuperado)
│           └── _summary.json
└── ndw/ndw_bronze/
    └── current_traffic_flow/
        └── ingestion_date=2026-06-28/
            ├── data.xml            ← XML descomprimido
            ├── data.json           ← convertido para JSON
            └── _summary.json

silver/                             ← dados limpos em Parquet
├── cbs/
│   ├── neighbourhood_key_figures/**/*.parquet
│   ├── average_woz_value/**/*.parquet
│   ├── housing_stock/**/*.parquet
│   └── household_income/**/*.parquet
├── pdok/
│   ├── bag_pand/**/*.parquet
│   └── bag_verblijfsobject/**/*.parquet
├── knmi/
│   └── daily_weather_all_stations/**/*.parquet
└── ndw/
    └── current_traffic_flow/**/*.parquet
```

### Como acessar

- **Navegador de arquivos (SeaweedFS Filer)**: http://localhost:8888/buckets/
- **API S3**: http://localhost:9000

---

## 7. Camada de Processamento — PySpark

### O que faz

O script `processing/local/bronze_to_silver.py` lê os arquivos crus do bucket bronze, aplica transformações e salva Parquet no bucket silver.

### Transformações aplicadas

Para cada dataset:

1. **Leitura**: lê JSON/CSV do bronze via `s3a://`
2. **Desaninhamento**: expande arrays JSON (`explode("value")`)
3. **Metadados**: adiciona colunas `ingestion_date`, `ingestion_timestamp`, `source`, `dataset_name`
4. **Deduplicação**: remove linhas duplicadas usando `ROW_NUMBER()` particionado pela chave natural do dataset, mantendo sempre o registro mais recente
5. **Escrita**: salva em Parquet com overwrite dinâmico por partição (`ingestion_date`), então reprocessar uma data não duplica dados

### Configuração S3 no Spark

O Spark precisa de configurações específicas para acessar o SeaweedFS como S3:

```python
spark = SparkSession.builder
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")  # obrigatório fora da AWS
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,...")
    .getOrCreate()
```

### Como rodar

```bash
# Com data específica
python processing/local/bronze_to_silver.py 2026-06-28

# Com data de hoje
python processing/local/bronze_to_silver.py
```

---

## 8. Camada de Analytics — dbt

### O que é dbt

dbt (data build tool) permite escrever transformações SQL com controle de versão, testes automáticos e documentação gerada automaticamente. Cada arquivo `.sql` vira uma view ou tabela no DW.

### Dois targets configurados

| Target | Quando usar | Onde materializa |
|---|---|---|
| `dev` | Desenvolvimento local, testes | DuckDB local `/tmp/grontia_dev.duckdb` |
| `motherduck` | Produção, CI/CD | MotherDuck (DuckDB na nuvem) |

### Camadas dbt

#### STAGING (views)
Views que ficam "em cima" do silver. Fazem apenas:
- Renomear colunas (nomes holandeses → nomes em inglês)
- Castear tipos (`VARCHAR` → `INTEGER`, `DOUBLE`, `DATE`)
- Filtrar linhas inválidas (`WHERE region_code IS NOT NULL`)

São views, ou seja, **não copiam dados** — leem direto do Parquet no bucket silver.

| Model | Dataset | Campos principais |
|---|---|---|
| `stg_cbs_neighbourhood_key_figures` | CBS 85984NED | region_code, period_code, population, households |
| `stg_cbs_average_woz_value` | CBS 85036NED | region_code, period_code, avg_woz_value_eur |
| `stg_cbs_housing_stock` | CBS 82900NED | region_code, period_code, dwelling_type, number_of_dwellings |
| `stg_cbs_household_income` | CBS 86161NED | region_code, period_code, avg/median_household_income_eur |
| `stg_cbs_sale_prices_annual` | CBS 83625NED | region_code, period_code, preço médio de venda |
| `stg_cbs_sale_price_index` | CBS 85792NED | period_code, índice de preço trimestral |
| `stg_cbs_rent_increase` | CBS 70675ned | period_code, aumento de aluguel |
| `stg_cbs_municipality_regions` | CBS 85755NED | município → província |
| `stg_pdok_bag_pand` | PDOK BAG Pand | id, geometry, bouwjaar, status |
| `stg_pdok_bag_verblijfsobject` | PDOK BAG Verblijfsobject | id, geometry, gebruiksdoel, oppervlakte_m2 |
| `stg_knmi_daily_weather` | KNMI daggegevens | station_code, date, temp_avg_c, precipitation_mm |
| `stg_ndw_traffic_flow` | NDW XML.gz | id, vehicle_flow_rate, avg_vehicle_speed_kmh |

#### MARTS (tables)
Tabelas analíticas finais. São **físicas no DW** (materializadas), com joins entre as views de staging.

| Mart | O que responde | Join principal |
|---|---|---|
| `mart_regional_dashboard` | Dashboard unificado por região | CBS neighbourhood + WOZ + income via (region_code, period_code) |
| `mart_housing_market` | Mercado imobiliário por tipo de imóvel | CBS housing_stock + WOZ via (region_code, period_code) |
| `mart_weather_summary` | Resumo meteorológico diário | KNMI direto, sem join |
| `mart_neighbourhood_reference` | Preço médio de venda estimado hoje por município, wijk e buurt | Preços de venda + índice de preço + WOZ + municípios |

### Comandos dbt

```bash
cd analytics/dbt

# Verificar se os models são válidos (sem rodar)
dbt parse --target dev

# Rodar todos os models (local)
dbt run --target dev --profiles-dir .

# Rodar e testar (local)
dbt run --target dev && dbt test --target dev

# Produção (MotherDuck)
dbt run --target motherduck
dbt test --target motherduck

# Ver documentação gerada
dbt docs generate --target dev
dbt docs serve
```

---

## 9. Data Warehouse — DuckDB + MotherDuck

### DuckDB local

DuckDB é um banco de dados analítico embutido — funciona como uma biblioteca Python, sem servidor, sem instalação. Ideal para desenvolvimento.

- Arquivo: `/tmp/grontia_dev.duckdb`
- Lê Parquet direto do bucket silver via extensão `httpfs`
- Mesma sintaxe SQL do MotherDuck

### MotherDuck (nuvem)

MotherDuck é DuckDB hospedado na nuvem com free tier permanente (10 GB). É o DW de produção do projeto.

**Para usar:**
1. Criar conta em https://app.motherduck.com
2. Gerar token em Settings → Tokens
3. Configurar:

```bash
# .env
MOTHERDUCK_TOKEN=seu_token_aqui
```

```bash
# GitHub Secrets (para CI/CD)
MOTHERDUCK_TOKEN = seu_token_aqui
MINIO_ENDPOINT   = https://seu-s3-publico.com  # ou túnel (ngrok, cloudflared)
MINIO_ACCESS_KEY = minioadmin
MINIO_SECRET_KEY = minioadmin
```

**Schemas criados pelo dbt:**

| Schema | Conteúdo |
|---|---|
| `STAGING` | 12 views (uma por dataset) |
| `MARTS` | 4 tabelas analíticas finais |

---

## 10. Fontes de dados

### CBS — Statistics Netherlands ✅ Funcional

**O que é**: Instituto de estatísticas da Holanda. Dados socioeconômicos por região.

**API**: OData REST API pública, sem autenticação.

**Como funciona a ingestão**:
```
GET https://opendata.cbs.nl/ODataFeed/odata/{TABLE_ID}/TypedDataSet
→ JSON com array "value"; segue odata.nextLink até o fim
  (a ODataApi recusa consultas com mais de 10k linhas)
→ Filtro opcional por dataset (odata_filter) para reduzir dimensões aos totais
→ Salvo em bronze/cbs/cbs_bronze/{dataset}/ingestion_date={data}/typeddataset.json
```

**Datasets ingeridos**:

| Dataset | Table ID | O que contém |
|---|---|---|
| neighbourhood_key_figures | 85984NED | População, domicílios, renda por bairro/município (2024) |
| average_woz_value | 85036NED | Valor médio de imóveis (WOZ) por região |
| housing_stock | 82900NED | Estoque de imóveis por tipo e região |
| household_income | 86161NED | Renda média e mediana por domicílio e região |
| sale_prices_annual | 83625NED | Preço médio de venda anual por região |
| sale_price_index_quarterly | 85792NED | Índice de preço de venda trimestral |
| rent_increase | 70675ned | Aumento de aluguel |
| municipality_regions | 85755NED | Município → província |

---

### PDOK — Geospatial Data ⚠️ Funcional com limitações

**O que é**: Portal de dados geoespaciais do governo holandês. Dados de edificações e endereços (BAG).

**API**: WFS 2.0 (Web Feature Service), sem autenticação.

**Como funciona a ingestão**:
```
GET https://service.pdok.nl/lv/bag/wfs/v2_0?...&startIndex=0&count=1000
→ GeoJSON com array "features"
→ Loop até esgotar todos os registros (paginação por startIndex)
→ Cada página salva como page_0000.json, page_0001.json, ...
```

**Limitação atual**: o WFS recusa `startIndex > 50000`, então cada dataset é limitado a uma `bbox` (RD New / EPSG:28992) no catálogo. O padrão cobre 2×2 km do centro de Amsterdam. Para o país inteiro será preciso outra estratégia (download do extrato BAG completo ou várias bboxes).

**Datasets ingeridos**:

| Dataset | O que contém |
|---|---|
| bag_pand | Edificações (polígono, ano de construção, status) |
| bag_verblijfsobject | Objetos de uso/residência (área, finalidade, status) |

---

### KNMI — Weather Data ✅ Funcional

**O que é**: Instituto meteorológico holandês. Dados climáticos diários de todas as estações.

**API**: POST para endpoint CSV, sem autenticação.

**Como funciona a ingestão**:
```
POST https://www.daggegevens.knmi.nl/klimatologie/daggegevens
  body: start=20260621&end=20260627&vars=ALL&stns=ALL
→ CSV com linhas de metadados iniciadas por "#"
→ Parser custom recupera o header "# STN,..." e descarta os demais comentários
→ Salvo como data.csv; o silver lê todas as colunas como string e o dbt faz o cast
```

**Colunas principais** (em décimos de unidade — padrão KNMI):
- `TG` = temperatura média (÷10 = °C)
- `TN/TX` = temperatura mínima/máxima
- `RH` = precipitação (÷10 = mm)
- `FG` = vento médio (÷10 = m/s)
- `SQ` = horas de sol (÷10 = horas)

---

### NDW — Traffic Data ✅ Funcional (snapshot diário)

**O que é**: Data warehouse nacional de tráfego holandês. Velocidade e fluxo por ponto de medição, atualizados a cada minuto na fonte — hoje o Grontia pega só um snapshot por dia.

**Formato**: XML comprimido em `.gz` (schema DATEX II).

**Como funciona a ingestão**:
```
GET https://opendata.ndw.nu/trafficspeed.xml.gz
→ Arquivo .gz baixado
→ Parser DATEX II em streaming (iterparse) → lista de dicts
→ Salvo como data.xml (descomprimido) + data.json (convertido)
```

---

## 11. CI/CD — GitHub Actions

### Workflows disponíveis

#### `ci-cd.yml` — Pipeline principal
Roda em **push para main** e em **pull requests**.

```
validate
  ├── py_compile em todos os DAGs
  └── yaml.safe_load no datasets.yml
          ↓
dbt-validate
  └── dbt parse --target dev
          ↓ (apenas em push para main)
dbt-deploy
  ├── dbt run --target motherduck
  └── dbt test --target motherduck
          ↓ (paralelo com dbt-deploy)
security-scan
  ├── bandit (vulnerabilidades Python)
  └── safety (vulnerabilidades de dependências)
```

#### `pr-validation.yml` — Validação de PRs
Roda em PRs que tocam `orchestration/`, `analytics/dbt/` ou `processing/`.
- Valida DAGs (DagBag import)
- Valida catalog YAML
- `dbt parse` com target dev

#### `security-scan.yml` — Scan semanal
Toda segunda-feira às 2h UTC:
- Bandit (code security)
- Safety (dependency vulnerabilities)

### Secrets necessários no GitHub

Para o `dbt-deploy` funcionar no CI, configure em **Settings → Secrets → Actions**:

```
MOTHERDUCK_TOKEN   = token do MotherDuck
MINIO_ENDPOINT     = URL pública do storage S3 (ou túnel)
MINIO_ACCESS_KEY   = minioadmin
MINIO_SECRET_KEY   = minioadmin
```

---

## 12. Como subir o ambiente local

### Pré-requisitos
- Docker Desktop instalado e rodando
- Python 3.11+
- Java 11+ (para PySpark)

### Passo 1 — Clonar e configurar variáveis

```bash
git clone https://github.com/seu-usuario/grontia.git
cd grontia

# Copiar template de variáveis
cp .env.example orchestration/airflow/.env
# Editar com seu MOTHERDUCK_TOKEN se tiver
```

### Passo 2 — Subir Airflow + storage S3

```bash
cd orchestration/airflow
docker compose up -d

# Aguardar inicialização (~30 segundos)
docker compose ps
```

**Serviços disponíveis:**
- Airflow UI: http://localhost:8080 (admin / admin)
- Storage S3 (SeaweedFS): API em http://localhost:9000, navegador em http://localhost:8888/buckets/ (credenciais S3: minioadmin / minioadmin)

### Passo 3 — Instalar dependências Python locais

```bash
# Para PySpark
pip install pyspark

# Para dbt
pip install dbt-duckdb

# Para rodar o DAG localmente (testes)
pip install boto3 pyyaml requests
```

---

## 13. Como rodar o pipeline completo

### Etapa 1 — Ingestão (Airflow)

No Airflow UI (http://localhost:8080):
1. Ir em **DAGs**
2. Habilitar o DAG `30_multi_source_daily_ingestion`
3. Clicar em **Trigger DAG** para rodar manualmente

Ou via CLI:
```bash
docker compose exec airflow-scheduler airflow dags trigger 30_multi_source_daily_ingestion
```

Após rodar, verificar os arquivos em http://localhost:8888/buckets/bronze/

### Etapa 2 — Transformação bronze → silver (PySpark)

```bash
# Com a data em que o DAG rodou
python processing/local/bronze_to_silver.py 2026-06-28

# Verificar em http://localhost:8888/buckets/silver/
```

### Etapa 3 — Analytics (dbt)

```bash
cd analytics/dbt

# Local (DuckDB)
dbt run --target dev
dbt test --target dev

# Nuvem (MotherDuck) — requer MOTHERDUCK_TOKEN no .env
dbt run --target motherduck
```

### Etapa 4 — Consultar os dados

**DuckDB local:**
```python
import duckdb
con = duckdb.connect("/tmp/grontia_dev.duckdb")
con.execute("SELECT * FROM main_MARTS.mart_regional_dashboard LIMIT 10").df()
```

**MotherDuck:**
```python
import duckdb
con = duckdb.connect("md:grontia?motherduck_token=SEU_TOKEN")
con.execute("SELECT * FROM MARTS.mart_regional_dashboard LIMIT 10").df()
```

---

## 14. O que está funcional

| Componente | Status | Observação |
|---|---|---|
| Docker Compose (Airflow 3 + SeaweedFS) | ✅ Pronto | `docker compose up -d` |
| DAG 30 — CBS ingestão | ✅ Pronto | 8 datasets, ODataFeed com nextLink, re-execução idempotente |
| DAG 30 — PDOK ingestão com paginação | ✅ Pronto | Loop WFS com startIndex, limitado a uma bbox |
| DAG 30 — KNMI ingestão CSV | ✅ Pronto | POST + parser header # |
| DAG 30 — NDW ingestão XML.gz | ✅ Pronto | Parser DATEX II em streaming |
| bronze_to_silver.py | ✅ Pronto | PySpark, dedup, Parquet, overwrite por partição |
| dbt staging models (CBS) | ✅ Pronto | 8 models com testes |
| dbt staging models (PDOK) | ✅ Pronto | 2 models |
| dbt staging models (KNMI) | ✅ Pronto | 1 model com conversão de unidades |
| dbt staging models (NDW) | ✅ Pronto | 1 model |
| dbt mart_regional_dashboard | ✅ Pronto | Join CBS região+período |
| dbt mart_housing_market | ✅ Pronto | Join housing+WOZ |
| dbt mart_weather_summary | ✅ Pronto | KNMI direto |
| dbt mart_neighbourhood_reference | ✅ Pronto | Preço médio estimado atual por município, wijk e buurt (base para avaliar endereços) |
| profiles.yml (dev + motherduck) | ✅ Pronto | Dois targets configurados |
| CI/CD GitHub Actions | ✅ Pronto | validate + dbt parse; o deploy depende do MotherDuck (seção 15) |
| datasets.yml (catalog) | ✅ Pronto | Todas as fontes sem duplicatas |
| .env.example | ✅ Pronto | Template completo |

---

## 15. O que ainda falta

| Item | Prioridade | O que é necessário |
|---|---|---|
| MOTHERDUCK_TOKEN | 🔴 Alta | Criar conta em app.motherduck.com |
| Storage S3 público (para CI/CD) | 🟡 Média | Túnel ou VPS para o dbt-deploy acessar o silver |
| Mart de tráfego (NDW) | 🟡 Média | Criar `mart_traffic_flow.sql` quando silver NDW estiver populado |
| Testes de qualidade de dados | 🟡 Média | Hoje são 24 testes genéricos; KNMI, NDW e PDOK têm só 1–2 cada. Faltam freshness e ranges |
| PDOK além da bbox | 🟡 Média | Cobrir o país inteiro (extrato BAG ou várias bboxes) |
| Teste de DAGs | 🟡 Média | `tests/dags/test_dag_example.py` ainda é o exemplo padrão |
| Bandit bloqueante no CI | 🟢 Baixa | Hoje roda com `--exit-zero` e nunca reprova o build |
| Graphviz instalado | 🟢 Baixa | Para gerar imagem do ERD via `generate_erd.py` |
| dbt docs | 🟢 Baixa | `dbt docs generate && dbt docs serve` |
| Streaming NDW | ⏳ Futuro | Pipeline near real-time do tráfego |
| Avaliação de endereço | ⏳ Futuro | Usar `mart_neighbourhood_reference` para avaliar endereço + preço informados pelo usuário |

---

## 16. Modelo de dados (ERD)

### Chave de relacionamento principal

Todos os datasets CBS se relacionam pela chave composta:
```
(region_code, period_code)
   ↕                ↕
(RegioS,      Perioden)   ← nome original na API CBS
```

`region_code` é o código de região CBS (ex: `GM0363` = Amsterdam)
`period_code` é o período de referência (ex: `2023JJ00` = ano 2023)

### Hierarquia de schemas

```
SILVER.CBS_NEIGHBOURHOOD_KEY_FIGURES ──────────────────┐
SILVER.CBS_AVERAGE_WOZ_VALUE ──────────────────────────┤
SILVER.CBS_HOUSEHOLD_INCOME ───────────────────────────┤
         ↓ dbt views                                   │
STAGING.STG_CBS_NEIGHBOURHOOD_KEY_FIGURES              │
STAGING.STG_CBS_AVERAGE_WOZ_VALUE                      ├→ MARTS.MART_REGIONAL_DASHBOARD
STAGING.STG_CBS_HOUSEHOLD_INCOME                       │
                                                       │
SILVER.CBS_HOUSING_STOCK ──────────────────────────────┤
SILVER.CBS_AVERAGE_WOZ_VALUE ──────────────────────────┘
         ↓ dbt views
STAGING.STG_CBS_HOUSING_STOCK ─────────────────────────→ MARTS.MART_HOUSING_MARKET
STAGING.STG_CBS_AVERAGE_WOZ_VALUE ─────────────────────→ MARTS.MART_HOUSING_MARKET

SILVER.KNMI_DAILY_WEATHER
         ↓ dbt view
STAGING.STG_KNMI_DAILY_WEATHER ────────────────────────→ MARTS.MART_WEATHER_SUMMARY

SILVER.PDOK_BAG_PAND ──────────────────────────────────→ STAGING.STG_PDOK_BAG_PAND
SILVER.PDOK_BAG_VERBLIJFSOBJECT ───────────────────────→ STAGING.STG_PDOK_BAG_VERBLIJFSOBJECT

SILVER.NDW_TRAFFIC_FLOW ───────────────────────────────→ STAGING.STG_NDW_TRAFFIC_FLOW

STAGING.STG_CBS_SALE_PRICES_ANNUAL ────────────────────┐
STAGING.STG_CBS_SALE_PRICE_INDEX ──────────────────────┤
STAGING.STG_CBS_AVERAGE_WOZ_VALUE ─────────────────────├→ MARTS.MART_NEIGHBOURHOOD_REFERENCE
STAGING.STG_CBS_NEIGHBOURHOOD_KEY_FIGURES ─────────────┤
STAGING.STG_CBS_MUNICIPALITY_REGIONS ──────────────────┘
```

---

## 17. Glossário

| Termo | Significado |
|---|---|
| **Bronze** | Dados crus, exatamente como vieram da fonte (JSON, CSV, XML) |
| **Silver** | Dados limpos, normalizados, em formato Parquet |
| **Gold / Mart** | Dados analíticos finais, prontos para consumo por BI |
| **DAG** | Directed Acyclic Graph — o "fluxo" de tarefas no Airflow |
| **dbt model** | Arquivo `.sql` que define uma view ou tabela no DW |
| **Staging** | Primeira camada dbt — apenas renomeia e casteia, sem lógica de negócio |
| **Mart** | Tabela final dbt — junta staging models para responder perguntas analíticas |
| **SeaweedFS** | Storage S3-compatível rodando localmente no Docker (substituiu o MinIO) |
| **MotherDuck** | DuckDB hospedado na nuvem (free tier permanente) |
| **httpfs** | Extensão do DuckDB que permite ler arquivos direto de URLs HTTP/S3 |
| **s3a://** | Protocolo Hadoop para acessar S3 a partir do PySpark |
| **WFS** | Web Feature Service — protocolo para dados geoespaciais (PDOK) |
| **BAG** | Basisregistraties Adressen en Gebouwen — cadastro de endereços/edificações da Holanda |
| **CBS** | Centraal Bureau voor de Statistiek — instituto de estatísticas holandês |
| **KNMI** | Koninklijk Nederlands Meteorologisch Instituut — instituto meteorológico holandês |
| **NDW** | Nationaal Datawarehouse Wegverkeer — banco de dados de tráfego holandês |
| **PDOK** | Publieke Dienstverlening Op de Kaart — portal geoespacial holandês |
| **WOZ** | Waardering Onroerende Zaken — avaliação de imóveis para fins fiscais |
| **Medallion** | Arquitetura de dados em camadas: Bronze → Silver → Gold |
| **Dedup** | Deduplicação — remoção de registros duplicados |
| **Parquet** | Formato de arquivo colunar, eficiente para analytics |
