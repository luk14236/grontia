# Deploy da app web no Heroku

O Grontia roda em três lugares:

| Parte | Onde | O que faz |
|---|---|---|
| Pipeline de dados (Airflow, PySpark, dbt, SeaweedFS) | Sua máquina (`docker compose`) | Coleta os dados todo dia e publica os marts no MotherDuck (DAG 40 com `GRONTIA_DBT_TARGET=motherduck`) |
| Banco | MotherDuck, banco `grontia` | Staging, marts e histórico dos testes |
| App web (`web/` + `evaluation/`) | Heroku | Lê do MotherDuck e das APIs públicas; não grava nada |

O Heroku só recebe a app web. O `.slugignore` deixa o pipeline, o dbt e a documentação fora do slug, e o `requirements.txt` da raiz instala só o que a app usa.

## Arquivos de deploy (raiz do repositório)

| Arquivo | Conteúdo |
|---|---|
| `Procfile` | `web: uvicorn web.app:app --host 0.0.0.0 --port $PORT` |
| `requirements.txt` | Dependências da app web (FastAPI, uvicorn, pypdf, duckdb, requests, PyYAML) |
| `.python-version` | `3.11` |
| `.slugignore` | Pastas que não vão para o Heroku |

## Passo a passo

1. **Um token só para o Heroku.** Em app.motherduck.com → Settings → Create token, crie um token novo com um nome como `grontia-heroku`, separado do que o dbt usa. Assim dá para revogá-lo sem parar o pipeline. Tokens somente leitura (Read Scaling) só existem no plano Business; no plano gratuito o token pode gravar. Para compensar, a app abre o banco com `access_mode = READ_ONLY` (`evaluation/warehouse.py`), e o DuckDB recusa qualquer escrita (`Cannot execute statement of type "CREATE" ... attached in read-only mode`). Isso protege contra erro da própria app, não contra quem roubar o token: guarde-o só nas config vars do Heroku.

2. **Heroku CLI e login.** No WSL, sem `sudo`, o pacote standalone vai para a pasta do usuário:
   ```bash
   mkdir -p ~/.local/lib && curl -fsSL https://cli-assets.heroku.com/channels/stable/heroku-linux-x64.tar.gz | tar -xz -C ~/.local/lib
   ln -sf ~/.local/lib/heroku/bin/heroku ~/bin/heroku
   heroku login
   ```
   Opcional: o MCP oficial do Heroku (`claude mcp add heroku -- heroku mcp:start`) usa esse mesmo login.

3. **Criar a app na Europa** (os dados e os usuários estão na Holanda; o nome precisa ser único no Heroku):
   ```bash
   heroku create grontia --region eu
   ```

4. **Configurar as variáveis.** Comece a linha com um espaço para o token não ficar no histórico do shell:
   ```bash
    heroku config:set MOTHERDUCK_TOKEN=<token grontia-heroku>
   heroku config:set GRONTIA_DBT_TARGET=motherduck GRONTIA_HTTP_TIMEOUT=8
   ```
   | Variável | Obrigatória | Para quê |
   |---|---|---|
   | `MOTHERDUCK_TOKEN` | sim | Leitura do banco `grontia` (token próprio do Heroku) |
   | `GRONTIA_DBT_TARGET` | sim, `motherduck` | Faz a avaliação ler do MotherDuck |
   | `GRONTIA_HTTP_TIMEOUT` | recomendada, `8` | O roteador do Heroku corta requisições depois de 30 s; com 8 s por chamada, uma fonte lenta falha sem derrubar a avaliação |
   | `GRONTIA_RATE_LIMIT` | não (padrão `10`) | Avaliações e PDFs por visitante a cada 10 minutos |
   | `GRONTIA_WEB_PASSWORD` | não | Se definida, o site inteiro pede essa senha (qualquer usuário) |

5. **Deploy.** O Heroku detecta Python pelo `requirements.txt`:
   ```bash
   git push heroku main
   heroku ps:scale web=1
   heroku open
   ```
   Para deploy automático a cada push no GitHub, conecte o repositório em Dashboard → Deploy → GitHub.

6. **Verificar:**
   ```bash
   curl https://<sua-app>.herokuapp.com/api/health
   heroku logs --tail
   ```
   Depois faça uma avaliação pelo site. A primeira de cada dyno demora alguns segundos a mais porque o DuckDB baixa a extensão do MotherDuck e abre a conexão.

## Deploy automático (GitHub Actions)

O workflow `.github/workflows/deploy-web.yml` publica no Heroku a cada push em `main` que mexa na app web (`web/`, `evaluation/`, `requirements.txt`, `Procfile`, `.python-version`, `.slugignore`). Commits só do pipeline não disparam deploy, para não reiniciar o dyno à toa. Também dá para rodar à mão em Actions → *Deploy web app (Heroku)* → *Run workflow*.

1. **check:** instala o `requirements.txt`, importa a app, testa o leitor de PDF com um anúncio de exemplo e confere a sintaxe do JS.
2. **deploy:** `git push` para `git.heroku.com/grontia.git` e espera o `/api/health` responder.

Configuração única: em GitHub → Settings → Secrets and variables → Actions, crie o secret **`HEROKU_API_KEY`**. O valor é uma chave da API do Heroku só para esse fim:
```bash
heroku authorizations:create -d "GitHub Actions deploy (grontia)" --expires-in 31536000
```
A chave vale 1 ano; depois disso, gere outra e troque o secret. O `git push heroku main` manual continua funcionando.

## Como os dados chegam ao site

O site lê sempre o que está no MotherDuck. O DAG 40 atualiza o banco depois de cada ingestão diária na sua máquina. Se a máquina ficar desligada, o site continua no ar com os dados da última carga, e os testes de freshness avisam quando o pipeline voltar.

## Testado localmente

A configuração acima foi testada num container `python:3.11-slim` limpo, com só o `requirements.txt` e o comando do `Procfile`, lendo do MotherDuck:
- dependências: 106 MB (o limite de slug do Heroku é 500 MB);
- avaliação: 6 a 12 s, com as fontes de contexto em paralelo (a primeira inclui a conexão com o MotherDuck);
- limite de requisições: a 11ª chamada em 10 minutos recebe `429` com `Retry-After`.

## Custos e limites

- O Heroku não tem plano gratuito; confira o preço atual dos dynos antes de criar a app.
- Cada avaliação faz algumas consultas pequenas no MotherDuck. O plano gratuito do MotherDuck tem cota diária de compute, dividida com o build diário do DAG 40.
- A app não grava nada (endereço, PDF ou resultado). Os logs do roteador do Heroku registram IP e caminho das requisições, não o conteúdo.
