# Setup And Troubleshooting

## Recommended On Windows

- Use `Git Bash` or `WSL` to run `.sh` scripts.
- If you stay in PowerShell, prefer direct `docker compose ...` commands.

## Start

### Git Bash / WSL

```bash
./start-lakehouse.sh
```

### PowerShell

```powershell
docker compose up -d --build
```

## Open Services

- Superset: `http://localhost:8088`
- Airflow: `http://localhost:8089`
- Spark Master: `http://localhost:8080`
- HDFS NameNode: `http://localhost:9870`

## Common Windows Problems

### 1. Bash script fails

Symptoms:

- `/bin/bash^M: bad interpreter`
- script runs strangely in Git Bash

Cause:

- cloned files were converted to CRLF

Fix:

1. Re-clone the repo after the `.gitattributes` fix
2. Or convert `.sh` files back to LF
3. Run scripts with `Git Bash` or `WSL`

### 2. Docker credential error

Symptoms:

- `error getting credentials`
- image pull fails even though Docker Desktop is open

Try these steps:

1. Quit Docker Desktop completely
2. Open Docker Desktop again
3. Run:

```powershell
docker logout
docker login
```

4. If it still fails, inspect:

`C:\Users\<your-user>\.docker\config.json`

If Docker Desktop credential helper is broken, temporarily remove:

```json
"credsStore": "desktop"
```

then try `docker login` and `docker compose up -d --build` again.

### 3. Clean rebuild

```powershell
docker compose down
docker compose up -d --build
```

## HDFS Layout

Created automatically by `start-lakehouse.sh`:

- `/lakehouse/bronze`
- `/lakehouse/silver`
- `/lakehouse/gold`
- `/user/hive/warehouse`

