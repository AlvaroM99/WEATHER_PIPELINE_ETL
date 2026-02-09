# Scripts de Deployment

Colección de scripts Bash para automatizar deployment, health checks y testing del Weather ETL Pipeline.

---

## 📋 Contenido

| Script | Descripción | Uso |
|--------|-------------|-----|
| [`health_check.sh`](health_check.sh) | Health checks completos | `./health_check.sh` |
| [`entrypoint.sh`](entrypoint.sh) | Entrypoint del contenedor Docker | `./entrypoint.sh` |
| [`deploy.sh`](deploy.sh) | Script maestro de deployment | `./deploy.sh <env> <version>` |
| [`smoke_tests.sh`](smoke_tests.sh) | Tests post-deployment | `./smoke_tests.sh` |

---

## 🏥 health_check.sh

### Descripción

Ejecuta 7 health checks para verificar que todos los servicios estén funcionando correctamente.

### Health Checks

1. **Airflow Webserver**: Verifica que responda en `/health`
2. **Airflow Scheduler**: Verifica que el proceso esté corriendo
3. **PostgreSQL**: Intenta conectar a la base de datos
4. **MinIO**: Verifica endpoint `/minio/health/live`
5. **Python Imports**: Verifica que los módulos se importen correctamente
6. **Disk Space**: Verifica que haya >10% de espacio libre
7. **Memory**: Monitorea uso de memoria (warning >90%)

### Uso

```bash
# Ejecución básica
./scripts/health_check.sh

# Con variables de entorno custom
AIRFLOW_WEBSERVER_URL=http://localhost:8080 \
POSTGRES_HOST=postgres \
ENVIRONMENT=production \
./scripts/health_check.sh

# En Docker
docker-compose exec airflow /opt/airflow/scripts/health_check.sh
```

### Exit Codes

- `0`: Todos los checks pasaron ✅
- `1`: Al menos un check falló ❌

### Output

```bash
========================================
Weather ETL Pipeline - Health Check
Environment: production
========================================
[INFO] Checking Airflow webserver...
[INFO] Airflow webserver: OK
[INFO] Checking Airflow scheduler...
[INFO] Airflow scheduler: OK
[INFO] Checking PostgreSQL connection...
[INFO] PostgreSQL connection: OK
[INFO] Checking MinIO connection...
[INFO] MinIO health: OK
[INFO] Checking Python module imports...
[INFO] Python imports: OK
[INFO] Checking disk space...
[INFO] Disk space: OK (75% used)
[INFO] Checking memory usage...
[INFO] Memory usage: OK (65%)
========================================
✅ All health checks PASSED
========================================
```

### Variables de Entorno

| Variable | Default | Descripción |
|----------|---------|-------------|
| `AIRFLOW_WEBSERVER_URL` | `http://localhost:8080` | URL del webserver |
| `POSTGRES_HOST` | `postgres` | Hostname de PostgreSQL |
| `POSTGRES_PORT` | `5432` | Puerto de PostgreSQL |
| `MINIO_ENDPOINT` | `minio:9000` | Endpoint de MinIO |
| `ENVIRONMENT` | `development` | Entorno actual |
| `MAX_RETRIES` | `3` | Número de reintentos |
| `RETRY_DELAY` | `5` | Delay entre reintentos (segundos) |

---

## 🚀 entrypoint.sh

### Descripción

Entrypoint del contenedor Docker de Airflow. Maneja inicialización, creación de usuarios y startup de servicios.

### Funcionalidades

1. **Wait for Dependencies**: Espera a que PostgreSQL esté listo
2. **Database Initialization**: Inicializa/migra la base de datos de Airflow
3. **Admin User Creation**: Crea usuario admin si no existe
4. **Service Startup**: Inicia webserver, scheduler o worker según configuración
5. **Graceful Shutdown**: Maneja señales SIGTERM/SIGINT

### Uso

```bash
# Webserver (default)
AIRFLOW_ROLE=webserver ./scripts/entrypoint.sh

# Scheduler
AIRFLOW_ROLE=scheduler ./scripts/entrypoint.sh

# Worker (Celery)
AIRFLOW_ROLE=worker ./scripts/entrypoint.sh
```

### Variables de Entorno

| Variable | Default | Descripción |
|----------|---------|-------------|
| `AIRFLOW_ROLE` | `webserver` | Rol del servicio |
| `ENVIRONMENT` | `development` | Entorno actual |
| `AIRFLOW_USER` | `admin` | Username del admin |
| `AIRFLOW_PASSWORD` | `admin` | Password del admin |
| `AIRFLOW_EMAIL` | `admin@example.com` | Email del admin |

### Docker Compose

```yaml
services:
  airflow:
    command: /opt/airflow/scripts/entrypoint.sh
    environment:
      AIRFLOW_ROLE: webserver
```

---

## 📦 deploy.sh

### Descripción

Script maestro de deployment con backup, health checks y rollback automático.

### Uso

```bash
# Deployment normal
./scripts/deploy.sh <environment> <version>

# Ejemplos
./scripts/deploy.sh development latest
./scripts/deploy.sh production v1.2.3

# Rollback manual
./scripts/deploy.sh --rollback

# Help
./scripts/deploy.sh --help
```

### Flujo de Deployment

```
1. Pre-deployment Checks
   ├─ Verify docker installed
   ├─ Verify docker-compose installed
   ├─ Check .env file exists
   └─ Check disk space (>5GB)

2. Backup Current State
   ├─ Backup .env file
   ├─ Export database (production only)
   └─ Save services state

3. Pull New Images
   ├─ Pull from registry
   └─ Tag for environment

4. Deploy Services
   ├─ Pull dependencies
   ├─ Zero-downtime deploy (production)
   └─ Simple restart (development)

5. Post-deployment Tests
   ├─ Wait 30s stabilization
   ├─ Health checks (20 retries)
   ├─ Smoke tests
   └─ Rollback if fail

6. Cleanup
   ├─ Remove dangling images
   └─ Clean old backups (keep 5)
```

### Environments

```bash
# Development
- Restart simple
- Sin health checks críticos
- Backups opcionales

# Production
- Zero-downtime deployment
- Health checks obligatorios
- Auto-rollback si falla
- Backup completo
```

### Exit Codes

- `0`: Deployment exitoso
- `1`: Error en deployment (con rollback si prod)

### Variables de Entorno

| Variable | Default | Descripción |
|----------|---------|-------------|
| `DOCKER_REGISTRY` | `ghcr.io` | Registry de imágenes |
| `GITHUB_REPOSITORY` | - | Nombre del repo |

---

## 🧪 smoke_tests.sh

### Descripción

Suite de tests rápidos para verificar funcionalidad básica post-deployment.

### Tests

1. **Airflow API**: Verifica que `/api/v1/health` responda 200
2. **DAGs Loaded**: Verifica que al menos 1 DAG esté cargado
3. **Database Tables**: Verifica que tablas críticas existan
4. **MinIO Buckets**: Verifica que buckets bronze/silver/gold existan
5. **Python Imports**: Verifica imports críticos

### Uso

```bash
# Ejecución básica
./scripts/smoke_tests.sh

# Con custom URL
AIRFLOW_WEBSERVER_URL=http://localhost:8080 \
AIRFLOW_USER=admin \
AIRFLOW_PASSWORD=admin \
./scripts/smoke_tests.sh

# En Docker
docker-compose exec airflow /opt/airflow/scripts/smoke_tests.sh
```

### Exit Codes

- `0`: Todos los tests pasaron
- `1`: Al menos un test falló

### Output

```bash
=========================================
Weather ETL Pipeline - Smoke Tests
Environment: production
=========================================
[INFO] Testing Airflow API...
✅ Airflow API is responding
[INFO] Testing DAGs are loaded...
✅ Found 5 DAGs loaded
[INFO] Testing database tables exist...
✅ Database tables exist
[INFO] Testing MinIO buckets...
✅ MinIO buckets exist
[INFO] Testing critical Python imports...
✅ Python imports successful
=========================================
✅ All smoke tests PASSED
=========================================
```

### Variables de Entorno

| Variable | Default | Descripción |
|----------|---------|-------------|
| `AIRFLOW_WEBSERVER_URL` | `http://localhost:8080` | URL del webserver |
| `AIRFLOW_USER` | `admin` | Username de Airflow |
| `AIRFLOW_PASSWORD` | `admin` | Password de Airflow |
| `ENVIRONMENT` | `development` | Entorno actual |

---

## 🔧 Desarrollo de Scripts

### Añadir Nuevo Script

```bash
# 1. Crear archivo
touch scripts/my_script.sh

# 2. Hacer ejecutable
chmod +x scripts/my_script.sh

# 3. Añadir shebang y header
cat > scripts/my_script.sh << 'EOF'
#!/bin/bash
# ============================================================================
# My Script
# ============================================================================
# Description: What this script does
# Usage: ./scripts/my_script.sh <args>
# ============================================================================

set -e

# Your code here
EOF

# 4. Test
bash scripts/my_script.sh

# 5. Integrar en CI/CD si necesario
```

### Best Practices

1. **Shebang**: Siempre usar `#!/bin/bash`
2. **Set flags**: Usar `set -e` (exit on error)
3. **Colors**: Definir constantes para output coloreado
4. **Logging**: Usar funciones `log_info`, `log_error`, `log_warn`
5. **Exit codes**: 0 = éxito, 1 = error
6. **Help**: Implementar `--help` flag
7. **Idempotencia**: Scripts deben ser idempotentes
8. **Testing**: Test con ShellCheck antes de commit

### Testing con ShellCheck

```bash
# Instalar ShellCheck
# Ubuntu/Debian
sudo apt install shellcheck

# macOS
brew install shellcheck

# Test scripts
shellcheck scripts/*.sh

# Fix issues reportados
```

### Template Base

```bash
#!/bin/bash
# ============================================================================
# Script Name
# ============================================================================
set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

main() {
    log_info "Starting script..."

    # Your logic here

    log_info "Script completed successfully!"
}

# Run main
main "$@"
```

---

## 📚 Referencias

### Documentación

- [Bash Guide](https://mywiki.wooledge.org/BashGuide)
- [ShellCheck](https://www.shellcheck.net/)
- [Google Shell Style Guide](https://google.github.io/styleguide/shellguide.html)

### Proyecto

- [CD_GUIDE.md](../CD_GUIDE.md) - Guía completa de CD
- [SETUP_CD.md](../SETUP_CD.md) - Setup rápido
- [CD_SUMMARY.md](../CD_SUMMARY.md) - Resumen ejecutivo

---

## 🐛 Troubleshooting

### Script No Ejecutable

```bash
chmod +x scripts/*.sh
```

### Syntax Error

```bash
# Verificar con ShellCheck
shellcheck scripts/health_check.sh

# Verificar encoding
file scripts/health_check.sh
# Debe ser: ASCII text executable

# Si tiene BOM o CRLF
dos2unix scripts/health_check.sh
```

### Command Not Found

```bash
# Verificar shebang
head -1 scripts/health_check.sh
# Debe ser: #!/bin/bash

# Ejecutar directamente con bash
bash scripts/health_check.sh
```

---

**Todos los scripts están documentados y testeados** ✅
