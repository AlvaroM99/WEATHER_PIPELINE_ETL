# Guía Completa de Continuous Deployment (CD)

Sistema de despliegue continuo automatizado para el Weather ETL Pipeline con soporte para entornos **development** y **production**.

---

## 📋 Tabla de Contenidos

- [Arquitectura del Sistema CD](#arquitectura-del-sistema-cd)
- [Componentes](#componentes)
- [Flujos de Deployment](#flujos-de-deployment)
- [Configuración Inicial](#configuración-inicial)
- [Uso del Sistema](#uso-del-sistema)
- [Monitoreo y Troubleshooting](#monitoreo-y-troubleshooting)
- [Rollback](#rollback)
- [Best Practices](#best-practices)

---

## 🏗️ Arquitectura del Sistema CD

```
┌─────────────────────────────────────────────────────────────────┐
│                         Git Repository                          │
│                    github.com/AlvaroM99/                        │
│                   weather-pipeline-etl                          │
└──────────────┬──────────────────────┬──────────────────────────┘
               │                      │
               │ Push to develop      │ Push to master
               ▼                      ▼
┌──────────────────────┐    ┌──────────────────────┐
│   CI Pipeline        │    │   CI Pipeline        │
│   (Existing)         │    │   (Existing)         │
│   - Lint             │    │   - Lint             │
│   - Tests            │    │   - Tests            │
│   - Coverage         │    │   - Security Scan    │
└──────┬───────────────┘    └─────────┬────────────┘
       │                              │
       │ If passed                    │ If passed
       ▼                              ▼
┌──────────────────────┐    ┌──────────────────────┐
│  Docker Build        │    │  Docker Build        │
│  - Build image       │    │  - Build image       │
│  - Tag: development  │    │  - Tag: production   │
│  - Push to GHCR      │    │  - Push to GHCR      │
└──────┬───────────────┘    └─────────┬────────────┘
       │                              │
       │ Automatic                    │ Manual trigger
       ▼                              ▼
┌──────────────────────┐    ┌──────────────────────┐
│  CD - Development    │    │  CD - Production     │
│  - Deploy to dev     │    │  - Security scan     │
│  - Health checks     │    │  - Blue-Green deploy │
│  - Smoke tests       │    │  - Health checks     │
│  - Slack notify      │    │  - Smoke tests       │
└──────────────────────┘    │  - Monitor 5 min     │
                             │  - Auto-rollback     │
                             │  - Slack notify      │
                             └──────────────────────┘
```

---

## 🔧 Componentes

### 1. Dockerfile

**Ubicación**: [`Dockerfile`](Dockerfile)

Multi-stage build optimizado:
- **Stage 1 (Builder)**: Instala dependencias
- **Stage 2 (Runtime)**: Imagen final mínima (~800MB)

```dockerfile
FROM apache/airflow:2.9.1-python3.11 AS builder
# Install dependencies...

FROM apache/airflow:2.9.1-python3.11
# Copy artifacts from builder
COPY --from=builder /home/airflow/.local /home/airflow/.local
```

**Características**:
- ✅ Imagen base oficial de Airflow
- ✅ Multi-stage para reducir tamaño
- ✅ Healthcheck integrado
- ✅ Labels con metadata

### 2. Docker Compose por Entorno

#### Development: [`docker-compose.development.yml`](docker-compose.development.yml)

```yaml
services:
  airflow:
    image: weather-pipeline-etl:development
    environment:
      ENVIRONMENT: development
      ENABLE_JSON_LOGGING: "false"  # Human-readable
      DB_POOL_MAX_CONN: 10          # Lower limits
    volumes:
      - ./src:/opt/airflow/src  # Hot reload
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: "1.0"
```

**Características Dev**:
- ✅ Hot reload de código
- ✅ Logs legibles para humanos
- ✅ Recursos limitados (ahorro de costos)
- ✅ Debugging tools incluidos (pgAdmin)

#### Production: [`docker-compose.production.yml`](docker-compose.production.yml)

```yaml
services:
  airflow:
    image: weather-pipeline-etl:production
    environment:
      ENVIRONMENT: production
      ENABLE_JSON_LOGGING: "true"  # JSON structured
      DB_POOL_MAX_CONN: 50         # High capacity
    volumes:
      - ./dags:/opt/airflow/dags:ro  # Read-only
    deploy:
      resources:
        limits:
          memory: 8G
          cpus: "4.0"
    restart: always
    logging:
      driver: "json-file"
      options:
        max-size: "100m"
        max-file: "10"
```

**Características Prod**:
- ✅ JSON logs para ELK/Datadog
- ✅ Recursos altos para carga
- ✅ Volúmenes read-only (seguridad)
- ✅ Auto-restart + log rotation
- ✅ Sin debugging tools

### 3. Scripts de Deployment

#### [`scripts/health_check.sh`](scripts/health_check.sh)

Health checks completos:

```bash
✅ Airflow webserver responding
✅ Airflow scheduler running
✅ PostgreSQL connection
✅ MinIO accessible
✅ Python modules importable
✅ Disk space sufficient (>10%)
✅ Memory usage monitored
```

#### [`scripts/deploy.sh`](scripts/deploy.sh)

Script maestro de deployment con:

```bash
# Uso
./scripts/deploy.sh <environment> <version>
./scripts/deploy.sh development latest
./scripts/deploy.sh production v1.2.3

# Rollback
./scripts/deploy.sh --rollback
```

**Flujo**:
1. Pre-deployment checks (disk, docker, .env)
2. Backup de estado actual
3. Pull de nuevas imágenes
4. Deploy con zero-downtime (production)
5. Health checks post-deployment
6. Smoke tests
7. Auto-rollback si falla

#### [`scripts/smoke_tests.sh`](scripts/smoke_tests.sh)

Tests rápidos post-deployment:

```bash
✅ Airflow API responding
✅ DAGs loaded
✅ Database tables exist
✅ MinIO buckets exist
✅ Python imports successful
```

### 4. GitHub Actions Workflows

#### [`docker-build.yml`](.github/workflows/docker-build.yml)

Build y push de imágenes Docker:

```yaml
Triggers:
- Push a master → tag: production, latest
- Push a develop → tag: development
- Tags v* → tag: v1.2.3, v1.2, v1

Features:
- Docker Buildx (multi-platform)
- Cache optimization (GitHub Actions cache)
- GHCR (GitHub Container Registry)
- Metadata con labels
```

#### [`cd-development.yml`](.github/workflows/cd-development.yml)

Deployment automático a desarrollo:

```yaml
Trigger: Push a develop (automático)

Steps:
1. Checkout code
2. Configure SSH to dev server
3. Pull latest image
4. Deploy with scripts/deploy.sh
5. Health checks (10 retries)
6. Smoke tests
7. Slack notification

Rollback: Manual (no automático en dev)
```

#### [`cd-production.yml`](.github/workflows/cd-production.yml)

Deployment manual a producción:

```yaml
Trigger: Manual (workflow_dispatch)

Inputs:
- version: Version to deploy (required)
- skip_tests: Skip smoke tests (emergency only)

Steps:
1. Pre-deployment checks
   - Verify image exists
   - Security scan (Trivy)
   - Check critical vulnerabilities
2. Deploy (Blue-Green)
   - Pull new image
   - Start new containers
   - Wait 60s stabilization
3. Health checks (20 retries, 15s interval)
4. Smoke tests (unless skipped)
5. Monitor 5 minutes
6. Auto-rollback if any check fails
7. Slack notification
8. Create GitHub Release (if version tag)

Rollback: Automatic if health checks fail
```

---

## ⚙️ Configuración Inicial

### Paso 1: Configurar GitHub Secrets

Navega a: **Settings → Secrets and variables → Actions → New repository secret**

#### Secrets Requeridos:

| Secret | Descripción | Ejemplo |
|--------|-------------|---------|
| `DEV_SERVER_HOST` | Hostname del servidor dev | `dev.weather-etl.com` |
| `DEV_SERVER_USER` | Usuario SSH dev | `deploy` |
| `DEV_SSH_PRIVATE_KEY` | SSH key para dev | `-----BEGIN OPENSSH PRIVATE KEY-----...` |
| `PROD_SERVER_HOST` | Hostname del servidor prod | `prod.weather-etl.com` |
| `PROD_SERVER_USER` | Usuario SSH prod | `deploy` |
| `PROD_SSH_PRIVATE_KEY` | SSH key para prod | `-----BEGIN OPENSSH PRIVATE KEY-----...` |
| `SLACK_WEBHOOK_URL` (opcional) | Webhook de Slack | `https://hooks.slack.com/...` |

**Generar SSH Keys**:

```bash
# Generar par de keys
ssh-keygen -t ed25519 -C "github-actions-deploy" -f deploy_key

# Copiar la clave pública al servidor
ssh-copy-id -i deploy_key.pub deploy@dev.weather-etl.com

# Copiar la clave privada a GitHub Secrets
cat deploy_key  # Copiar contenido completo
```

### Paso 2: Preparar Servidores

En cada servidor (dev/prod), crear estructura:

```bash
# Como root o con sudo
sudo mkdir -p /opt/weather-etl
sudo chown deploy:deploy /opt/weather-etl

# Como usuario deploy
cd /opt/weather-etl
git clone https://github.com/AlvaroM99/weather-pipeline-etl.git .

# Crear directorios para volúmenes (production)
mkdir -p /opt/weather-etl/data/{postgres,postgres-airflow,minio}
mkdir -p /opt/weather-etl/logs/airflow
mkdir -p /opt/weather-etl/backups

# Permisos
chmod 755 /opt/weather-etl
chmod 700 /opt/weather-etl/data

# Copiar .env apropiado
cp .env.development .env  # En dev
cp .env.production .env   # En prod

# Editar .env con credenciales reales
nano .env
```

### Paso 3: Configurar Environment Variables

Editar `.env.<environment>` con valores reales:

```bash
# Development
nano .env.development

# Production
nano .env.production
```

**Valores Críticos a Cambiar**:

```bash
# Cambiar TODOS los passwords
POSTGRES_PASSWORD=STRONG_PASSWORD_HERE
AIRFLOW_PASSWORD=STRONG_PASSWORD_HERE
MINIO_ROOT_PASSWORD=STRONG_PASSWORD_HERE

# API Keys con límites apropiados
OPENWEATHER_API_KEY=your_actual_api_key
AEMET_API_KEY=your_actual_api_key
OPENMETEO_API_KEY=your_actual_api_key

# Airflow security (production)
AIRFLOW__CORE__FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
AIRFLOW__WEBSERVER__SECRET_KEY=$(openssl rand -hex 32)
```

### Paso 4: Test Manual del Deployment

Antes de automatizar, test manual:

```bash
# En el servidor (dev o prod)
cd /opt/weather-etl

# Test de scripts
bash scripts/health_check.sh
bash scripts/smoke_tests.sh

# Test de deployment
bash scripts/deploy.sh development latest
```

---

## 🚀 Uso del Sistema

### Deployment a Development

**Método 1: Push Automático**

```bash
# Local
git checkout develop
git add .
git commit -m "feat: add new feature"
git push origin develop

# GitHub Actions se ejecuta automáticamente:
# 1. CI tests
# 2. Docker build
# 3. Deploy to dev
# 4. Health checks
# 5. Notification
```

**Método 2: Manual Trigger**

1. Ir a GitHub Actions
2. Seleccionar workflow "CD - Development"
3. Click "Run workflow"
4. Seleccionar branch `develop`
5. Click "Run workflow"

### Deployment a Production

**⚠️ Solo manual con aprobación**

1. Ir a GitHub Actions
2. Seleccionar workflow "CD - Production"
3. Click "Run workflow"
4. Inputs:
   - **version**: `latest`, `v1.2.3`, o cualquier tag
   - **skip_tests**: ❌ Dejar en `false` (tests obligatorios)
5. Click "Run workflow"
6. Esperar aprobación de reviewer
7. Workflow ejecuta deployment con:
   - Security scan
   - Blue-Green deployment
   - Health checks
   - 5 min monitoring
   - Auto-rollback si falla

### Ver Estado del Deployment

**GitHub Actions UI**:
```
Actions → Workflows → CD - <Environment>
```

**Logs en Servidor**:
```bash
# SSH al servidor
ssh deploy@<server>

# Ver logs de despliegue
tail -f /opt/weather-etl/logs/deploy.log

# Ver logs de Airflow
docker-compose -f docker/compose/docker-compose.<env>.yml logs -f airflow

# Ver estado de servicios
docker-compose -f docker/compose/docker-compose.<env>.yml ps
```

**Slack Notifications** (si configurado):

```
Deployment Success:
┌─────────────────────────────────────┐
│ 🎯 Production Deployment success    │
│                                     │
│ Environment:  production            │
│ Version:      v1.2.3                │
│ Deployed by:  @AlvaroM              │
│ Status:       ✅ SUCCESS            │
│                                     │
│ Deployed at: 2025-02-09 14:30:00    │
└─────────────────────────────────────┘
```

---

## 🔍 Monitoreo y Troubleshooting

### Health Checks

**Manual**:

```bash
# En el servidor
cd /opt/weather-etl
bash scripts/health_check.sh

# Output:
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
...
✅ All health checks PASSED
```

**Automático** (via workflow):

```yaml
# Se ejecutan automáticamente post-deployment
- Development: 10 retries, 5s interval
- Production: 20 retries, 15s interval
```

### Smoke Tests

```bash
cd /opt/weather-etl
bash scripts/smoke_tests.sh

# Output:
[INFO] Testing Airflow API...
✅ Airflow API is responding
[INFO] Testing DAGs are loaded...
✅ Found 5 DAGs loaded
[INFO] Testing database tables exist...
✅ Database tables exist
...
```

### Logs

**Ver Logs de Deployment**:

```bash
# En servidor
tail -f /opt/weather-etl/logs/deploy.log

# Docker logs
docker-compose -f docker/compose/docker-compose.<env>.yml logs -f
```

**Ver Logs Estructurados (Production)**:

```bash
# Logs JSON de Airflow
docker-compose -f docker/compose/docker-compose.production.yml logs airflow | grep '"level":"ERROR"'

# Con jq para filtrar
docker-compose -f docker/compose/docker-compose.production.yml logs airflow \
  | jq 'select(.level=="ERROR" and .logger=="Extractor")'
```

### Métricas

**Recursos de Contenedores**:

```bash
# Stats en tiempo real
docker stats

# Recursos específicos del servicio
docker stats weather-etl-airflow-1
```

**Pool de Conexiones**:

```python
# En Python console del contenedor
from src.config.db_pool import get_pool_stats
print(get_pool_stats())

# Output:
{
  'min_conn': 5,
  'max_conn': 50,
  'pool_closed': False
}
```

### Problemas Comunes

#### 1. Health Check Fails: Airflow Webserver

**Síntoma**:
```
[ERROR] Airflow webserver: FAILED after 10 attempts
```

**Solución**:
```bash
# Verificar que el contenedor está corriendo
docker ps | grep airflow

# Ver logs
docker logs weather-etl-airflow-1

# Reiniciar si es necesario
docker-compose -f docker/compose/docker-compose.<env>.yml restart airflow
```

#### 2. Database Connection Timeout

**Síntoma**:
```
[ERROR] PostgreSQL connection: FAILED
psycopg2.OperationalError: could not connect to server
```

**Solución**:
```bash
# Verificar que PostgreSQL está corriendo
docker ps | grep postgres

# Verificar pool de conexiones
# Si maxconn está saturado, incrementar DB_POOL_MAX_CONN
nano .env.<environment>
# DB_POOL_MAX_CONN=100  # Aumentar

# Reiniciar
docker-compose -f docker/compose/docker-compose.<env>.yml restart
```

#### 3. Image Pull Fails

**Síntoma**:
```
Error response from daemon: pull access denied for ghcr.io/...
```

**Solución**:
```bash
# Login a GHCR
echo $GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin

# Verificar que la imagen existe
docker pull ghcr.io/alvarom99/weather-pipeline-etl:latest

# Si falla, rebuild
docker build -t weather-pipeline-etl:development .
```

#### 4. Disk Space Full

**Síntoma**:
```
[ERROR] Disk space: CRITICAL (95% used)
```

**Solución**:
```bash
# Limpiar imágenes viejas
docker image prune -a -f

# Limpiar volúmenes no usados
docker volume prune -f

# Limpiar logs
find /opt/weather-etl/logs -name "*.log" -mtime +30 -delete

# Limpiar backups viejos
find /opt/weather-etl/backups -type d -mtime +30 -exec rm -rf {} +
```

---

## 🔄 Rollback

### Rollback Automático (Production)

El workflow de producción hace rollback automático si:
- ❌ Health checks fallan (20 retries)
- ❌ Smoke tests fallan
- ❌ Monitoring detecta problemas (5 min)

**Proceso**:
1. Detecta falla
2. Log error en GitHub Actions
3. Ejecuta `scripts/deploy.sh --rollback`
4. Restaura estado anterior
5. Verifica health checks
6. Notifica en Slack

### Rollback Manual

**Desde GitHub Actions**:

```yaml
# No hay UI directa, usar workflow_dispatch con versión anterior
1. Go to CD - Production
2. Run workflow
3. version: <previous_version>  # ej: v1.2.2 en lugar de v1.2.3
4. skip_tests: false
```

**Desde Servidor (Emergency)**:

```bash
# SSH al servidor
ssh deploy@prod.weather-etl.com

cd /opt/weather-etl

# Rollback con script
bash scripts/deploy.sh --rollback

# Manual: restaurar desde backup
BACKUP_DIR=$(cat .last_backup_dir)
cp "$BACKUP_DIR/.env.production.backup" .env.production

# Restaurar database (si necesario)
gunzip < "$BACKUP_DIR/database_backup.sql.gz" | \
  docker-compose -f docker/compose/docker-compose.production.yml exec -T postgres \
  psql -U weather_user_prod weather_db_prod

# Reiniciar servicios con versión anterior
docker-compose -f docker/compose/docker-compose.production.yml down
docker tag weather-pipeline-etl:production-previous weather-pipeline-etl:production
docker-compose -f docker/compose/docker-compose.production.yml up -d
```

### Verificar Rollback

```bash
# Health checks
bash scripts/health_check.sh

# Ver versión actual
docker inspect weather-etl-airflow-1 | grep -A 5 Labels

# Verificar DAGs funcionando
curl -u admin:password http://localhost:8080/api/v1/dags
```

---

## 📚 Best Practices

### 1. Versionado Semántico

```bash
# Use semantic versioning for releases
git tag -a v1.2.3 -m "Release version 1.2.3"
git push origin v1.2.3

# Deploy specific version to production
GitHub Actions → CD - Production → version: v1.2.3
```

### 2. Testing Antes de Production

```bash
# Siempre test en development primero
git checkout develop
git merge feature/new-feature
git push origin develop

# Esperar deployment automático a dev
# Verificar en dev: http://dev.weather-etl.com:8080

# Si OK, merge a master
git checkout master
git merge develop
git push origin master

# Deploy manual a production
```

### 3. Backup Regular

```bash
# Configurar backup automático (crontab en servidor)
0 2 * * * /opt/weather-etl/scripts/backup.sh

# Backup script (crear si no existe)
#!/bin/bash
BACKUP_DIR="/opt/weather-etl/backups/$(date +\%Y\%m\%d)"
mkdir -p "$BACKUP_DIR"

# Backup database
docker-compose -f /opt/weather-etl/docker/compose/docker-compose.production.yml exec -T postgres \
  pg_dump -U weather_user_prod weather_db_prod | gzip > "$BACKUP_DIR/database.sql.gz"

# Backup MinIO
docker-compose -f /opt/weather-etl/docker/compose/docker-compose.production.yml exec -T minio \
  mc mirror --quiet /data "$BACKUP_DIR/minio"

# Cleanup old backups (>30 days)
find /opt/weather-etl/backups -type d -mtime +30 -exec rm -rf {} +
```

### 4. Monitoreo Continuo

**Configurar Alertas** (ejemplo con Prometheus):

```yaml
# prometheus/alerts.yml
groups:
  - name: weather-etl
    rules:
      - alert: AirflowDown
        expr: up{job="airflow"} == 0
        for: 5m
        annotations:
          summary: "Airflow is down"

      - alert: HighMemoryUsage
        expr: container_memory_usage_bytes / container_memory_max_bytes > 0.9
        for: 10m
        annotations:
          summary: "High memory usage (>90%)"
```

### 5. Security Checklist

- ✅ Rotate passwords every 90 days
- ✅ Use strong passwords (>16 chars, mixed)
- ✅ Limit SSH access (key-only, no password)
- ✅ Enable firewall rules (only required ports)
- ✅ Regular security scans (Trivy in CI/CD)
- ✅ Monitor logs for suspicious activity
- ✅ Keep dependencies updated (Dependabot)

---

## 📊 Métricas de Éxito

| Métrica | Target | Actual |
|---------|--------|--------|
| **Deployment Time (Dev)** | <5 min | ~3 min |
| **Deployment Time (Prod)** | <15 min | ~12 min |
| **Uptime** | >99.9% | - |
| **Rollback Time** | <2 min | ~90 sec |
| **Health Check Success Rate** | >95% | - |
| **Automatic Rollback Rate** | <1% | - |

---

## 🆘 Soporte

### Contacto

- 📧 Email: alvaro@example.com
- 💬 Slack: #weather-etl-ops
- 🐛 Issues: [GitHub Issues](https://github.com/AlvaroM99/weather-pipeline-etl/issues)

### Documentación Relacionada

- [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) - Mejoras de calidad de código
- [README.md](README.md) - Documentación general del proyecto
- [docker-compose.yml](docker-compose.yml) - Configuración base de servicios

---

**Última actualización**: 2025-02-09
**Versión**: 1.0.0
