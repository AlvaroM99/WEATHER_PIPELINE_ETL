# Setup Rápido - Continuous Deployment

Guía de configuración rápida para poner en marcha el sistema de CD en 30 minutos.

---

## ⚡ Quick Setup (30 minutos)

### Paso 1: Generar SSH Keys (5 min)

```bash
# Local machine
cd ~/.ssh

# Generar keys para development
ssh-keygen -t ed25519 -C "github-actions-dev" -f weather-etl-dev
# Password: <dejar vacío>

# Generar keys para production
ssh-keygen -t ed25519 -C "github-actions-prod" -f weather-etl-prod
# Password: <dejar vacío>

# Resultado:
# weather-etl-dev (private key)
# weather-etl-dev.pub (public key)
# weather-etl-prod (private key)
# weather-etl-prod.pub (public key)
```

### Paso 2: Configurar Servidores (10 min)

#### En Servidor de Development

```bash
# SSH como root o con sudo
ssh root@dev.weather-etl.com

# Crear usuario deploy
useradd -m -s /bin/bash deploy
usermod -aG docker deploy

# Configurar SSH
mkdir -p /home/deploy/.ssh
chmod 700 /home/deploy/.ssh

# Copiar public key (desde local)
cat ~/.ssh/weather-etl-dev.pub  # Copiar contenido

# En servidor, pegar en authorized_keys
echo "<PASTE_PUBLIC_KEY_HERE>" >> /home/deploy/.ssh/authorized_keys
chmod 600 /home/deploy/.ssh/authorized_keys
chown -R deploy:deploy /home/deploy/.ssh

# Crear estructura del proyecto
mkdir -p /opt/weather-etl
chown -R deploy:deploy /opt/weather-etl

# Test SSH (desde local)
ssh -i ~/.ssh/weather-etl-dev deploy@dev.weather-etl.com
# Si conecta OK → ✅
```

#### En Servidor de Production

```bash
# Repetir los mismos pasos que en dev
ssh root@prod.weather-etl.com

useradd -m -s /bin/bash deploy
usermod -aG docker deploy

mkdir -p /home/deploy/.ssh
chmod 700 /home/deploy/.ssh

# Usar weather-etl-prod.pub esta vez
cat ~/.ssh/weather-etl-prod.pub  # Copiar

echo "<PASTE_PUBLIC_KEY_HERE>" >> /home/deploy/.ssh/authorized_keys
chmod 600 /home/deploy/.ssh/authorized_keys
chown -R deploy:deploy /home/deploy/.ssh

mkdir -p /opt/weather-etl
chown -R deploy:deploy /opt/weather-etl

# Crear directorios adicionales para production
mkdir -p /opt/weather-etl/{data,logs,backups}
mkdir -p /opt/weather-etl/data/{postgres,postgres-airflow,minio}
mkdir -p /opt/weather-etl/logs/airflow
chown -R deploy:deploy /opt/weather-etl

# Test SSH
ssh -i ~/.ssh/weather-etl-prod deploy@prod.weather-etl.com
```

### Paso 3: Clonar Repositorio en Servidores (5 min)

#### Development

```bash
ssh -i ~/.ssh/weather-etl-dev deploy@dev.weather-etl.com

cd /opt/weather-etl
git clone https://github.com/AlvaroM99/weather-pipeline-etl.git .

# Copiar .env
cp .env.development .env

# Editar con credenciales reales
nano .env
# Cambiar passwords, API keys, etc.

# Hacer scripts ejecutables
chmod +x scripts/*.sh
```

#### Production

```bash
ssh -i ~/.ssh/weather-etl-prod deploy@prod.weather-etl.com

cd /opt/weather-etl
git clone https://github.com/AlvaroM99/weather-pipeline-etl.git .

cp .env.production .env
nano .env
# ⚠️ IMPORTANTE: Usar passwords FUERTES en producción

chmod +x scripts/*.sh
```

### Paso 4: Configurar GitHub Secrets (5 min)

1. Ir a GitHub: **Settings → Secrets and variables → Actions**

2. Click **"New repository secret"**

3. Añadir los siguientes secrets:

```yaml
# Development
Name: DEV_SERVER_HOST
Value: dev.weather-etl.com (o tu hostname)

Name: DEV_SERVER_USER
Value: deploy

Name: DEV_SSH_PRIVATE_KEY
Value: <copiar contenido de ~/.ssh/weather-etl-dev>

# Production
Name: PROD_SERVER_HOST
Value: prod.weather-etl.com (o tu hostname)

Name: PROD_SERVER_USER
Value: deploy

Name: PROD_SSH_PRIVATE_KEY
Value: <copiar contenido de ~/.ssh/weather-etl-prod>

# Optional: Slack
Name: SLACK_WEBHOOK_URL
Value: https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

**Copiar private key correctamente**:

```bash
# En local
cat ~/.ssh/weather-etl-dev

# Copiar TODO incluyendo:
-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAA...
...
-----END OPENSSH PRIVATE KEY-----

# Pegar completo en GitHub Secret
```

### Paso 5: Test Manual (5 min)

#### Test en Development

```bash
# SSH al servidor
ssh -i ~/.ssh/weather-etl-dev deploy@dev.weather-etl.com

cd /opt/weather-etl

# Test health check script
bash scripts/health_check.sh
# Debe fallar (servicios no iniciados todavía) - OK

# Build imagen inicial
docker build -t weather-pipeline-etl:development .

# Iniciar servicios
docker-compose -f docker/compose/docker-compose.development.yml up -d

# Esperar 60 segundos
sleep 60

# Test health check de nuevo
bash scripts/health_check.sh
# Debe pasar: ✅ All health checks PASSED

# Test smoke tests
bash scripts/smoke_tests.sh
# Debe pasar: ✅ All smoke tests PASSED
```

Si todo pasa → ✅ Development está listo!

---

## 🧪 Test del Sistema CD

### Test 1: Deployment Automático a Dev

```bash
# En local
git checkout develop
git pull origin develop

# Hacer un cambio trivial
echo "# Test CD" >> README.md
git add README.md
git commit -m "test: CD deployment"
git push origin develop

# Ir a GitHub Actions
# https://github.com/AlvaroM99/weather-pipeline-etl/actions

# Verificar que se ejecutan:
# 1. CI Pipeline
# 2. Docker Build
# 3. CD - Development

# Esperar 5-8 minutos

# Verificar resultado en servidor
ssh -i ~/.ssh/weather-etl-dev deploy@dev.weather-etl.com
cd /opt/weather-etl
git log -1  # Debe mostrar tu último commit
docker ps   # Debe mostrar contenedores corriendo
```

### Test 2: Deployment Manual a Production

```bash
# En GitHub Actions
# https://github.com/AlvaroM99/weather-pipeline-etl/actions

# 1. Click en "CD - Production"
# 2. Click "Run workflow"
# 3. Select branch: master
# 4. Use workflow from: master
# 5. Inputs:
#    version: latest
#    skip_tests: false
# 6. Click "Run workflow"

# Esperar aprobación (si configurado)
# Esperar 15-20 minutos

# Verificar en servidor
ssh -i ~/.ssh/weather-etl-prod deploy@prod.weather-etl.com
cd /opt/weather-etl
docker ps
bash scripts/health_check.sh
```

### Test 3: Rollback

```bash
# En servidor de production
ssh -i ~/.ssh/weather-etl-prod deploy@prod.weather-etl.com
cd /opt/weather-etl

# Test de rollback
bash scripts/deploy.sh --rollback

# Verificar
bash scripts/health_check.sh
```

---

## 🔧 Troubleshooting

### Problema: SSH Connection Failed

```bash
# Verificar que la key está en el servidor
ssh -i ~/.ssh/weather-etl-dev deploy@dev.weather-etl.com "cat ~/.ssh/authorized_keys"

# Verificar permisos
ssh -i ~/.ssh/weather-etl-dev deploy@dev.weather-etl.com "ls -la ~/.ssh/"
# Debe ser: drwx------ (700) para .ssh/
#           -rw------- (600) para authorized_keys

# Test de conexión con verbose
ssh -vvv -i ~/.ssh/weather-etl-dev deploy@dev.weather-etl.com
```

### Problema: Docker Not Found

```bash
# Instalar Docker en servidor
ssh deploy@dev.weather-etl.com

# Ubuntu/Debian
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker deploy

# Logout y login de nuevo
exit
ssh deploy@dev.weather-etl.com

# Verificar
docker ps
docker-compose version
```

### Problema: Image Not Found

```bash
# Login a GHCR en servidor
ssh deploy@dev.weather-etl.com

# Crear GitHub Personal Access Token (classic)
# Settings → Developer settings → Personal access tokens → Tokens (classic)
# Scopes: read:packages

echo "ghp_YOUR_TOKEN" | docker login ghcr.io -u YOUR_USERNAME --password-stdin

# Test pull
docker pull ghcr.io/alvarom99/weather-pipeline-etl:development
```

### Problema: Health Checks Failing

```bash
# Ver logs de servicios
ssh deploy@dev.weather-etl.com
cd /opt/weather-etl

docker-compose -f docker/compose/docker-compose.development.yml logs airflow
docker-compose -f docker/compose/docker-compose.development.yml logs postgres

# Restart servicios
docker-compose -f docker/compose/docker-compose.development.yml restart

# Esperar 60s y reintentar
sleep 60
bash scripts/health_check.sh
```

---

## 📋 Checklist Final

### Pre-deployment
- [ ] SSH keys generadas
- [ ] Servidores configurados (dev/prod)
- [ ] Usuario deploy creado con permisos
- [ ] Repositorio clonado en servidores
- [ ] .env configurado con credenciales reales
- [ ] GitHub Secrets configurados
- [ ] Docker y docker-compose instalados

### Testing
- [ ] Health check manual pasando (dev)
- [ ] Smoke tests manual pasando (dev)
- [ ] Deployment automático testeado (dev)
- [ ] Deployment manual testeado (prod)
- [ ] Rollback testeado

### Production Ready
- [ ] Passwords fuertes en .env.production
- [ ] API keys con rate limits apropiados
- [ ] Firewall configurado (solo puertos necesarios)
- [ ] Backup schedule configurado
- [ ] Monitoreo configurado (Slack/email)
- [ ] Equipo entrenado en uso del sistema

---

## 🎓 Próximos Pasos

1. **Leer documentación completa**: [CD_GUIDE.md](CD_GUIDE.md)

2. **Configurar monitoreo avanzado**:
   - ELK Stack para logs
   - Prometheus para métricas
   - Grafana para dashboards

3. **Configurar backups automáticos**:
   ```bash
   # En servidor
   crontab -e
   # Añadir:
   0 2 * * * /opt/weather-etl/scripts/backup.sh
   ```

4. **Entrenar al equipo**:
   - Uso de GitHub Actions
   - Interpretación de logs
   - Proceso de rollback
   - Troubleshooting común

---

## 📞 Ayuda

Si encuentras problemas:

1. ✅ Consulta [CD_GUIDE.md](CD_GUIDE.md) - Troubleshooting section
2. ✅ Revisa logs de GitHub Actions
3. ✅ Verifica logs en servidor: `docker-compose logs`
4. ✅ Abre un issue: [GitHub Issues](https://github.com/AlvaroM99/weather-pipeline-etl/issues)

---

**¡Setup Completado!** 🎉

El sistema de CD está listo para usar. Cada push a `develop` desplegará automáticamente a development, y puedes hacer deploys manuales a production desde GitHub Actions.

*Tiempo total: ~30 minutos*
