# Flight Reservations Lab — AKS / Kafka / Flink / Couchbase

Este laboratorio demuestra mediante un savepoint y una restauración que los
PVC locales de los TaskManagers pueden reemplazarse sin perder el estado
recuperable almacenado externamente. El resultado depende de ejecutar
exitosamente la prueba descrita en este documento.

## Versiones fijadas

| Componente | Versión |
|---|---|
| Flink | 1.20.0 (imagen `flink:1.20-scala_2.12-java17`) |
| Flink Kubernetes Operator | 1.15.0 |
| Strimzi Kafka Operator | 0.48.0 |
| Couchbase Autonomous Operator | 2.8.x |
| Couchbase Server | 7.6.4 |
| ABFS Hadoop client | 3.4.1 |
| AKS | clúster existente `aks-cluster-dev` |
| Namespace | `stratos-rnd-dev` |

## Estado del laboratorio

**READY FOR NON-DESTRUCTIVE PHASE A**

**NOT YET READY FOR DESTRUCTIVE PHASE (migración PVC)**

Antes de ejecutar la migración destructiva, validar en el AKS real:

1. PyFlink + PythonDriver + Flink 1.20 + Operator 1.15 en runtime.
2. Workload Identity + ABFS funcionando dentro del pod.
3. Cómo reacciona exactamente Flink Kubernetes Operator 1.15 a un cambio
   de `volumeClaimTemplates` (20Gi → 10Gi) mientras el deployment está
   suspendido.

## Modo Standalone

El FlinkDeployment usa **`mode: standalone`** porque los
`taskManager.volumeClaimTemplates` que permiten PVC individuales por
TaskManager están soportados a través de **StatefulSet** en modo
standalone. En modo `native` el operador usa Deployments y no
proporciona ese mecanismo de PVC por réplica[reference:0].

## PVC: almacenamiento local del TaskManager

### volumeClaimTemplates + volumeMounts

Cada `volumeClaimTemplate` debe tener un `volumeMounts` correspondiente
en el `podTemplate` para que el volumen sea realmente accesible desde el
contenedor de Flink[reference:1]:

```yaml
podTemplate:
  spec:
    containers:
      - name: flink-main-container
        volumeMounts:
          - name: taskmanager-state
            mountPath: /pv
```

Sin este mount, Kubernetes crea los PVC pero Flink no los utiliza.

### Working Directory y Local Recovery

Para que el PVC represente efectivamente el **working state** de Flink,
se configura:

```yaml
flinkConfiguration:
  state.backend: rocksdb
  state.backend.local-recovery: "true"
  process.taskmanager.working-dir: "/pv"
```

- `state.backend.local-recovery`: habilita la recuperación local del
  estado desde el working directory[reference:2].
- `process.taskmanager.working-dir`: apunta al volumen montado (`/pv`),
  donde RocksDB almacena su working directory y los artefactos locales
  del TaskManager[reference:3].

### Identidad determinística del TaskManager

Para local recovery entre reinicios, el TaskManager necesita un
`taskmanager.resource-id` determinístico[reference:4]. En Kubernetes con
StatefulSet, el nombre del Pod es estable, por lo que se inyecta como
variable de entorno:

```yaml
podTemplate:
  spec:
    containers:
      - name: flink-main-container
        env:
          - name: POD_NAME
            valueFrom:
              fieldRef:
                fieldPath: metadata.name
```

Y se configura:

```yaml
flinkConfiguration:
  taskmanager.resource-id: "$(POD_NAME)"
```

Esto garantiza que `tm_$(POD_NAME)` sea siempre el mismo working directory
para el mismo Pod, permitiendo la recuperación local tras un reinicio.

### Diagrama

```
                    Flink
                      │
             ┌────────┴────────┐
             │                 │
       local working       durable state
          state                 │
             │                  │
          PVC 20Gi           ADLS Gen2
             │             checkpoints
             │             savepoints
             │             HA
             │
       /pv dentro del TM
```

## Semántica de procesamiento

Flink utiliza checkpoints con `EXACTLY_ONCE` para mantener consistente el
**estado administrado por Flink** durante la recuperación.

El sink hacia Couchbase utiliza `upsert(reservationId, doc)`, por lo que las
escrituras son **idempotentes** respecto al `reservationId`.

El laboratorio demuestra recuperación del **estado de Flink** mediante un
savepoint externo y escritura idempotente en Couchbase. Esto **no debe
interpretarse** como una garantía de exactly-once transaccional extremo a
extremo entre Kafka, Flink y Couchbase.

## PyFlink: pendiente de validación en runtime

La ejecución de un job PyFlink en FlinkDeployment con Operator 1.15
**requiere validación en runtime con la imagen exacta de Flink 1.20**. La
documentación oficial de Flink muestra `flink run --python job.py` como la
forma canónica, y el comportamiento del Operator con `jarURI` + `entryClass`
puede variar.

**Estrategia**: desplegar primero **sin tocar los PVC** (Fase A) para validar
el stack completo.

```bash
kubectl logs -n stratos-rnd-dev <jobmanager-pod> | grep -iE "python|driver|class|module"
```

## volumeClaimTemplates ≠ resize automático de PVC

**IMPORTANTE**:

Los `volumeClaimTemplates` definen las características deseadas de los PVC
utilizados por los TaskManagers, pero **no deben interpretarse como un
mecanismo para reducir PVC existentes**.

En este laboratorio los `volumeClaimTemplates` se utilizan con
`mode: standalone`, donde el Operator utiliza StatefulSets para provisionar
los PVC de los TaskManagers.

Cambiar `storage: 20Gi` a `storage: 10Gi` **no implica** que los PVC
existentes de 20Gi sean reducidos automáticamente.

Kubernetes no permite reducir un PVC por debajo de su capacidad actual.
Por lo tanto, la migración requiere eliminar los PVC existentes y permitir
su recreación con el tamaño deseado.

Antes de cualquier eliminación se debe inspeccionar el StatefulSet generado,
el FlinkDeployment y los PVC reales.

**NO continuar** únicamente porque Git contenga `10Gi`.

## Repetibilidad del laboratorio

El modo `initial` del producer debe ejecutarse **solo una vez contra un
estado fresco**. Si se vuelve a ejecutar sin reset, el ValueState de Flink
acumula (`AV123 = 74`, `AV456 = 126`) aunque Couchbase haga upsert.

Ver sección “Reset del laboratorio” al final.

## Prerrequisitos de Infraestructura (NO versionados en Git)

### 1. Habilitar Workload Identity en AKS

```bash
az aks update -g <RG> -n aks-cluster-dev --enable-oidc-issuer --enable-workload-identity
```

### 2. Crear Managed Identity, rol y Federated Credential

```bash
az identity create --name flink-lab-identity -g <RG> -l eastus
CLIENT_ID=$(az identity show -n flink-lab-identity -g <RG> --query clientId -o tsv)
TENANT_ID=$(az account show --query tenantId -o tsv)
SA_ID=$(az storage account show -n <SA> -g <RG> --query id -o tsv)

az role assignment create --assignee $CLIENT_ID \
  --role "Storage Blob Data Contributor" --scope $SA_ID

OIDC_ISSUER=$(az aks show -n aks-cluster-dev -g <RG> \
  --query oidcIssuerProfile.issuerUrl -o tsv)

az identity federated-credential create \
  --name flink-fed-cred \
  --identity-name flink-lab-identity \
  -g <RG> \
  --issuer $OIDC_ISSUER \
  --subject system:serviceaccount:stratos-rnd-dev:flink \
  --audience api://AzureADTokenExchange
```

### 3. Crear el Secret de Couchbase (CRÍTICO — no debe estar en Git)

```bash
kubectl create secret generic couchbase-auth \
  -n stratos-rnd-dev \
  --from-literal=username=Administrator \
  --from-literal=password='<TU_PASSWORD_SEGURO>'
```

### 4. Crear contenedores en Azure Storage (ADLS Gen2)

```bash
az storage container create --name flink-checkpoints --account-name <SA>
az storage container create --name flink-savepoints --account-name <SA>
az storage container create --name flink-ha --account-name <SA>
```

### 5. Argo Workflows

Debe estar instalado. ServiceAccount y Role/RoleBinding versionados.

### 6. Verificar capacidad real del namespace `stratos-rnd-dev`

```bash
kubectl get pvc -n stratos-rnd-dev
kubectl describe resourcequota -n stratos-rnd-dev
kubectl describe nodes | grep -A 5 "Allocated resources"
```

## Verificación de Workload Identity en runtime

```bash
JM_POD=$(kubectl get pods -n stratos-rnd-dev -l component=jobmanager -o name | head -1)
kubectl exec -n stratos-rnd-dev $JM_POD -- env | grep AZURE_
kubectl exec -n stratos-rnd-dev $JM_POD -- ls -l /var/run/secrets/azure/tokens/
```

Debe mostrar `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE`
y el token file respectivamente.

## Contabilidad de cuota

| Recurso | Antes | Después |
|---|---|---|
| Flink TM-1 | 20 GiB | 10 GiB |
| Flink TM-2 | 20 GiB | 10 GiB |
| Flink TM-3 | 20 GiB | 10 GiB |
| Kafka | 5 GiB | 5 GiB |
| Couchbase | 20 GiB | 20 GiB |
| **Total** | **85 GiB** | **55 GiB** |
| Quota | 100 GiB | 100 GiB |
| Margen | 15 GiB | 45 GiB |

## Despliegue inicial (Fase A)

```bash
kubectl apply \
  -f namespace/ \
  -f quota/ \
  -f kafka/ \
  -f couchbase/ \
  -f flink/
```

Verificar Workload Identity. Ejecutar WorkflowTemplate en modo `initial`.
**NO continuar** hasta que pase.

Inspeccionar recursos reales generados por el operador en modo standalone:

```bash
kubectl get flinkdeployment flight-reservation-processor \
  -n stratos-rnd-dev -o yaml | grep -A 10 volumeClaimTemplates
kubectl get statefulset -n stratos-rnd-dev
kubectl get pvc -n stratos-rnd-dev
kubectl get svc -n stratos-rnd-dev
```

## Ejecución del WorkflowTemplate

### Validación inicial

```bash
argo submit --from workflowtemplate/flight-reservations-integration \
  -n stratos-rnd-dev \
  -p test-mode=initial \
  -p expected-av123=37 -p expected-av456=63 \
  -p expected-flight-count-av123=37 -p expected-flight-count-av456=63
```

### Validación post-migración

```bash
argo submit --from workflowtemplate/flight-reservations-integration \
  -n stratos-rnd-dev \
  -p test-mode=post-migration \
  -p expected-av123=38 -p expected-av456=64 \
  -p expected-flight-count-av123=38 -p expected-flight-count-av456=64
```

## Procedimiento de migración (secuencia v12)

```
A. Deploy inicial (mode: standalone, PVC montado, local recovery)
B. Ejecutar initial test (37/63)
C. Crear savepoint
D. Esperar COMPLETED
E. Suspender Flink (Git)
F. Confirmar SUSPENDED + 0 TaskManagers
G. PAUSAR AUTO-SYNC DE ARGO CD   ← ANTES del cambio Git
H. Cambiar Git 20Gi → 10Gi
I. Commit + push
J. Manual Argo CD sync --prune=false
K. Inspeccionar FlinkDeployment + StatefulSet
L. Confirmar desired storage = 10Gi
M. Confirmar PVC existentes siguen siendo 20Gi  ← es lo esperado
N. FINAL DESTRUCTIVE CHECK (5 condiciones)
O. DELETE de los 3 PVC  ← ÚNICA OPERACIÓN MANUAL
P. Confirmar que NO aparecen PVC 20Gi nuevos
Q. Restaurar savepoint (Git)
R. Reanudar Argo CD auto-sync
S. Ejecutar post-migration test (38/64)
```

### Fase A — Validación inicial (SIN tocar PVC)

Ver comandos arriba.

### Fase B — Crear savepoint y esperar COMPLETED

```bash
JM_SVC=$(kubectl get svc -n stratos-rnd-dev | grep flight-reservation-processor | grep jobmanager | awk '{print $1}')
kubectl port-forward -n stratos-rnd-dev svc/$JM_SVC 8081:8081 &

JOB_ID=$(curl -s http://localhost:8081/jobs | jq -r '.jobs[0].id')

RESP=$(curl -s -X POST http://localhost:8081/jobs/$JOB_ID/savepoints \
  -H "Content-Type: application/json" \
  -d '{"target-directory":"abfss://flink-savepoints@<SA>.dfs.core.windows.net/savepoints","cancel-job":false}')

REQUEST_ID=$(echo "$RESP" | jq -r '."request-id"')

while true; do
  STATUS=$(curl -s http://localhost:8081/jobs/$JOB_ID/savepoints/$REQUEST_ID | jq -r '.status.id')
  echo "Status: $STATUS"
  case "$STATUS" in
    COMPLETED)
      SAVEPOINT_PATH=$(curl -s http://localhost:8081/jobs/$JOB_ID/savepoints/$REQUEST_ID | jq -r '."operation.location"')
      echo "$SAVEPOINT_PATH" > /tmp/savepoint-path.txt
      break ;;
    FAILED|"") exit 1 ;;
  esac
  sleep 5
done
```

**No continuar** hasta que `Status: COMPLETED`.

### Fase C — Suspender job vía Git

Editar `flink/flink-deployment.yaml`: `state: suspended`. Commit, push.

### Fase D — Verificar suspensión y TaskManagers detenidos

```bash
kubectl get flinkdeployment flight-reservation-processor \
  -n stratos-rnd-dev -o jsonpath='{.status.jobStatus.state}'
# Debe mostrar SUSPENDED

kubectl get pods -n stratos-rnd-dev -l component=taskmanager
# Debe estar VACÍO
```

### Fase E — PAUSAR AUTO-SYNC DE ARGO CD (ANTES del cambio Git)

```bash
argocd app set stratos-rnd-dev --sync-policy manual
```

Equivalente por kubectl:

```bash
kubectl patch application stratos-rnd-dev -n argocd \
  --type merge -p '{"spec":{"syncPolicy":{"automated":null}}}'
```

Verificar:

```bash
argocd app get stratos-rnd-dev | grep -i "Sync Policy"
# Debe indicar Manual
```

### Fase F — Cambiar tamaño de PVC vía Git

Editar `flink/flink-deployment.yaml`: `storage: 20Gi` → `storage: 10Gi`.

Commit y push:

```bash
git add flink/flink-deployment.yaml
git commit -m "chore: reduce taskmanager PVCs from 20Gi to 10Gi"
git push
```

### Fase G — Manual Argo CD sync --prune=false

```bash
argocd app sync stratos-rnd-dev --prune=false
```

**Importante**: `--prune=false` evita que Argo CD elimine recursos
inesperadamente durante una operación sensible.

### Fase H — Inspeccionar FlinkDeployment y StatefulSet

```bash
kubectl get flinkdeployment flight-reservation-processor \
  -n stratos-rnd-dev \
  -o jsonpath='{.spec.taskManager.volumeClaimTemplates[0].spec.resources.requests.storage}{"\n"}'
# Debe devolver 10Gi

STATEFULSET_NAME=$(kubectl get statefulset -n stratos-rnd-dev \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep taskmanager | head -1)

kubectl get statefulset $STATEFULSET_NAME -n stratos-rnd-dev \
  -o jsonpath='{.spec.volumeClaimTemplates[*].spec.resources.requests.storage}{"\n"}'
# Debe devolver 10Gi
```

### Fase I — Confirmar PVC existentes siguen siendo 20Gi

**Esto es lo esperado.** Los PVC antiguos NO se reducen.

```bash
kubectl get pvc -n stratos-rnd-dev
# taskmanager-state-* → 20Gi (esperado)
```

### Fase J — FINAL DESTRUCTIVE CHECK (5 condiciones)

```bash
echo "=== FINAL DESTRUCTIVE CHECK ==="

echo "1. Flink state:"
kubectl get flinkdeployment flight-reservation-processor \
  -n stratos-rnd-dev \
  -o jsonpath='{.status.jobStatus.state}{"\n"}'

echo "2. TaskManagers:"
kubectl get pods -n stratos-rnd-dev -l component=taskmanager

echo "3. Existing PVCs:"
kubectl get pvc -n stratos-rnd-dev

echo "4. FlinkDeployment desired storage:"
kubectl get flinkdeployment flight-reservation-processor \
  -n stratos-rnd-dev \
  -o jsonpath='{.spec.taskManager.volumeClaimTemplates[0].spec.resources.requests.storage}{"\n"}'

echo "5. Argo CD sync policy:"
argocd app get stratos-rnd-dev | grep -i "Sync Policy"
```

**Requisitos obligatorios** (todos):

1. Flink state = `SUSPENDED`
2. TaskManager pods = **vacío / “No resources found”**
3. PVCs = los 3 PVC esperados de **20Gi** (estado actual)
4. desired storage = **10Gi** (estado deseado)
5. Argo CD = **Manual**

**STOP CONDITION**: Si cualquier condición falla, **DETENER**. No eliminar
los PVC hasta que todas se cumplan.

### Fase K — DELETE PVC (ÚNICA OPERACIÓN MANUAL DEL LABORATORIO)

**Advertencia**: operación manual y destructiva. El estado recuperable está
en el savepoint de Azure.

**No usar selectores como `-l app=flink`** porque los PVC generados por
el StatefulSet no necesariamente heredan esas labels. Obtener los nombres
reales primero:

```bash
kubectl get pvc -n stratos-rnd-dev
```

Y eliminar explícitamente:

```bash
kubectl delete pvc <pvc-tm-0> <pvc-tm-1> <pvc-tm-2> -n stratos-rnd-dev
```

**Motivo de mantenerlo manual**: GitOps no significa que todas las
operaciones destructivas deban ser completamente automáticas.

- **GitOps**: declara el nuevo estado (10Gi).
- **Kubernetes**: no puede shrinkear un PVC existente.
- **Operator**: administra continuamente el FlinkDeployment.
- **Humano**: ejecuta explícitamente la operación destructiva tras
  verificar las STOP CONDITIONS.

### Fase L — Confirmar que NO se recrean PVC 20Gi

```bash
kubectl get pvc -n stratos-rnd-dev -w
```

En otra terminal:

```bash
kubectl get statefulset -n stratos-rnd-dev -w
```

Y:

```bash
kubectl get events -n stratos-rnd-dev --sort-by=.lastTimestamp
```

**STOP CONDITION**: Si algún PVC `taskmanager-state-*` con 20 GiB aparece,
**DETENER**. No eliminar repetidamente. Verificar que Argo CD sigue en modo
manual y que el FlinkDeployment dice `10Gi`.

**No asumir** que el Operator creará inmediatamente los nuevos PVC de 10Gi.
Ese es precisamente uno de los comportamientos que el experimento debe
observar.

### Fase M — Restaurar desde savepoint vía Git

Editar `flink/flink-deployment.yaml`:

```yaml
  job:
    state: running
    initialSavepointPath: "<SAVEPOINT_PATH de Fase B>"
    savepointRedeployNonce: 1
```

Commit, push.

### Fase N — Reanudar Argo CD auto-sync

```bash
argocd app set stratos-rnd-dev --sync-policy automated
argocd app sync stratos-rnd-dev
```

### Fase O — Validación post-migración

```bash
argo submit --from workflowtemplate/flight-reservations-integration \
  -n stratos-rnd-dev \
  -p test-mode=post-migration \
  -p expected-av123=38 -p expected-av456=64 \
  -p expected-flight-count-av123=38 -p expected-flight-count-av456=64
```

## Reset del laboratorio

**IMPORTANTE**: Pausar Argo CD antes de cualquier operación de reset.

```
0. PAUSAR ARGO CD AUTO-SYNC
1. Suspender Flink
2. Eliminar FlinkDeployment
3. Eliminar PVC (con nombres reales, sin selectores)
4. Limpiar Couchbase (collection processed o truncar bucket)
5. Eliminar topic de Kafka y recrearlo
6. Restaurar manifests en Git
7. Reactivar Argo CD
8. Ejecutar initial
```

Comandos:

```bash
# 0. Pausar Argo CD
argocd app set stratos-rnd-dev --sync-policy manual

# 1-2. Suspender y eliminar FlinkDeployment
kubectl delete flinkdeployment flight-reservation-processor -n stratos-rnd-dev

# 3. Eliminar PVC (obtener nombres reales primero)
kubectl get pvc -n stratos-rnd-dev
kubectl delete pvc <pvc-real-0> <pvc-real-1> <pvc-real-2> -n stratos-rnd-dev

# 4. Limpiar Couchbase (via cbq)
# cbq -u Administrator -p <pass> -e http://flight-couchbase-srv:8093 \
#   -s "DELETE FROM \`flight-reservations\`.\`reservations\`.\`processed\`;"

# 5. Recrear topic Kafka
kubectl delete kafkatopic flight-reservations -n stratos-rnd-dev
kubectl apply -f kafka/kafka-topic.yaml

# 6-7. Restaurar manifests y reactivar Argo CD
git checkout main -- flink/flink-deployment.yaml
argocd app set stratos-rnd-dev --sync-policy automated
argocd app sync stratos-rnd-dev

# 8. Ejecutar initial test
argo submit --from workflowtemplate/flight-reservations-integration \
  -n stratos-rnd-dev -p test-mode=initial
```

**NO** ejecutar `initial` dos veces sin reset.