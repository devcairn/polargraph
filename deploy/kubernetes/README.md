# PolarGraph — Kubernetes Deployment

## Prerequisites

- Kubernetes 1.27+ (gRPC probe support requires 1.24+)
- `kubectl` configured against your cluster
- A container image pushed to a registry (see image tag below)
- A storage class that supports `ReadWriteOnce` (most cloud providers supply one)

## Deploy

1. **Set a real API key** in `secret.yaml` before applying:
   ```bash
   echo -n "your-secret-key" | base64
   # paste the result over the placeholder in secret.yaml
   ```

2. **Update the image** in `deployment.yaml` — replace `OWNER` with your
   GitHub org or username:
   ```yaml
   image: ghcr.io/YOUR_ORG/polargraph:v1.2.3
   ```

3. **Apply everything** with Kustomize:
   ```bash
   kubectl apply -k deploy/kubernetes/
   ```

4. **Verify**:
   ```bash
   kubectl -n polargraph get pods
   kubectl -n polargraph logs -l app=polargraph
   ```

## Updating the image tag

Edit `deployment.yaml` and change the `image:` line, then re-apply:

```bash
kubectl apply -k deploy/kubernetes/
# or patch in-place:
kubectl -n polargraph set image deployment/polargraph polargraph=ghcr.io/OWNER/polargraph:v1.2.4
```

## Scaling

### Manual replicas

PolarGraph uses RocksDB, which requires exclusive write access. **Do not
increase `replicas` on the primary deployment without setting up replication
first** — multiple writers against the same PVC will corrupt the database.

For read scaling, deploy a separate replica Deployment (see Read Replicas below).

### HPA (CPU-based)

The included `hpa.yaml` is wired for CPU at 70 %. It is only meaningful when
read replicas are in use. Enable it after configuring WAL replication:

```bash
kubectl apply -f deploy/kubernetes/hpa.yaml
```

## Enabling TLS (cert-manager)

Install [cert-manager](https://cert-manager.io/) and create a `Certificate`
resource. Then mount the resulting secret into the pod and pass the paths via
`--tls-cert` / `--tls-key`:

```yaml
# In deployment.yaml, add to env:
- name: POLARGRAPH_TLS_CERT
  value: /tls/tls.crt
- name: POLARGRAPH_TLS_KEY
  value: /tls/tls.key

# Add to volumeMounts:
- name: tls
  mountPath: /tls
  readOnly: true

# Add to volumes:
- name: tls
  secret:
    secretName: polargraph-tls   # created by cert-manager Certificate
```

The cert-manager `Certificate` resource:

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: polargraph-tls
  namespace: polargraph
spec:
  secretName: polargraph-tls
  dnsNames:
    - polargraph-grpc.polargraph.svc.cluster.local
  issuerRef:
    name: letsencrypt-prod   # adjust to your Issuer
    kind: ClusterIssuer
```

## Read replicas via WAL streaming

1. Keep the primary Deployment (`replicas: 1`) as-is.

2. Create a second Deployment for replicas. Add this env var to the replica
   container (pointing at the primary's in-cluster gRPC address):

   ```yaml
   - name: POLARGRAPH_REPLICA_OF
     value: "polargraph-grpc.polargraph.svc.cluster.local:50051"
   ```

3. Replicas reject all write RPCs and stream WAL entries from the primary.
   Route read traffic to them via a separate Service selecting on a
   `role: replica` label.

4. With replicas in place, the HPA on the replica Deployment scales reads
   horizontally without touching the primary's PVC.
