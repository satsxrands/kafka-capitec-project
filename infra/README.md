# Local Kafka cluster (Rancher Desktop)

3 Kafka brokers in KRaft mode (no Zookeeper) on Rancher Desktop's built-in Kubernetes (k3s, containerd).
Pod and service names match the course templates: `kafka-0`, `kafka-service:9092`.

## Start

```bash
rdctl start --container-engine.name=containerd --kubernetes.enabled=true \
  --virtual-machine.memory-in-gb=8 --virtual-machine.number-cpus=4
kubectl config use-context rancher-desktop
kubectl apply -f infra/kafka.yaml
kubectl rollout status statefulset/kafka
```

## Check

```bash
kubectl exec kafka-0 -- kafka-metadata-quorum --bootstrap-server kafka-service:9092 describe --status
```

## Stop / reset

```bash
rdctl shutdown                               # stop, keep data
kubectl delete -f infra/kafka.yaml && kubectl delete pvc -l app=kafka   # wipe the cluster
```
