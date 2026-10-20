$ErrorActionPreference = 'Stop'
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-config.yaml
kubectl apply -f k8s/02-storage.yaml
kubectl apply -f k8s/03-infrastructure.yaml
kubectl wait --for=condition=Ready pod -l app=nats -n pulsestream --timeout=180s
kubectl wait --for=condition=Ready pod -l app=postgres -n pulsestream --timeout=180s
kubectl apply -f k8s/04-application.yaml
kubectl wait --for=condition=Available deployment/pulsestream-api -n pulsestream --timeout=180s
kubectl wait --for=condition=Available deployment/pulsestream-worker -n pulsestream --timeout=180s
Write-Host 'PulseStream is ready. Run: kubectl port-forward svc/pulsestream-api 8000:8000 -n pulsestream'
