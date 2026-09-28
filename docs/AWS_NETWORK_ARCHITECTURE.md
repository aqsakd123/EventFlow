# AWS network boundary

The application entrypoint is the `api-gateway` deployment. Its Kubernetes
Service is the only application Service declared as `type: LoadBalancer` and
is configured as an internet-facing AWS Network Load Balancer. The business
services remain internal `ClusterIP` Services:

```text
Internet
   |
Internet-facing AWS NLB
   |
api-gateway pods (:8080)       private subnets
   |\
   | +--> event-service (:8081, ClusterIP)
   +-----> registration-service (:8082, ClusterIP)
             |
             +--> private databases / brokers

Private pods -- HTTPS egress --> NAT Gateway --> ipwho.is / Open-Meteo
```

The public path is therefore:

```text
client -> LoadBalancer(api-gateway) -> api-gateway -> internal service DNS
```

`event-service` and `registration-service` have no public LoadBalancer or
NodePort. In the active Docker Compose lab they only bind diagnostic ports to
`127.0.0.1`; the gateway is the only service bound to all host interfaces.

## AWS prerequisites

- Run EKS worker nodes/pods in private subnets.
- Provide public subnets for the internet-facing load balancer and private
  subnets with a default route through a NAT Gateway for outbound HTTPS.
- Install/configure the AWS Load Balancer Controller or the cluster's AWS
  cloud-provider integration so the `LoadBalancer` Service provisions the NLB.
- Allow inbound 80/443 at the load balancer security boundary, allow the load
  balancer to reach gateway port 8080, and allow gateway pods to reach the
  internal service ports.
- Keep RDS, brokers, and other stateful dependencies private; do not add
  public Services for them.

The repository contains the Kubernetes boundary and application routing. An
actual AWS VPC/EKS/NLB/NAT deployment is still a manual deployment step and
is not claimed as executed in this workspace.
