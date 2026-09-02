
```markdown
# DYALEMCHIRZ - AI-Native Resilience Operating Platform

DYALEMCHIRZ is an AI-native platform built on top of Kubernetes for understanding, protecting, simulating, and recovering complex infrastructure.

## Architecture

DYALEMCHIRZ consists of five core engines:

1. **Asset Graph** - Continuously updated graph of infrastructure assets and dependencies
2. **AI Engine** - Provider-neutral AI for anomaly detection, prediction, and reasoning
3. **Resilience Engine** - Detect, assess, predict, recommend, and recover
4. **Digital Twin** - What-if simulation of infrastructure failures
5. **Edge Fabric** - Local operation with intermittent connectivity

## Building

From the root of the repository:

```bash
go build -o dya/bin/dya-controller ./dya/cmd/dya-controller
go build -o dya/bin/dya-cli ./dya/cmd/dya-cli
```

Running

```bash
# Run the controller
./dya/bin/dya-controller

# Use the CLI
./dya/bin/dya-cli version
./dya/bin/dya-cli health
```

Development Phases

· Phase 0: Architecture Audit ✅
· Phase 1: Foundation (Current)
· Phase 2: Asset Model
· Phase 3: Asset Graph
· Phase 4: Event Intelligence
· Phase 5: AI Engine
· Phase 6: Resilience Engine
· Phase 7: Digital Twin
· Phase 8: Security Intelligence
· Phase 9: Edge Fabric

License

Apache License 2.0 - see LICENSE for details.

DYALEMCHIRZ is a fork of Kubernetes and maintains all upstream licensing and attribution.

```

- Commit: `docs: add DYALEMCHIRZ README`

---
