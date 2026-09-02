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
