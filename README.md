# Day 11 — Broadcast and Accumulators

## Overview

This project demonstrates how Apache Spark uses **Broadcast Variables** and **Accumulators** when processing data in a distributed environment.

The scenario is transaction validation against a small product master table.

## Concepts Covered

- Broadcast Variables
- Accumulators
- RDD processing
- Distributed data processing
- Driver and Executor communication
- Read-only reference data
- Distributed counters
- Transaction validation

## Scenario

We have a small product master table:

| Product ID | Product |
|------------|---------|
| P101 | Laptop |
| P102 | Mouse |
| P103 | Keyboard |

Transactions are processed using an RDD.

Each transaction contains:

```text
(Transaction ID, Product ID, Quantity)
