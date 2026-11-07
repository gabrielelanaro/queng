#!/usr/bin/env python3
"""Generate CSV inputs for the distributed join fixture."""

import argparse
import csv
from pathlib import Path
import random


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sales", type=positive, default=100_000)
    parser.add_argument("--products", type=positive, default=10_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("samples/generated"))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    randomizer = random.Random(args.seed)
    with (args.output / "products.csv").open("w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["product_id", "product"])
        for product in range(1, args.products + 1):
            writer.writerow([product, f"product_{product}"])
    with (args.output / "sales.csv").open("w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["product_id", "sale"])
        for _ in range(args.sales):
            writer.writerow([randomizer.randint(1, args.products), randomizer.randint(1, 1000)])
    print(f"Generated {args.sales:,} sales and {args.products:,} products in {args.output}")


if __name__ == "__main__":
    main()
