"""
Phase 8 Portfolio Project 10 — AI Executive Reporting Assistant
This is a proof-of-concept. Review all generated claims before sharing.
Run from the repository root:  python src/ai_executive_brief.py
Set OPENAI_API_KEY and optionally OPENAI_MODEL in your .env file (see .env.example).
Never commit your API key.
"""
import os
from pathlib import Path

import pandas as pd
from openai import OpenAI

REPO_ROOT = Path(__file__).resolve().parent.parent

try:
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env")
except ImportError:
    pass

def build_metrics(csv_path=REPO_ROOT / "data" / "supply_chain_orders_raw.csv"):
    df = pd.read_csv(csv_path)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_cost_ngn"] = pd.to_numeric(df["unit_cost_ngn"], errors="coerce")
    df["delivery_days"] = pd.to_numeric(df["delivery_days"], errors="coerce")
    df = df.drop_duplicates()
    df["order_value_ngn"] = df["quantity"] * df["unit_cost_ngn"]
    return {
        "row_count": int(len(df)),
        "total_order_value_ngn": round(float(df["order_value_ngn"].sum()), 2),
        "orders_by_status": df["status"].fillna("Unknown").value_counts().to_dict(),
        "average_delivery_days": round(float(df["delivery_days"].mean()), 2),
        "top_products_by_order_value": (
            df.groupby("product")["order_value_ngn"].sum()
              .sort_values(ascending=False).head(5).round(2).to_dict()
        )
    }

def generate_brief(metrics):
    client = OpenAI()  # reads OPENAI_API_KEY from environment
    model = os.getenv("OPENAI_MODEL", "replace-with-a-model-available-to-your-account")
    prompt = f"""
You are an analyst preparing a concise executive brief for a supply-chain manager.
Use ONLY the supplied metrics. Do not invent causes, explanations, or missing values.
Clearly distinguish observed facts from possible interpretations.
If a metric is missing, say so. Include:
1. Executive summary (3 bullets)
2. Key performance indicators
3. Risks or questions requiring investigation
4. Three practical next steps
5. A short data limitations note

Metrics (JSON-like Python dictionary):
{metrics}
"""
    response = client.responses.create(model=model, input=prompt)
    return response.output_text

if __name__ == "__main__":
    metrics = build_metrics()
    brief = generate_brief(metrics)
    with open("executive_brief.md", "w", encoding="utf-8") as f:
        f.write(brief)
    print(brief)
