"""Exercise actual app registration in a subprocess with only synthetic durable data."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class AccountAdviceRoutingTests(unittest.TestCase):
    def test_static_request_id_lookup_precedes_dynamic_account_descriptor(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = dict(os.environ, ASTOCK_RESEARCH_DB_PATH=str(root / "jobs.db"),
                               ASTOCK_RUNTIME_DIR=str(root / "runs"),
                               TRADINGAGENTS_RESULTS_DIR=str(root / "reports"),
                               TRADINGAGENTS_CACHE_DIR=str(root / "cache"),
                               TRADINGAGENTS_MEMORY_LOG_PATH=str(root / "memory.md"))
            script = '''
import asyncio, base64, importlib, json, os
from unittest.mock import patch
import httpx
from research_api.http.dto import ResearchJobRequest
from research_api.jobs.model import _Job
from research_api.storage.job_store import ResearchJobStore
request = ResearchJobRequest(ticker="600519", trade_date="2026-10-08", client_request_id="account-advice-model")
job = _Job(job_id="synthetic-job", request=request, ticker="600519", trade_date="2026-10-08",
           config={"llm_provider":"deepseek", "deep_think_llm":"synthetic-deep", "backend_url":"https://synthetic.invalid/v1"}, selected_analysts=["market"])
job.status = "succeeded"
job.result = {"ticker":"600519", "trade_date":"2026-10-08", "report_path":os.environ["TRADINGAGENTS_RESULTS_DIR"] + "/synthetic.json"}
store = ResearchJobStore()
store.save(job.persistence_record())
store.close()
# App import may restore only this fixture; never load repository or production dotenv.
with patch("dotenv.load_dotenv", return_value=False):
    module = importlib.import_module("research_api.app")
os.environ["ASTOCK_ACCOUNT_ADVICE_TOKEN"] = "synthetic_" * 8
os.environ["ASTOCK_ACCOUNT_MODEL_LIMITS_JSON"] = json.dumps([{"provider":"deepseek", "model":"synthetic-deep", "backend_url":"https://synthetic.invalid/v1", "context_window":1000000, "output_reserve":4000}])
async def check():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=module.app), base_url="http://synthetic.invalid") as client:
        lookup = await client.get("/research/jobs/by-client-request-id/account-advice-model")
        assert lookup.status_code == 200, "static lookup was shadowed"
        assert lookup.json()["job_id"] == job.job_id
        assert lookup.json()["client_request_id"] == "account-advice-model"
        report_id = base64.urlsafe_b64encode(job.result["report_path"].encode()).decode().rstrip("=")
        descriptor = await client.get("/research/jobs/synthetic-job/account-advice-model", params={"report_id":report_id, "ticker":"600519", "trade_date":"2026-10-08"}, headers={"Authorization":"Bearer " + os.environ["ASTOCK_ACCOUNT_ADVICE_TOKEN"]})
        assert descriptor.status_code == 200, "account descriptor registration failed"
        assert descriptor.json()["model"] == "synthetic-deep"
asyncio.run(check())
module._job_store.close()
print("routing-regression: passed")
'''
            completed = subprocess.run([sys.executable, "-c", script], env=environment,
                                       cwd=Path(__file__).resolve().parents[3], capture_output=True,
                                       text=True, timeout=30, check=True)
            self.assertIn("routing-regression: passed", completed.stdout)


if __name__ == "__main__":
    unittest.main()
