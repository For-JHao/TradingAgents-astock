"""Synthetic private endpoint tests; no dotenv, production files or paid calls."""
import asyncio
import base64
import copy
import json
import os
import unittest
from unittest.mock import patch
from contextlib import ExitStack
import httpx
from fastapi import FastAPI, Request
from langchain_core.messages import AIMessage
from research_api import account_advice as advice

TOKEN = "synthetic_" * 8
REPORT = base64.urlsafe_b64encode(b"/fixture/reports/report.json").decode().rstrip("=")
LIMIT = {"provider":"deepseek","model":"saved-deep","backend_url":"https://saved.invalid/v1","context_window":1000000,"output_reserve":4000}
JOB = {"job_id":"job1","ticker":"600519","trade_date":"2026-10-08","status":"succeeded",
       "config":{"llm_provider":"deepseek","deep_think_llm":"saved-deep","backend_url":"https://saved.invalid/v1"},
       "result":{"ticker":"600519","trade_date":"2026-10-08","report_path":"/fixture/reports/report.json"}}
PROFILE={"schemaVersion":1,"revision":1,"currency":"CNY","cashAvailable":None,"positions":[{"ticker":"600519","market":"SSE","companyName":"贵州茅台","averageCost":"0","quantity":"1"}],"updatedAt":"2026-10-08T00:00:00Z"}
PAYLOAD={"researchContext":{"schemaVersion":1,"identity":{"bindingId":"b1","sourceRequestId":"r1","jobId":"job1","reportId":REPORT,"ticker":"600519","tradeDate":"2026-10-08","metadata":{"engineVersion":"v0.5.21"},"sourceChecksum":"a"*64},
 "analystReports":[{"role":"market","status":"selected","content":"完整原研究😀"*12000}],"dataQuality":{"summary":"披露缺口","missingTasks":[],"complete":True},
 "investmentDebate":{"history":"多头1 空头1 多头2","judge_decision":"保留裁决","float":1e-7},"investmentPlan":"完整投资计划","traderPlan":"完整交易论证",
 "riskDebate":{"history":"激进1 保守1 中性1","judge_decision":"保留风险裁决"},"finalDecision":{"content":"最终投资建议：持有","rating":"Hold"},"priorContext":"原始记忆"},
 "account":{"profile":PROFILE,"capturedAt":"2026-10-08T00:00:00Z"},"valuation":{"valuationAt":"2026-10-08T00:01:00Z","rows":[],"marketValue":None,"totalAssets":None,"cashRatio":None,"complete":False}}

class EndpointTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.environment = patch.dict(os.environ, {"ASTOCK_ACCOUNT_ADVICE_TOKEN":TOKEN,"ASTOCK_ACCOUNT_MODEL_LIMITS_JSON":json.dumps([LIMIT]),"DEEPSEEK_API_KEY":"synthetic-key","ASTOCK_DEEP_THINK_LLM":"current-wrong","ASTOCK_BACKEND_URL":"https://current-wrong.invalid"})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.job = copy.deepcopy(JOB)
        self.app = FastAPI()
        @self.app.get("/research/jobs/{job_id}/account-advice-model")
        async def descriptor(request:Request,job_id:str):
            return await advice.model_descriptor(request,job_id,lambda _:copy.deepcopy(self.job))
        @self.app.post("/research/jobs/{job_id}/account-advice")
        async def invoke(request:Request,job_id:str):
            return await advice.account_advice(request,job_id,lambda _:copy.deepcopy(self.job))
        self.client=httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app),base_url="http://internal.invalid",headers={"Authorization":"Bearer "+TOKEN})
        self.addAsyncCleanup(self.client.aclose)
        self.query={"report_id":REPORT,"ticker":"600519","trade_date":"2026-10-08"}

    async def descriptor(self):
        response=await self.client.get("/research/jobs/job1/account-advice-model",params=self.query)
        self.assertEqual(response.status_code,200)
        return response.json()

    async def invocation(self):
        descriptor=await self.descriptor()
        return {"schemaVersion":1,"reportId":REPORT,"ticker":"600519","tradeDate":"2026-10-08","modelIdentity":descriptor["modelIdentity"],"timeoutMs":600000,"payload":copy.deepcopy(PAYLOAD)}

    async def test_descriptor_exact_saved_config_and_missing_endpoint_capacity_fail_closed(self):
        descriptor=await self.descriptor()
        self.assertEqual(descriptor["model"],"saved-deep")
        self.assertNotIn("backend",descriptor)
        self.assertNotIn("key",str(descriptor))
        original=descriptor["modelIdentity"]
        with patch.dict(os.environ,{"ASTOCK_DEEP_THINK_LLM":"another-global"}):
            self.assertEqual((await self.descriptor())["modelIdentity"],original)
        for changes in [{"backend_url":None},{"backend_url":""},{"llm_provider":"unsupported"},{"deep_think_llm":None}]:
            self.job=copy.deepcopy(JOB);self.job["config"].update(changes)
            response=await self.client.get("/research/jobs/job1/account-advice-model",params=self.query)
            self.assertEqual(response.json()["reasonCode"],"model_unavailable")
        self.job=copy.deepcopy(JOB)
        for raw in ["[]",json.dumps([LIMIT,LIMIT]),json.dumps([{**LIMIT,"backend_url":None}]),json.dumps([{**LIMIT,"model":"other"}]),json.dumps([{**LIMIT,"context_window":4000}])]:
            with patch.dict(os.environ,{"ASTOCK_ACCOUNT_MODEL_LIMITS_JSON":raw}):
                response=await self.client.get("/research/jobs/job1/account-advice-model",params=self.query)
                self.assertEqual(response.json()["reasonCode"],"model_unavailable")

    async def test_auth_precedes_body_parse_and_validation_never_reflects_payload(self):
        for token,reason in [("wrong","model_auth_error"),("","model_auth_error")]:
            response=await self.client.post("/research/jobs/job1/account-advice",content="private-value"*1000,headers={"Authorization":"Bearer "+token})
            self.assertEqual(response.json()["reasonCode"],reason)
            self.assertNotIn("private-value",response.text)
        with patch.dict(os.environ,{"ASTOCK_ACCOUNT_ADVICE_TOKEN":""}):
            response=await self.client.post("/research/jobs/job1/account-advice",content="private")
            self.assertEqual(response.status_code,503)
        body=await self.invocation();body["prompt"]="private-override"
        response=await self.client.post("/research/jobs/job1/account-advice",json=body)
        self.assertEqual(response.status_code,422)
        self.assertNotIn("private-override",response.text)
        raw=json.dumps(await self.invocation(),ensure_ascii=False)
        for invalid in [raw.replace('"schemaVersion": 1','"schemaVersion": 1, "schemaVersion": 1',1),raw.replace('"quantity": "1"','"quantity": "1", "quantity": "2"'),raw.replace('"float": 1e-07','"float": NaN')]:
            response=await self.client.post("/research/jobs/job1/account-advice",content=invalid)
            self.assertEqual(response.status_code,422)

    async def test_identity_drift_input_capacity_bodylimit_and_busy_have_zero_paid_calls(self):
        body=await self.invocation()
        async def unexpected(*_args,**_kwargs):
            raise AssertionError("must not call")
        with patch.object(advice,"native_model",side_effect=unexpected) as factory:
            for altered in [{"modelIdentity":"b"*64},{"reportId":"other"},{"ticker":"000001"}]:
                response=await self.client.post("/research/jobs/job1/account-advice",json={**body,**altered})
                self.assertEqual(response.status_code,409)
            changed=copy.deepcopy(body);changed["payload"]["researchContext"]["identity"]["jobId"]="other"
            self.assertEqual((await self.client.post("/research/jobs/job1/account-advice",json=changed)).status_code,409)
            with patch.dict(os.environ,{"ASTOCK_ACCOUNT_MODEL_LIMITS_JSON":json.dumps([{**LIMIT,"context_window":5000}])}):
                changed=await self.invocation()
                self.assertEqual((await self.client.post("/research/jobs/job1/account-advice",json=changed)).status_code,413)
            self.assertEqual((await self.client.post("/research/jobs/job1/account-advice",content=b"x"*(advice.MAX_BODY+1))).status_code,413)
            await advice._semaphore.acquire()
            try:self.assertEqual((await self.client.post("/research/jobs/job1/account-advice",json=body)).status_code,429)
            finally:advice._semaphore.release()
            factory.assert_not_called()

    async def test_exact_raw_payload_unicode_float_and_success_keep_source_unchanged(self):
        body=await self.invocation();before=copy.deepcopy(self.job);seen=[]
        class Model:
            async def ainvoke(self,messages,config=None):
                seen.append(messages)
                return AIMessage(content=[{"type":"text","text":"完整账户论证"}],response_metadata={"finish_reason":"stop"})
        raw=json.dumps(body,ensure_ascii=False,separators=(",",":")).replace('"float":1e-07','"float":1e-7')
        expected=raw[raw.index('"payload":')+10:-1]
        with patch.object(advice,"native_model",return_value=Model()):
            response=await self.client.post("/research/jobs/job1/account-advice",content=raw)
        self.assertEqual(response.status_code,200)
        self.assertEqual(seen[0][0].content,advice.SYSTEM_PROMPT)
        self.assertEqual(seen[0][1].content,expected)
        self.assertIn('"float":1e-7',seen[0][1].content)
        self.assertIn(PAYLOAD["researchContext"]["analystReports"][0]["content"],seen[0][1].content)
        self.assertEqual(response.json()["model"],"saved-deep")
        self.assertEqual(self.job,before)

    async def test_actual_native_sdk_routes_key_model_endpoint_no_retries_and_no_trace_cache(self):
        from tradingagents.llm_clients import create_llm_client
        from langchain_core.tracers.context import collect_runs
        captured=[]
        async def transport(request):
            captured.append(request)
            if len(captured)==1:
                return httpx.Response(500,json={"error":{"message":"private-url-key-account"}})
            return httpx.Response(200,json={"id":"synthetic","object":"chat.completion","created":1,"model":"saved-deep","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"完整账户论证"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}})
        async_client=httpx.AsyncClient(transport=httpx.MockTransport(transport))
        self.addAsyncCleanup(async_client.aclose)
        def native(provider,model,base_url,**kwargs):
            self.assertEqual((provider,model,base_url),("deepseek","saved-deep","https://saved.invalid/v1"))
            self.assertEqual(kwargs["max_retries"],0);self.assertEqual(kwargs["app_retries"],0);self.assertEqual(kwargs["timeout"],600)
            return create_llm_client(provider,model,base_url,http_async_client=async_client,**kwargs)
        body=await self.invocation()
        from langchain_core.caches import InMemoryCache
        cache=InMemoryCache()
        with patch("tradingagents.llm_clients.create_llm_client",side_effect=native),patch.dict(os.environ,{"LANGCHAIN_TRACING_V2":"true","LANGSMITH_TRACING":"true","LANGSMITH_API_KEY":"synthetic-tracing"}),patch("langchain_core.language_models.chat_models.get_llm_cache",return_value=cache),collect_runs() as collector:
            response=await self.client.post("/research/jobs/job1/account-advice",json=body)
            self.assertEqual(response.status_code,502);self.assertEqual(response.json()["reasonCode"],"model_service_error");self.assertEqual(len(captured),1)
            response=await self.client.post("/research/jobs/job1/account-advice",json=body)
            self.assertEqual(response.status_code,200);self.assertEqual(len(captured),2)
            self.assertEqual(collector.traced_runs,[])
            self.assertEqual(cache._cache,{})
        request=captured[-1]
        self.assertEqual(str(request.url),"https://saved.invalid/v1/chat/completions")
        self.assertEqual(request.headers["authorization"],"Bearer synthetic-key")
        self.assertEqual(json.loads(request.content)["model"],"saved-deep")
        self.assertEqual(json.loads(request.content).get("max_completion_tokens",json.loads(request.content).get("max_tokens")),4000)
        self.assertEqual(request.extensions["timeout"]["read"],600)

    async def test_actual_anthropic_and_openai_responses_use_saved_endpoint_and_terminal(self):
        from tradingagents.llm_clients import create_llm_client
        fixtures = {
            "anthropic": {"id":"msg_fixture","type":"message","role":"assistant","model":"saved-deep","content":[{"type":"text","text":"完整账户论证"}],"stop_reason":"end_turn","stop_sequence":None,"usage":{"input_tokens":1,"output_tokens":1}},
            "openai": {"id":"resp_fixture","object":"response","created_at":1,"model":"saved-deep","status":"completed","output":[{"id":"msg_fixture","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"完整账户论证","annotations":[]}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}},
        }
        for provider,response_body in fixtures.items():
            self.job=copy.deepcopy(JOB);self.job["config"]["llm_provider"]=provider
            with patch.dict(os.environ,{"ASTOCK_ACCOUNT_MODEL_LIMITS_JSON":json.dumps([{**LIMIT,"provider":provider}]),"ANTHROPIC_API_KEY":"synthetic-anthropic","OPENAI_API_KEY":"synthetic-openai","ANTHROPIC_BASE_URL":"https://wrong-current.invalid","OPENAI_BASE_URL":"https://wrong-current.invalid"}):
                body=await self.invocation();captured=[]
                protocol_httpx=httpx
                if provider == "anthropic":
                    import anthropic
                    import importlib
                    # SDK1.x uses httpx2; older installed SDKs use httpx. Match its actual client base.
                    client_base=next(base for base in anthropic.DefaultAsyncHttpxClient.__mro__
                                     if base.__name__ == "AsyncClient" and base.__module__.split(".")[0] in {"httpx", "httpx2"})
                    protocol_httpx=importlib.import_module(client_base.__module__.split(".")[0])
                async def transport(request):
                    captured.append(request)
                    return protocol_httpx.Response(200,json=response_body)
                async_client=protocol_httpx.AsyncClient(transport=protocol_httpx.MockTransport(transport))
                def native(selected,model,base_url,**kwargs):
                    return create_llm_client(selected,model,base_url,**({"http_async_client":async_client} if selected != "anthropic" else {}),**kwargs)
                try:
                    with ExitStack() as stack:
                        stack.enter_context(patch("tradingagents.llm_clients.create_llm_client",side_effect=native))
                        if provider == "anthropic":
                            stack.enter_context(patch("langchain_anthropic.chat_models._get_default_async_httpx_client",return_value=async_client))
                        response=await self.client.post("/research/jobs/job1/account-advice",json=body)
                    self.assertEqual(response.status_code,200)
                    self.assertEqual(response.json()["resultMarkdown"],"完整账户论证")
                    self.assertEqual(len(captured),1)
                    self.assertTrue(str(captured[0].url).startswith("https://saved.invalid/v1/"))
                    if provider == "anthropic":self.assertEqual(captured[0].headers["x-api-key"],"synthetic-anthropic")
                    else:self.assertEqual(captured[0].headers["authorization"],"Bearer synthetic-openai")
                    wire=json.loads(captured[0].content)
                    self.assertEqual(wire["model"],"saved-deep")
                    self.assertEqual(wire.get("max_output_tokens",wire.get("max_tokens")),4000)
                finally:
                    await async_client.aclose()

    async def test_actual_native_sdk_timeout_and_cancellation_close_async_request(self):
        from tradingagents.llm_clients import create_llm_client
        started=asyncio.Event();cancelled=asyncio.Event();calls=[]
        async def transport(request):
            calls.append(request);started.set()
            try:await asyncio.Event().wait()
            finally:cancelled.set()
        async_client=httpx.AsyncClient(transport=httpx.MockTransport(transport));self.addAsyncCleanup(async_client.aclose)
        def native(provider,model,base_url,**kwargs):
            return create_llm_client(provider,model,base_url,http_async_client=async_client,**kwargs)
        body=await self.invocation();body["timeoutMs"]=20
        with patch("tradingagents.llm_clients.create_llm_client",side_effect=native):
            response=await self.client.post("/research/jobs/job1/account-advice",json=body)
        self.assertEqual(response.json()["reasonCode"],"model_timeout");self.assertTrue(cancelled.is_set());self.assertEqual(len(calls),1)
        started.clear();cancelled.clear();body["timeoutMs"]=600000
        with patch("tradingagents.llm_clients.create_llm_client",side_effect=native):
            task=asyncio.create_task(self.client.post("/research/jobs/job1/account-advice",json=body))
            await asyncio.wait_for(started.wait(),1);task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
        self.assertTrue(cancelled.is_set());self.assertEqual(len(calls),2)
        self.assertFalse(advice._semaphore.locked())
        started.clear();cancelled.clear();disconnected=asyncio.Event()
        class Request:
            async def is_disconnected(self):return disconnected.is_set()
        descriptor=await self.descriptor()
        with patch("tradingagents.llm_clients.create_llm_client",side_effect=native):
            model=advice.native_model(descriptor,"https://saved.invalid/v1",600000)
            task=asyncio.create_task(advice.invoke_model(Request(),model,"{}",600000))
            await asyncio.wait_for(started.wait(),1);disconnected.set()
            with self.assertRaises(advice.AdviceFailure) as failure:
                await asyncio.wait_for(task,1)
        self.assertEqual(failure.exception.code,"model_cancelled")
        self.assertTrue(cancelled.is_set());self.assertEqual(len(calls),3)

class OutputTests(unittest.TestCase):
    def test_missing_adapter_or_async_guarantee_fails_closed(self):
        descriptor={"provider":"deepseek","model":"saved-deep","outputReserve":4000}
        with patch("tradingagents.llm_clients.create_llm_client",side_effect=ImportError("optional dependency")):
            with self.assertRaises(advice.AdviceFailure) as failure:advice.native_model(descriptor,"https://saved.invalid",600000)
            self.assertEqual(failure.exception.code,"model_unavailable")
        from types import SimpleNamespace
        with patch("tradingagents.llm_clients.create_llm_client",return_value=SimpleNamespace(get_llm=lambda:SimpleNamespace(max_retries=0,ainvoke=lambda:None))):
            with self.assertRaises(advice.AdviceFailure):advice.native_model(descriptor,"https://saved.invalid",600000)

    def test_sdk_error_types_status_and_codes_are_sanitized(self):
        import openai
        for status,code,reason in [(401,None,"model_auth_error"),(403,None,"model_auth_error"),(408,None,"model_timeout"),(504,None,"model_timeout"),(429,None,"model_rate_limited"),(503,None,"model_service_error"),(400,"context_length_exceeded","context_capacity_exceeded"),(422,None,"model_request_rejected")]:
            response=httpx.Response(status,request=httpx.Request("POST","https://private-url.invalid"))
            error=openai.APIStatusError("private-account-key",response=response,body={"code":code})
            classified=advice.classify(error)
            self.assertEqual(classified.code,reason)
            self.assertNotIn("private",advice.failure(classified).body.decode())
        self.assertEqual(advice.classify(openai.APITimeoutError(request=httpx.Request("POST","https://private.invalid"))).code,"model_timeout")
        self.assertEqual(advice.classify(openai.APIConnectionError(request=httpx.Request("POST","https://private.invalid"))).code,"model_connection_error")
        self.assertEqual(advice.classify(ValueError("timeout 401 private")).code,"model_result_unknown")

    def test_native_terminal_shapes_and_invalid_outputs(self):
        for metadata in [{"finish_reason":"stop"},{"stop_reason":"end_turn"},{"status":"completed"},{"finish_reason":"STOP"}]:
            self.assertEqual(advice.result_text(AIMessage(content="完整",response_metadata=metadata)),"完整")
        for metadata,reason in [({"finish_reason":"length"},"model_output_incomplete"),({"stop_reason":"max_tokens"},"model_output_incomplete"),({"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}},"model_output_incomplete"),({"finish_reason":"SAFETY"},"model_output_filtered"),({},"model_output_invalid")]:
            with self.assertRaises(advice.AdviceFailure) as failure:advice.result_text(AIMessage(content="partial",response_metadata=metadata))
            self.assertEqual(failure.exception.code,reason)
        for content in ["", " ", "x"*(advice.MAX_MARKDOWN+1), [{"type":"tool_use","id":"tool"}]]:
            with self.assertRaises(advice.AdviceFailure):advice.result_text(AIMessage(content=content,response_metadata={"finish_reason":"stop"}))
        with self.assertRaises(advice.AdviceFailure):advice.result_text(AIMessage(content="x",response_metadata={"finish_reason":"stop"},tool_calls=[{"name":"tool","args":{},"id":"x"}]))

if __name__ == "__main__":unittest.main()
