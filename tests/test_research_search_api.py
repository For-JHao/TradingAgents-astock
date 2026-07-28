import unittest
from unittest.mock import patch

import httpx

from research_api.app import _search_tickers_via_eastmoney


class EastmoneyTickerSearchTest(unittest.TestCase):
    @patch("research_api.app.httpx.get")
    def test_parses_eastmoney_quotation_candidates(self, mock_get) -> None:
        mock_get.return_value = httpx.Response(
            200,
            request=httpx.Request(
                "GET", "https://searchapi.eastmoney.com/api/suggest/get"
            ),
            json={
                "QuotationCodeTable": {
                    "Data": [
                        {
                            "Code": "601689",
                            "Name": "拓普集团",
                            "Classify": "AStock",
                            "SecurityType": "1",
                            "SecurityTypeName": "沪A",
                            "QuoteID": "1.601689",
                        }
                    ]
                }
            },
        )

        candidates = _search_tickers_via_eastmoney("拓普集团")

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].code, "601689")
        self.assertEqual(candidates[0].name, "拓普集团")
        self.assertEqual(candidates[0].quote_id, "1.601689")
        mock_get.assert_called_once_with(
            "https://searchapi.eastmoney.com/api/suggest/get",
            params={"input": "拓普集团", "type": "14"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8,
        )

    @patch("research_api.app.httpx.get")
    def test_transport_or_payload_failure_is_a_safe_empty_result(self, mock_get) -> None:
        mock_get.side_effect = httpx.TransportError("network failure")

        self.assertEqual(_search_tickers_via_eastmoney("拓普集团"), [])


if __name__ == "__main__":
    unittest.main()
