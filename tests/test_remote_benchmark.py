import asyncio

from raftkv import remote_benchmark as benchmark


def test_minute_histograms_match_success_outcomes(monkeypatch):
    async def fake_request(stub, command):
        return "ok", benchmark.VALUE if command[0] == "GET" else ""

    monkeypatch.setattr(benchmark, "request", fake_request)
    monkeypatch.setattr(benchmark.time, "time", lambda: 59.5)
    result = asyncio.run(benchmark.drive(None, "key", rate=20, seconds=1, write_percent=50,
                                         in_flight=20, minute_histograms=True))

    assert result["start_utc"] == "1970-01-01T00:00:59.500Z"
    assert result["scheduled_end_utc"] == "1970-01-01T00:01:00.500Z"
    assert [item["start_utc"] for item in result["minute_histograms"]] == [
        "1970-01-01T00:00:00.000Z", "1970-01-01T00:01:00.000Z",
    ]
    for minute in result["minute_histograms"]:
        for operation in minute["operations"].values():
            assert operation["outcomes"] == {"ok": 5}
            assert sum(operation["success_latency_counts"]) == 5
