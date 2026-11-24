import asyncio
import json

from raftkv.lab import Lab


def test_quorum_loss_experiment_with_real_node_processes(tmp_path):
    async def scenario():
        lab = Lab(tmp_path)
        try:
            await lab.reset(3)
            async with asyncio.timeout(15):
                while True:
                    state = (await lab.snapshot())["clusters"]
                    if all(
                        all("term" in node for node in cluster["nodes"])
                        and any(node.get("ready") for node in cluster["nodes"])
                        for cluster in state
                    ):
                        break
                    await asyncio.sleep(0.1)
            result = await lab.action("compare", {})
            assert result["passed"], json.dumps(result, indent=2)
            assert result["before"][1]["data"] == "committed before quorum loss"
            state = (await lab.snapshot())["clusters"]
            assert all(sum(node["role"] == "stopped" for node in cluster["nodes"]) == 2 for cluster in state)
            assert all(node["role"] != "leader" for node in state[1]["nodes"])
        finally:
            await lab.close()

    asyncio.run(scenario())
