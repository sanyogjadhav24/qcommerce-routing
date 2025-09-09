from typing import Any, Dict, List


class ORToolsVRPSolver:
    """
    Minimal stub implementation to avoid import/runtime errors.
    Extend with real OR-Tools vehicle routing logic when ready.
    """

    def __init__(self) -> None:
        self._configured: bool = True

    def solve(self, orders: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "status": "ok",
            "num_orders": len(orders),
            "routes": [],
        }

