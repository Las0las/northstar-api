"""
Slate Engine — top-N selection ranked by expected_profit.
Enforces score threshold + max slate size from policy.
"""
from backend.models.decision import Decision
from backend.models.policy import Policy
from typing import List


class SlateEngine:
    def __init__(self, policy: Policy):
        self.policy = policy
        self.min_score = policy.economic.min_score_threshold
        self.max_slate = policy.economic.max_slate_size

    def generate_slate(self, decisions: List[Decision]) -> List[Decision]:
        """
        Filter by score threshold, rank by expected_profit DESC,
        select top-N, assign ranks.
        """
        qualified = [
            d for d in decisions if d.score >= self.min_score
        ]
        ranked = sorted(
            qualified,
            key=lambda d: d.expected_profit,
            reverse=True
        )
        slate = ranked[:self.max_slate]
        for i, d in enumerate(slate):
            d.rank = i + 1
            d.selected = True
        # Mark non-selected
        selected_ids = {d.decision_id for d in slate}
        for d in decisions:
            if d.decision_id not in selected_ids:
                d.selected = False
                d.rank = 0
        return slate

    def generate_economic_slate(self, decisions: List[Decision]) -> List[Decision]:
        """Same as generate_slate but purely economic ranking."""
        return self.generate_slate(decisions)
