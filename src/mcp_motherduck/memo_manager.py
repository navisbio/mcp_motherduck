import logging

logger = logging.getLogger('mcp_motherduck_server.memo_manager')

class MemoManager:
    def __init__(self):
        self.insights: list[str] = []
        logger.info("MemoManager initialized")

    def add_insights(self, finding: str) -> None:
        """Add a new analysis finding to the in-memory collection"""
        if not finding:
            logger.error("Attempted to add empty finding")
            raise ValueError("Empty finding")
        
        self.insights.append(finding)
        logger.debug(f"Added new finding. Total findings: {len(self.insights)}")

    def get_insights_memo(self) -> str:
        """Generate a formatted memo from collected analysis findings"""
        logger.debug(f"Generating analysis memo with {len(self.insights)} findings")
        if not self.insights:
            logger.info("No analysis findings available")
            return "No analysis findings available yet."

        findings = "\n".join(f"- {finding}" for finding in self.insights)
        logger.debug("Generated analysis memo")
        
        memo = "🔍 Data Analysis Results\n\n"
        memo += "Key Findings & Insights:\n\n"
        memo += findings

        if len(self.insights) > 1:
            memo += "\n\nSummary:\n"
            memo += f"Analysis has identified {len(self.insights)} key insights."

        return memo

