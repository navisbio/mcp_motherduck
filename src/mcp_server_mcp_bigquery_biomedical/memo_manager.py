# import logging

# logger = logging.getLogger('mcp_motherduck_server.memo_manager')

# class MemoManager:
#     def __init__(self):
#         self.findings: list[str] = []
#         logger.info("MemoManager initialized")

#     def add_finding(self, finding: str) -> None:
#         """Add a new analysis finding to the in-memory collection"""
#         if not finding:
#             logger.error("Attempted to add empty analysis finding")
#             raise ValueError("Empty analysis finding")
        
#         self.findings.append(finding)
#         logger.debug(f"Added new analysis finding. Total findings: {len(self.findings)}")

#     def get_analysis_memo(self) -> str:
#         """Generate a formatted memo from collected analysis findings"""
#         logger.debug(f"Generating analysis memo with {len(self.findings)} findings")
#         if not self.findings:
#             logger.info("No analysis findings available")
#             return "No analysis memo available yet."

#         findings = "\n".join(f"- {finding}" for finding in self.findings)
#         logger.debug("Generated analysis memo")
        
#         memo = "🔍 Analysis Memo\n\n"
#         memo += "Key Findings and Insights:\n\n"
#         memo += findings

#         if len(self.findings) > 1:
#             memo += "\n\nSummary:\n"
#             memo += f"The analysis has identified {len(self.findings)} key findings."

#         return memo