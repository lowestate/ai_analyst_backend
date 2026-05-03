from app.agents.client import llm
from app.agents.finance_agent.tools import (
    calculate_cash_flow_tool,
    calculate_pnl_tool,
    analyze_expense_structure_tool
)

class FinanceAgentModel:
    def __init__(self):
        self.llm = llm()
        self.tools = [
            calculate_cash_flow_tool,
            calculate_pnl_tool,
            analyze_expense_structure_tool
        ]
        self.tools_by_name = {t.name: t for t in self.tools}
        self.llm_with_tools = self.llm.bind_tools(self.tools)

# Инициализируем инстанс агента в его собственном пространстве имен
finance_agent_instance = FinanceAgentModel()