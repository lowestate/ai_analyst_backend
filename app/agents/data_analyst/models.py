from app.agents.client import llm
from app.agents.data_analyst.tools import (
    analyze_columns, correlation_matrix, detect_outliers,
    analyze_trends, analyze_dependency, pairplot_tool,
    feature_importances_tool, feature_tree_tool
)

class DataAnalystModel:
    def __init__(self):
        self.llm = llm()
        self.tools = [
            analyze_columns, correlation_matrix, detect_outliers,
            analyze_trends, analyze_dependency, pairplot_tool,
            feature_importances_tool, feature_tree_tool
        ]
        self.tools_by_name = {t.name: t for t in self.tools}
        # Биндим тулы к модели
        self.llm_with_tools = self.llm.bind_tools(self.tools)

# Создаем инстанс ЗДЕСЬ, и импортируем его потом в data_analyst/nodes.py
da_agent_instance = DataAnalystModel()