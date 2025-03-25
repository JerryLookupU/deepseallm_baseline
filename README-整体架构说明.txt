├── classify_answer/
│   ├── __init__.py
│   ├── base_llm.py  基础类模块
│   ├── class_source.py
│   ├── classify_function_call.py   函数调用模块
│   ├── classify_solver.py 主要函数入口
│   ├── config.py
│   ├── llm_text2sql.py  text2sql部分
│   ├── meta_solver.py   原子问题解决模块
│   ├── process_file.py
│   ├── question_classify.py 问题分类模块
│   ├── question_condition_sep.py
│   ├── question_rewrite.py     问题重写模块
│   ├── question_sub_chain.py
│   ├── question_sub_chain_v2.py
│   ├── question_sub_chain_v3.py   问题思维链拆解模块
│   ├── RAG_solver.py
│   ├── retriver_sql_generate.py   检索器
│   └── table_answer.py  问题回答部分
└── data_process/
    ├── __init__.py
    ├── deepseaetl.py
    ├── deepseaetl_v1.py
    ├── deepseaetl_v2.py  数据标定脚本
    └── load_data.py

上述是整体代码结构
其中
device_info_array.xlsx 是设备基础信息
few_shot.xlsx 是sql生成样例 用于快速sql生成：
    待补充sql样例包括：  发动机开启sql 伸缩推开启关闭次数sql (可以参考A架开机次数sql) ... 增加一个类型的sql 以免过拟合
    sql结构建议采用 分层结构 如何分层 根据案例中的例子可以获取（分层sql能提高sql生成成功率）
    sql分层及分析analysis 可以通过deepseek 进行分析获取

待优化部分（问题分类，主要区分出能耗计算部分，可以在sql案例中补充 sql计算能耗案例，就可以完全统一sql实现）
  任务拆解部分： 采用 deepseek的 think部分进行RAG召回样例进行思维链，也需要不少工作量
