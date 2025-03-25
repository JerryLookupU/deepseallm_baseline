"""
retriver_sql_generate - 

Author: cavit
Date: 2025/2/25
"""
import json
import jieba
import pandas as pd
from langchain_core.documents import Document
from langchain_community.retrievers import BM25Retriever
from llm_text2sql import llm_struct
from loguru import logger


def data_builder_json(path):
    df = pd.read_excel(path)
    df = df.fillna("")
    ret = []
    for idx,item in df.iterrows():
        q = item["问题"]
        q_st = llm_struct(q)
        solver = item["sql案例"]
        analysis = item["解析"]
        data = {"question":q,"question_struct":q_st,"solver":solver,"analysis":analysis}
        ret.append(data)
    json.dump(ret, open("question_solver_example.json", "w", encoding="utf-8"), indent=4, ensure_ascii=False)


solver_data = json.load(open("question_solver_example.json",encoding="utf-8"))

solver_data1 = [item for item in solver_data if item["solver"] != "使用函数生成不适用sql计算"]
def bm25pre_func(x):
    r = jieba.cut(x)
    return list(r)


def data_to_document(data):
    result = []
    for idx,item in enumerate(data):
        q = item["question"]
        q_st = item["question_struct"].strip()
        ctx = f"""
        用户问题：{q}
        用户问题结构：{q_st}
        """.strip()
        doc = Document(page_content=ctx,metadata={"id":idx,"问题":q})
        result.append(doc)
    return result

def data_to_document1(data):
    result = []
    for idx,item in enumerate(data):
        q = item["question"]
        q_st = item["question_struct"].strip()
        ctx = f"""
        用户问题：{q}
        用户问题结构：{q_st}
        """.strip()
        doc = Document(page_content=ctx,metadata={"id":idx,"问题":q})
        result.append(doc)
    return result

document_data = data_to_document(solver_data)
document_data1 = data_to_document1(solver_data1)

bm25_retriever = BM25Retriever.from_documents(documents=document_data1,preprocess_func=bm25pre_func)
bm25_retriever.k = 2

name_retriever = BM25Retriever.from_documents(documents=document_data,preprocess_func=bm25pre_func)
name_retriever.k = 5

def solver_to_content(data_chunk):
    ret = []
    for item in data_chunk:
        q = item["question"]
        analysis = item["analysis"]
        if type(analysis) == str:
            analysis = analysis.strip()
        else:
            analysis = "无"
        solver_sql = item["solver"]
        ctx = f"""
        案例：
        问题：{q}
        解析：{analysis}
        以下为实例SQL：
        {solver_sql}
        """
        ret.append(ctx)
    return "以下为参考资料：\n" + "\n".join(ret)


def retriver_llm(question,sd=solver_data1,retriver=bm25_retriever):
    result = []
    question_struct = llm_struct(question)
    logger.info(f"question_struct:{question_struct}")
    question_struct = question_struct.strip().strip("`")
    ctx = f"""
    用户问题：{question}
    用户问题结构：{question_struct}
    """.strip()
    match_result = retriver.get_relevant_documents(query=ctx)
    array_idx = [item.metadata["id"] for item in match_result]
    for i in array_idx:
        result.append(sd[i])
    return solver_to_content(result)


if __name__ == "__main__":
    data_builder_json("few_shot.xlsx")
    # fp = open("../data/question_plan_b.jsonl", encoding="utf-8")
    # plan_b = []
    # for row in fp:
    #     line = json.loads(row)
    #     question = line["question"]
    #     doc = bm25_retriever.get_relevant_documents(query=question)
    #     print("========")
    #     print(question)
    #     print(doc)
    #     print("========\n")