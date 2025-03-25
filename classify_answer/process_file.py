"""
process_file - 

Author: cavit
Date: 2025/2/25
"""
import json

fp = open("../data/深远海初赛b榜题目.jsonl", "r",encoding="utf-8")
# result = open("result_energy_and_content.jsonl","r",encoding="utf-8")
result = json.load(open("result_plan_b_v6.json",encoding="utf-8"))
result_dict = {}
for item in result:
    # item = json.loads(item)
    result_dict[item["id"]] = item["answer"]

fpn = open("../submit/jojo这个是我的逃跑路线_result.jsonl","w",encoding="utf-8")
for row in fp:
    t = json.loads(row)
    if t["id"] in result_dict:
        t["answer"] = result_dict[t["id"]]
    fpn.write(json.dumps(t,ensure_ascii=False)+"\n")