"""
classify_solver - 

Author: cavit
Date: 2025/2/21
"""
import json
from classify_answer.question_sub_chain import energy_sub_chain,device_sub_chain
from classify_answer.question_sub_chain_v3 import action_sub_chain
from classify_answer.question_condition_sep import question_struct_step
from classify_answer.question_rewrite import question_rewrite
from loguru import logger
from question_rewrite import rewrite_subquestion
from meta_solver import meta_action_solver,meta_energy_solver,meta_device_solver,meta_device_info_solver,meta_action_fast_solver,meta_sql_solver,fast_answer
from classify_function_call import  agg_function_call
from table_answer import middle_answer
from table_answer import final_answer
from question_classify import question_classify
from base_llm import  conn
from tqdm import tqdm
from base_llm import flash_zhipuai
# "动作数据查询": """根据动作查询时间点，深海作业A的动作时刻查询问题，需要要明确动作或者明确设备，例如：xxxx/xx/xx A架第一次开机时间在什么时候？ xxxx/xx/xx 缆绳解除的时间点是什么时候？,动作包括："A架开机", "A架摆出", "征服者起吊", "征服者入水", "征服者出水", "A架关机", "A架摆回",
# "折臂吊车开机", "折臂吊车关机", "小艇入水", "缆绳解除", "小艇落座", "缆绳挂妥","ON DP","OFF DP" """,
# "设备数据查询": "具体设备在某个时刻或者个时间段内的数值情况，设备包括，推进器，侧推（艏推或者艏侧推），绞车，A架（一号二号门架），折臂吊车，发电机，舵桨 问题中一定会包含具体时间，主要是各种设备各个时刻数据指标状态，指标包括压力，电流电压，频率Hz，功率，相位，燃油。例如：在XXXX年XX月XX日XX时XX分，X号柴油发电机组燃油消耗率是多少？",
# "时长处理问题": "动作相隔时间判断问题，包括各类时长比较计算，例如：xxxx/xx/xx A架开机和关机时间间隔多久？, xxxx/xx/xx A架运行时长多长,xx/xx~xx/xx 平均时长, 包括深海作业A各类时长处理问题",
# "盘点动作": "什么设备执行了什么动作的问题。未知设备和未知动作，有具体日期或者时间时间段 例如：xxxx/xx/xx 什么设备进行了什么动作",
# "能耗问题": "计算各类设备的能耗数值问题, 例如：xxxx/xx/xx A架设备总能耗是多少？几月几日xx过程中xx设备能耗，xx日~xx日设备平均能耗等",
# "油耗问题": "计算发电机的油耗，例如：xxxx/xx/xx 上午 发电机油耗是多少L?",
# "资料查询问题": "查询设备的属性参数，查询设备的属性资料，包括给定设备指标查询数据表字段。例如：柴油发电机组滑油压力的范围是多少？一号柴油发电机组有功功率测量的范围是多少kW到多少kW？ 控制xxxx的字段名称是",
# "特殊条件时长问题": "A架，绞车，折臂吊车都存在待机情况，角度摆动存在持续存在，计算这些特殊情况的时常问题。例如：xxxx/xx/xx A架实际运行时长，xxxx/xx/xx 上午折臂吊车的待机时长是多少？ (注意实际运行时长和运行时长的区别)",
# "理论发电量计算": "计算发电机的理论发电量，包括发电效率，例如： xxxx/xx/xx ~ xxxx/xx/xx 时间 柴油发电机的理论发电量",
# "原始字段异常判断": "询问原始数据中的数据是否存在异常，例如： xxxx/xx/xx 上午 A架摆动数据是否异常",
# "未知分类": "不属于上述分类"
def action_solver(q,conn,question_type="",debug=False):
    if not debug:
        act_sep = question_struct_step(q)
        question,question_condition = act_sep["question"],act_sep["condition"]
        log_text = f"""
            原始问题为：{q}
            问题条件拆解为： {question}, 条件： {question_condition}
        """
        logger.info(log_text)
    else:
        question = q
    rw_question = question_rewrite(question)
    logger.info(f"重写后的问题为：{rw_question}")
    action_chain = action_sub_chain(rw_question)
    logger.info(json.dumps(action_chain,ensure_ascii=False,indent=4))
    board_label = []
    for ichain in action_chain:
        sub_question = ichain["子问题"]
        is_dep =  ichain["是否前序依赖"]
        use_sql = ichain["数据需求"]
        logger.info("当前任务：" + str(ichain))
        if not is_dep:
            answer = meta_action_solver(sub_question,conn=conn)
            ichain["回答"] = answer
            logger.info("当前问题：" + sub_question  + "\n"+"当前回答：" + str(ichain))
            board_label.append(str(ichain))
        else:
            board_label_str = "\n".join(board_label)
            rewrite_sub_question = rewrite_subquestion(board_label_str,sub_question)
            logger.info("当前问题：" + str(sub_question))
            logger.info("当前重写的问题：" + str(rewrite_sub_question))
            if use_sql == "sql":
                answer = meta_action_solver(rewrite_sub_question,conn=conn)
                ichain["回答"] = answer
                logger.info("当前问题：" + rewrite_sub_question  + "\n"+"当前回答：" + str(ichain))
                board_label.append(str(ichain))
            else:
                function_result = agg_function_call(rewrite_sub_question,board_label)
                if function_result is None:
                    function_result = []
                answer = middle_answer(rewrite_sub_question,board_label + function_result,question_type)
                ichain["回答"] = answer
                logger.info("当前问题：" + rewrite_sub_question  + "\n"+"当前回答：" + str(ichain))
                board_label.append(str(ichain))
    logger.info("最终问题回答列表："+board_label_str)
    final_function_result = agg_function_call(q,board_label_str,final=True)
    if final_function_result is None:
        final_function_result = ["未进行聚合函数处理"]

    board_label_str = "\n".join(board_label)
    final_function_result_str = "\n".join([str(i) for i in final_function_result])
    answer = final_answer(q,board_label_str + final_function_result_str+"回答结果按照: 问题条件进行回答",final=True)
    return answer


def simple_action_solver(q,conn,question_type=""):
    return meta_action_fast_solver(q,conn,question_type=question_type)



def duration_solver(q,conn,question_type="",debug=False):
    if not debug:
        act_sep = question_struct_step(q)
        question,question_condition = act_sep["question"],act_sep["condition"]
        log_text = f"""
            原始问题为：{q}
            问题条件拆解为： {question}, 条件： {question_condition}
        """
        logger.info(log_text)
    else:
        question = q
    rw_question = question_rewrite(question)
    logger.info(f"重写后的问题为：{rw_question}")
    action_chain = action_sub_chain(rw_question)
    logger.info(json.dumps(action_chain,ensure_ascii=False,indent=4))
    board_label = []
    for ichain in action_chain:
        sub_question = ichain["子问题"]
        is_dep =  ichain["是否前序依赖"]
        use_sql = ichain["数据需求"]
        logger.info("当前任务：" + str(ichain))
        if not is_dep:
            answer = meta_action_solver(sub_question,conn=conn)
            ichain["回答"] = answer
            logger.info("当前问题：" + sub_question  + "\n"+"当前回答：" + str(ichain))
            board_label.append(str(ichain))
        else:
            board_label_str = "\n".join(board_label)
            rewrite_sub_question = rewrite_subquestion(board_label_str,sub_question)
            logger.info("当前问题：" + str(sub_question))
            logger.info("当前重写的问题：" + str(rewrite_sub_question))
            if use_sql == "sql":
                answer = meta_action_solver(rewrite_sub_question,conn=conn)
                ichain["回答"] = answer
                logger.info("当前问题：" + rewrite_sub_question  + "\n"+"当前回答：" + str(ichain))
                board_label.append(str(ichain))
            else:
                function_result = agg_function_call(rewrite_sub_question,board_label)
                if function_result is None:
                    function_result = []
                answer = middle_answer(rewrite_sub_question,board_label + function_result,question_type)
                ichain["回答"] = answer
                logger.info("当前问题：" + rewrite_sub_question  + "\n"+"当前回答：" + str(ichain))
                board_label.append(str(ichain))
    ##
    logger.info("最终问题回答列表："+"\n".join(board_label))
    final_function_result = agg_function_call(q,board_label,final=True)
    if final_function_result is None:
        final_function_result = ["未进行聚合函数处理"]

    board_label_str = "\n".join(board_label)
    final_function_result_str = "\n".join([str(i) for i in final_function_result])
    answer = final_answer(q,board_label_str + final_function_result_str + "回答结果按照: 问题条件进行回答",final=True)
    return answer


def cover_action_solver(q,conn,question_type="",debug=False):
    if not debug:
        act_sep = question_struct_step(q)
        question,question_condition = act_sep["question"],act_sep["condition"]
        log_text = f"""
            原始问题为：{q}
            问题条件拆解为： {question}, 条件： {question_condition}
        """
        logger.info(log_text)
    else:
        question = q
    rw_question = question_rewrite(question)
    logger.info(f"重写后的问题为：{rw_question}")
    action_chain = action_sub_chain(rw_question)
    logger.info(json.dumps(action_chain,ensure_ascii=False,indent=4))
    board_label = []
    for ichain in action_chain:
        sub_question = ichain["子问题"]
        is_dep =  ichain["是否前序依赖"]
        use_sql = ichain["数据需求"]
        logger.info("当前任务：" + str(ichain))
        if not is_dep:
            answer = meta_action_solver(sub_question,conn=conn)
            ichain["回答"] = answer
            logger.info("当前问题：" + sub_question  + "\n"+"当前回答：" + str(ichain))
            board_label.append(str(ichain))
        else:
            board_label_str = "\n".join(board_label)
            rewrite_sub_question = rewrite_subquestion(board_label_str,sub_question)
            logger.info("当前问题：" + str(sub_question))
            logger.info("当前重写的问题：" + str(rewrite_sub_question))
            if use_sql == "sql":
                answer = meta_action_solver(rewrite_sub_question,conn=conn)
                ichain["回答"] = answer
                logger.info("当前问题：" + rewrite_sub_question  + "\n"+"当前回答：" + str(ichain))
                board_label.append(str(ichain))
            else:
                function_result = agg_function_call(rewrite_sub_question,board_label)
                if function_result is None:
                    function_result = []
                answer = middle_answer(rewrite_sub_question,board_label + function_result,"")
                ichain["回答"] = answer
                logger.info("当前问题：" + rewrite_sub_question  + "\n"+"当前回答：" + str(ichain))
                board_label.append(str(ichain))
    ##
    logger.info("最终问题回答列表："+"\n".join(board_label))
    final_function_result = agg_function_call(q,board_label,final=True)
    if final_function_result is None:
        final_function_result = ["未进行聚合函数处理"]

    board_label_str = "\n".join(board_label)
    final_function_result_str = "\n".join([str(i) for i in final_function_result])
    answer = final_answer(q,board_label_str + final_function_result_str + "回答结果按照: 问题条件进行回答",final=True)
    return answer





def energy_solver(q,conn,question_type,debug=False):
    question = q
    # if not debug:
    #     act_sep = question_struct_step(q)
    #     question,question_condition = act_sep["question"],act_sep["condition"]
    #     log_text = f"""
    #         原始问题为：{q}
    #         问题条件拆解为： {question}, 条件： {question_condition}
    #     """
    #     logger.info(log_text)
    # else:
    #     question = q
    energy_chain = energy_sub_chain(question)
    logger.info(json.dumps(energy_chain, ensure_ascii=False, indent=4))
    local_board_label = []
    for ichain in energy_chain:
        sub_question = ichain["子问题"]
        is_dep =  ichain["是否前序依赖"]
        use_sql = ichain["数据需求"]
        logger.info("当前任务：" + str(ichain))
        if not is_dep:
            if use_sql == "sql":
                inner_answer = meta_action_solver(sub_question,conn=conn)
                ichain["回答"] = inner_answer
                logger.info("当前问题：" + sub_question  + "\n"+"当前回答：" + str(ichain))
                local_board_label.append(str(ichain))
            elif use_sql == "energy":
                inner_answer = meta_energy_solver(sub_question,conn=conn)
                ichain["回答"] = inner_answer
                logger.info("当前问题：" + sub_question  + "\n"+"当前回答：" + str(ichain))
                local_board_label.append(str(ichain))
            else:
                local_board_label.append(str(ichain))
        else:
            board_label_str = "\n".join(local_board_label)
            # logger.info("当前历史回答情况："+board_label_str)
            rewrite_sub_question = rewrite_subquestion(board_label_str,sub_question)
            logger.info("当前问题：" + str(sub_question))
            logger.info("当前重写的问题：" + str(rewrite_sub_question))
            if use_sql == "sql":
                inner_answer = meta_action_solver(rewrite_sub_question,conn=conn)
                ichain["回答"] = inner_answer
                logger.info("当前问题：" + rewrite_sub_question  + "\n"+"当前回答：" + str(ichain))
                local_board_label.append(str(ichain))
            elif use_sql == "energy":
                inner_answer = meta_energy_solver(rewrite_sub_question,conn=conn)
                ichain["回答"] = inner_answer
                logger.info("当前问题：" + rewrite_sub_question  + "\n"+"当前回答：" + str(ichain))
                local_board_label.append(str(ichain))
            else:
                function_result = agg_function_call(rewrite_sub_question,local_board_label)
                if function_result is None:
                    function_result = []
                inner_answer = middle_answer(rewrite_sub_question,local_board_label + function_result,question_type=question_type)
                ichain["回答"] = inner_answer
                logger.info("当前问题：" + rewrite_sub_question  + "\n"+"当前回答：" + str(ichain))
                local_board_label.append(str(ichain))
    ##
    logger.info("最终问题回答列表："+"\n".join(local_board_label))
    # final_function_result = agg_function_call(q,local_board_label,final=True)
    # if final_function_result is None:
    #     final_function_result = []
    board_label_str = "\n".join(local_board_label)
    # final_function_result_str = "\n".join([str(i) for i in final_function_result])
    result_answer = fast_answer(q, "当前问题拆解的流程和流程回答结果如下：\n" +board_label_str  + "回答结果按照: 问题条件进行回答", final=True)
    return result_answer


def device_solver(q,conn,question_type="",debug=False):
    if not debug:
        act_sep = question_struct_step(q)
        question,question_condition = act_sep["question"],act_sep["condition"]
        log_text = f"""
            原始问题为：{q}
            问题条件拆解为： {question}, 条件： {question_condition}
        """
        logger.info(log_text)
    else:
        question = q
    energy_chain = device_sub_chain(question)
    logger.info(json.dumps(energy_chain, ensure_ascii=False, indent=4))
    board_label = []
    for ichain in energy_chain:
        sub_question = ichain["子问题"]
        is_dep =  ichain["是否前序依赖"]
        use_sql = ichain["数据需求"]
        logger.info("当前任务：" + str(ichain))
        if not is_dep:
            if use_sql == "action":
                answer = meta_action_solver(sub_question,conn=conn)
                ichain["回答"] = answer
                logger.info("当前问题：" + sub_question  + "\n"+"当前回答：" + str(ichain))
                board_label.append(str(ichain))
            elif use_sql == "device":
                answer = meta_device_solver(sub_question,conn=conn)
                ichain["回答"] = answer
                logger.info("当前问题：" + sub_question  + "\n"+"当前回答：" + str(ichain))
                board_label.append(str(ichain))
            else:
                board_label.append(str(ichain))
        else:
            board_label_str = "\n".join(board_label)
            # logger.info("当前历史回答情况："+board_label_str)
            rewrite_sub_question = rewrite_subquestion(board_label_str,sub_question)
            logger.info("当前问题：" + str(sub_question))
            logger.info("当前重写的问题：" + str(rewrite_sub_question))
            if use_sql == "action":
                answer = meta_action_solver(rewrite_sub_question,conn=conn)
                ichain["回答"] = answer
                logger.info("当前问题：" + rewrite_sub_question  + "\n"+"当前回答：" + str(ichain))
                board_label.append(str(ichain))
            elif use_sql == "device":
                answer = meta_device_solver(rewrite_sub_question,conn=conn)
                ichain["回答"] = answer
                logger.info("当前问题：" + rewrite_sub_question  + "\n"+"当前回答：" + str(ichain))
                board_label.append(str(ichain))
            else:
                function_result = agg_function_call(rewrite_sub_question,board_label)
                if function_result is None:
                    function_result = []
                answer = middle_answer(rewrite_sub_question,board_label + function_result)
                ichain["回答"] = answer
                logger.info("当前问题：" + rewrite_sub_question  + "\n"+"当前回答：" + str(ichain))
                board_label.append(str(ichain))
    ##
    logger.info("最终问题回答列表："+"\n".join(board_label))
    final_function_result = agg_function_call(q,board_label,final=True)
    if final_function_result is None:
        final_function_result = []
    final_function_result_str = "\n".join([str(i) for i in final_function_result])
    board_label_str = "\n".join(board_label)
    answer = final_answer(q,board_label_str + final_function_result_str+ "回答结果按照: 问题条件进行回答",final=True)
    return answer


# def type_process(question_type):
#     if question_type == "":
#         return True
#     if question_type == "动作数据查询":
#         return meta_sql_solver(question,conn=conn,question_type=question_type)
#     if question_type == "设备数据查询":
#         return meta_sql_solver(question,conn=conn,question_type=question_type)
#     if question_type == "时长处理问题":
#         return meta_sql_solver(question,conn=conn,question_type=question_type)
#     if question_type == "盘点动作":
#         return meta_sql_solver(question,conn=conn,question_type=question_type)
#     if question_type == "能耗问题":
#         return energy_solver(question,conn=conn,question_type=question_type)
#     if question_type == "油耗问题":
#         return energy_solver(question,conn=conn,question_type=question_type)
#     if question_type == "资料查询问题":
#         return meta_device_info_solver(question,conn=conn)
#     if question_type == "理论发电量计算":
#         return energy_solver(question,conn=conn,question_type=question_type)
#     if question_type == "特殊条件时长问题":
#         return meta_sql_solver(question,conn=conn,question_type=question_type)
#     if question_type == "原始字段异常判断":
#         return meta_sql_solver(question,conn=conn,question_type=question_type)
#     if question_type == "未知分类":
#         return meta_sql_solver(question,conn=conn)


def is_energy(quesiton):
    if "能耗" in quesiton:
        return True

    if "油耗" in quesiton:
        return True

    if "发电量" in quesiton:
        return True

    if "功耗" in quesiton:
        return True
    return  False


def anything_sql_solver(question,conn):
    question_sub_list = action_sub_chain(question)
    logger.info("=======问题拆解=============\n")
    logger.info(question_sub_list)
    logger.info("=======问题拆解完毕==========\n")
    board_label = []
    answer_cache = {}
    if question_sub_list is None:
        # 直接问题分类 并解决
        question_type = question_classify(question)
        return run_solver(question,conn,question_type=question_type)

    for idx,ichain in enumerate(question_sub_list):
        sub_question = ichain["子问题"]
        logger.info("=============================")
        logger.info("当前问题：" + str(sub_question))
        logger.info("=============================")

        magic_box = board_label + [answer_cache] + [ichain]
        rewrite_sub_question = rewrite_subquestion(magic_box,sub_question)
        is_agg = ichain.get("是否仅汇总",False)
        if is_agg:
            function_result = agg_function_call(rewrite_sub_question, magic_box)
            if function_result is None:
                function_result = []
            answer = middle_answer(rewrite_sub_question, magic_box + function_result)
            ichain["子问题回答"] = answer
            logger.info("当前问题：" + rewrite_sub_question + "\n" + "当前回答：" + str(ichain))
            board_label.append(ichain)
        else:
            sub_dep = ichain.get("依赖问题","")
            if sub_dep != "":
                if answer_cache.get(sub_dep,"") != "":
                    sub_dep_answer = answer_cache[sub_dep]
                else:
                    sub_dep_type = question_classify(sub_dep)
                    # 都需要对sub_dep重写
                    text_info = board_label + [answer_cache]
                    rewrite_sub_dep = rewrite_subquestion(text_info, sub_dep)
                    if sub_dep_type == "资料查询问题":
                        sub_dep_answer = meta_device_info_solver(rewrite_sub_dep,conn=conn)
                        ichain["依赖问题答案"] = sub_dep_answer
                    elif sub_dep_type == "能耗问题":
                        sub_dep_answer = meta_energy_solver(rewrite_sub_dep,conn=conn)
                        ichain["依赖问题答案"] = sub_dep_answer
                    elif sub_dep_type == "油耗问题":
                        sub_dep_answer = meta_energy_solver(rewrite_sub_dep,conn=conn)
                        ichain["依赖问题答案"] = sub_dep_answer
                    elif sub_dep_type == "理论发电量计算":
                        sub_dep_answer = meta_energy_solver(rewrite_sub_dep,conn=conn)
                        ichain["依赖问题答案"] = sub_dep_answer
                    elif sub_dep_type == "未知分类":
                        # 重写依赖问题 ( 仅判断一次 ，可以判断多次 但是增加开销)
                        is_enr = is_energy(rewrite_sub_dep)
                        if is_enr:
                            sub_dep_answer = meta_energy_solver(rewrite_sub_dep, conn=conn)
                        else:
                            sub_dep_answer = meta_sql_solver(rewrite_sub_dep, conn=conn)
                        ichain["依赖问题答案"] = sub_dep_answer
                    else:
                        sub_dep_answer = meta_sql_solver(rewrite_sub_dep,conn=conn)
                        ichain["依赖问题答案"] = sub_dep_answer
                answer_cache[rewrite_sub_dep] = sub_dep_answer

            sub_question_type = question_classify(rewrite_sub_question)
            logger.info("=======子问题分类=============\n")
            logger.info(sub_question_type)
            logger.info("=======子问题分类完毕==========\n")
            if sub_question_type == "资料查询问题":
                sub_answer = meta_device_info_solver(rewrite_sub_question,conn=conn)
            elif sub_question_type == "能耗问题":
                sub_answer = meta_energy_solver(rewrite_sub_question,conn=conn)
            elif sub_question_type == "油耗问题":
                sub_answer = meta_energy_solver(rewrite_sub_question,conn=conn)
            elif sub_question_type == "理论发电量计算":
                sub_answer = meta_energy_solver(rewrite_sub_question,conn=conn)
            elif sub_question_type == "未知分类":
                rewrite_sub_question = rewrite_subquestion(magic_box,rewrite_sub_question + "\n 问题表述不清，请根据上下文及相关信息重写问题，需要保障必要信息和问题")
                is_enr = is_energy(rewrite_sub_question)
                if is_enr:
                    sub_answer = meta_energy_solver(rewrite_sub_question,conn=conn)
                else:
                    sub_answer = meta_sql_solver(rewrite_sub_question,conn=conn)
            else:
                sub_answer = meta_sql_solver(rewrite_sub_question,conn=conn)
            answer_cache[rewrite_sub_question] = sub_answer
            ichain["子问题回答"] = sub_answer
            board_label.append(ichain)
    # 上述是子问题求解，生成最终回答
    logger.info("============最终信息===============")
    logger.info(board_label)
    logger.info("============最终信息完毕===============")
    board_label_json = json.dumps(board_label,ensure_ascii=False,indent=4)
    final = fast_answer(question,board_label_json,final=True)
    return final

# 注意 A榜每一类使用不同的函数 ，B榜 除能源类和资料RAG类外都替换得 meta_sql_solver
def run_solver(question,conn,question_type):
    if question_type == "":
        question_type = question_classify(question)
    if question_type == "动作数据查询":
        return meta_sql_solver(question,conn=conn,question_type=question_type)
    if question_type == "设备数据查询":
        return meta_sql_solver(question,conn=conn,question_type=question_type)
    if question_type == "时长处理问题":
        return meta_sql_solver(question,conn=conn,question_type=question_type)
    if question_type == "盘点动作":
        return meta_sql_solver(question,conn=conn,question_type=question_type)
    if question_type == "能耗问题":
        return energy_solver(question,conn=conn,question_type=question_type)
    if question_type == "油耗问题":
        return energy_solver(question,conn=conn,question_type=question_type)
    if question_type == "资料查询问题":
        return meta_device_info_solver(question,conn=conn)
    if question_type == "理论发电量计算":
        return energy_solver(question,conn=conn,question_type=question_type)
    if question_type == "特殊条件时长问题":
        return meta_sql_solver(question,conn=conn,question_type=question_type)
    if question_type == "原始字段异常判断":
        return meta_sql_solver(question,conn=conn,question_type=question_type)
    if question_type == "未知分类":
        return meta_sql_solver(question,conn=conn)




if __name__ == "__main__":
    result = []
    fp = open("../data/科考船复赛a榜.jsonl", encoding="utf-8")
    fp_ctx = open("../submit/result_ctx.jsonl", 'a', encoding='utf-8')
    for idx,item in tqdm(enumerate(fp)):
        if idx < 62:
            continue
        if idx % 10 == 0:
            # flash zhipuai if has kv cache
            flash_zhipuai()
            pass
        node = json.loads(item)
        question = node["question"]
        logger.info(f"问题========{question}===========")
        answer = anything_sql_solver(question, conn)
        node["answer"] = answer
        logger.info(f"回答========{answer}===========")
        result.append(node)
        fp_ctx.write(json.dumps(node, ensure_ascii=False) + "\n")

    fp_output = open("../submit/result.jsonl", 'w', encoding='utf-8')

    for item in result:
        fp_output.write(json.dumps(item, ensure_ascii=False) + "\n")

    # result = anything_sql_solver("2024/08/15~2024/08/23（含）A架第一次开机最晚的一天（以mm/dd格式输出）？",conn)
    # print(result)
    # answer = run_solver("2024/08/16 00:00:00~2024/08/23 00:00:00 所有推进器的总能耗为多少（单位为kWh，结果保留两位小数）？",conn,question_type="能耗问题")
    # print(answer)
    # logger.add("log/solver_action.log")
    # #
    # qs = json.load(open("plan_b_classify.json",encoding="utf-8"))
    # # print(qs["动作数据查询"])
    # fp = open("total_result_6.jsonl","w",encoding="utf-8")

    # result = []
    # fp = open("../data/深远海初赛b榜题目.jsonl",encoding="utf-8")
    # for item in tqdm(fp):
    #     node = json.loads(item)
    #     node["question_type"] = question_classify(node["question"])
    #     result.append(node)
    #
    # for idx,item in tqdm(enumerate(result)):
    #     x_answer = run_solver(question=item["question"],conn=conn,question_type=item["question_type"])
    #     item["answer"] = x_answer
    #     fp.write(json.dumps(item,ensure_ascii=False)+"\n")
    #     logger.info("当前问题：" + item["question"] + "\n"+"最终回答：" + str(x_answer))
    # # json.dump(res,open("result_plan_b_v6.json","w",encoding="utf-8"),indent=4,ensure_ascii=False)
    #
    # import json
    #
    # fp1 = open("../data/深远海初赛b榜题目.jsonl", "r", encoding="utf-8")
    # # result = open("result_energy_and_content.jsonl","r",encoding="utf-8")
    #
    #
    # result_dict = {}
    # for item in result:
    #     # item = json.loads(item)
    #     result_dict[item["id"]] = item["answer"]
    #
    # fpn = open("../submit/jojo这个是我的逃跑路线_result.jsonl", "w", encoding="utf-8")
    # for row in fp1:
    #     t = json.loads(row)
    #     if t["id"] in result_dict:
    #         t["answer"] = result_dict[t["id"]]
    #     fpn.write(json.dumps(t, ensure_ascii=False) + "\n")