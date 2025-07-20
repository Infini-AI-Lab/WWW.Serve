import json
import re
import numpy as np


def preprocess_latex(expr_str):
    if isinstance(expr_str, float) or isinstance(expr_str, int):
        expr_str = str(expr_str)

    expr_str = expr_str.strip()

    expr_str = expr_str.replace(" ", "")
    expr_str = expr_str.replace("\\dfrac", "\\frac")
    expr_str = expr_str.replace("\\left(", "(").replace("\\right)", ")")
    expr_str = expr_str.replace("\\left[", "[").replace("\\right]", "]")
    expr_str = expr_str.replace("\\left\\{", "{").replace("\\right\\}", "}")

    return expr_str


def is_answer_correct(given_expr, std_expr, tolerance=1e-6):
    if given_expr is None or std_expr is None:
        return False

    return given_expr in std_expr or std_expr in given_expr


def extract_last_boxed_answer(text):
    pattern = r'\\boxed\{(?:[^{}]|\{[^{}]*\})*\}'
    matches = re.findall(pattern, text)
    if matches:
        last_match = matches[-1]
        content = re.sub(r'^\\boxed\{|\}$', '', last_match)
        return content.strip()
    return None


def get_whole_answer_and_logprobs(logprobs_dict):
    ans = ""
    logprobs_list = []

    for item in logprobs_dict:
        ans += item["token"]
        logprobs_list.append(item["logprob"])
    
    return ans.strip(), logprobs_list


def evaluate_answers(data):
    correct = 0
    total = 0
    results = []

    for idx, item in enumerate(data):
        total += 1
        if "logprobs_dict" not in item or not item["logprobs_dict"]:
            results.append({
                "idx": idx,
                "true_answer": None,
                "model_answer": "NO_LOGPROBS_DICT",
                "correct": False,
                "avg_logprob": None,
                "var_logprob": None
            })
            continue

        true_ans = item["problem"]["answer"].strip()
        model_result, logprobs_list = get_whole_answer_and_logprobs(item["logprobs_dict"])
        model_ans = extract_last_boxed_answer(model_result)

        std_expr = preprocess_latex(true_ans)

        if model_ans is None:
            results.append({
                "idx": idx,
                "true_answer": std_expr,
                "model_answer": "NO_BOXED_FOUND",
                "correct": False,
                "avg_logprob": None,
                "var_logprob": None
            })
            continue
        

        try:
            given_expr = preprocess_latex(model_ans)
            is_correct = is_answer_correct(given_expr, std_expr)
            results.append({
                "idx": idx,
                "true_answer": std_expr,
                "model_answer": given_expr,
                "correct": is_correct,
                "avg_logprob": np.mean(logprobs_list) if logprobs_list else None,
                "var_logprob": np.var(logprobs_list) if logprobs_list else None
            })
            if is_correct:
                correct += 1
        except Exception as e:
            print(f"Error evaluating: {str(e)}")
            results.append({
                "idx": idx,
                "true_answer": std_expr,
                "model_answer": given_expr,
                "correct": False,
                "avg_logprob": None,
                "var_logprob": None
            })
    
    accuracy = correct / total if total > 0 else 0
    return {
        "accuracy": accuracy,
        "correct": correct,
        "total": total,
        "details": results
    }

if __name__ == "__main__":

    json_path = "/home/hywang/Reasoning/temp.json"

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    evaluation = evaluate_answers(data)

    print(f"Accuracy: {evaluation['accuracy']:.2%}")
    print(f"Correct: {evaluation['correct']}/{evaluation['total']}")

    output_path = json_path.replace(".json", "_eval.json")
    with open(output_path, "w", encoding='utf-8') as f:
        json.dump(evaluation, f, indent=4, ensure_ascii=False)
