import json
import re
from sympy import simplify, N, Eq, S, sympify
from sympy.parsing.latex import parse_latex
from sympy.core.sympify import SympifyError


def preprocess_latex(expr_str):
    if isinstance(expr_str, float) or isinstance(expr_str, int):
        expr_str = str(expr_str)

    expr_str = expr_str.strip()

    expr_str = expr_str.replace("\\dfrac", "\\frac")
    # expr_str = expr_str.replace("\\left(", "(").replace("\\right)", ")")
    # expr_str = expr_str.replace("\\cdot", "*").replace("\\times", "*")

    # expr_str = re.sub(r"\s+", " ", expr_str)
    # expr_str = expr_str.replace("\\ ", " ").replace("\\,", " ")

    return expr_str

def safe_parse(expr_str):
    try:
        return parse_latex(expr_str)
    except:
        try:
            return sympify(expr_str)
        except SympifyError:
            cleaned = re.sub(r"[^0-9+\-*/.()^a-zA-Z]", "", expr_str)
            return sympify(cleaned)
        

def normalize_expression(expr_str):
    if not expr_str:
        return S(0)

    expr_str = preprocess_latex(expr_str)
    
    try:
        expr = safe_parse(expr_str)
        return simplify(expr)
    except Exception as e:
        print(f"Error parsing '{expr_str}': {str(e)}")
        return None


def is_answer_correct(given, standard, tolerance=1e-6):
    given_expr = normalize_expression(given)
    std_expr = normalize_expression(standard)
    
    if given_expr is None or std_expr is None:
        return False

    if given_expr.is_Number and std_expr.is_Number:
        return abs(float(given_expr) - float(std_expr)) < tolerance

    try:
        return simplify(given_expr - std_expr) == 0
    except:
        return False


def extract_last_boxed_answer(text):
    pattern = r'\\boxed\{(?:[^{}]|\{[^{}]*\})*\}'
    matches = re.findall(pattern, text)
    if matches:
        last_match = matches[-1]
        content = re.sub(r'^\\boxed\{|\}$', '', last_match)
        return content.strip()
    return None


def evaluate_answers(data):
    correct = 0
    total = 0
    results = []
    
    for item in data:
        total += 1
        # true_ans = item["data"]["answer"]
        true_ans = extract_last_boxed_answer(item["data"]["solution"])
        model_result = item["result"]["content"]
        model_ans = extract_last_boxed_answer(model_result)
        
        if model_ans is None:
            results.append({
                "idx": item["data"]["idx"],
                "true_answer": true_ans,
                "model_answer": "NO_BOXED_FOUND",
                "correct": False
            })
            continue
            
        try:
            is_correct = is_answer_correct(model_ans, true_ans)
            results.append({
                "idx": item["data"]["idx"],
                "true_answer": true_ans,
                "model_answer": model_ans,
                "correct": is_correct
            })
            if is_correct:
                correct += 1
        except Exception as e:
            print(f"Error evaluating: {str(e)}")
            results.append({
                "idx": item["data"]["idx"],
                "true_answer": true_ans,
                "model_answer": model_ans,
                "correct": False
            })
    
    accuracy = correct / total if total > 0 else 0
    return {
        "accuracy": accuracy,
        "correct": correct,
        "total": total,
        "details": results
    }

if __name__ == "__main__":

    json_path = "../test_datasets/aime24/aime24_1.5B_7B.json"

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    evaluation = evaluate_answers(data)

    print(f"Accuracy: {evaluation['accuracy']:.2%}")
    print(f"Correct: {evaluation['correct']}/{evaluation['total']}")

    output_path = json_path.replace(".json", "_eval.json")
    with open(output_path, "w", encoding='utf-8') as f:
        json.dump(evaluation, f, indent=4, ensure_ascii=False)
