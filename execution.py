import traceback

def execute_code(code: str):
    local_vars = {}

    try:
        exec(code, {}, local_vars)

        result = local_vars.get("result", None)

        return {
            "result": result,
            "summary": "Execution successful",
            "error": None
        }

    except Exception as e:
        return {
            "result": None,
            "summary": "Execution failed",
            "error": traceback.format_exc()
        }