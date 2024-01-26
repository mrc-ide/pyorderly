import ast


def orderly_read(path):
    src = ast.parse(path.read_text())
    return _read_py(src)


# In the R version of this function we do a more involved read, trying
# to handle "static" versions of all of the orderly core
# functions. However,we then never actually used that for anything, so
# to avoid overcomplication we'll do the least possible here which is
# just to read the parameters from the file, verify that it is only
# called once, and that is called at the top-level.
#
# The return value does nod at future extension though.
def _read_py(src):
    ret = {"parameters": []}
    for expr in src.body:
        dat = _read_expr(expr)
        if dat and dat["name"] == "parameters":
            if ret["parameters"]:
                msg = f"Duplicate call to 'parameters()' on line {expr.lineno}"
                raise Exception(msg)
            ret["parameters"].append(dat["data"])
    ret["parameters"] = ret["parameters"][0] if ret["parameters"] else {}
    return ret


def _read_expr(expr):
    if _is_orderly_call(expr):
        if expr.value.func.attr == "parameters":
            return _read_parameters(expr.value)
        return None
    return None


def _is_orderly_call(expr):
    if not isinstance(expr, ast.Expr):
        return False
    if not isinstance(expr.value, ast.Call):
        return False
    call = expr.value
    return call.func.value.id == "orderly" and call.func.attr


def _read_parameters(call):
    if call.args:
        msg = "All arguments to 'parameters()' must be named"
        raise Exception(msg)
    data = {}
    for kw in call.keywords:
        nm = kw.arg
        value = kw.value
        if nm in data:
            msg = f"Duplicate argument '{nm}' to 'parameters()'"
            raise Exception(msg)
        if not isinstance(value, ast.Constant):
            msg = f"Invalid value for argument '{nm}' to 'parameters()'"
            raise Exception(msg)
        data[nm] = kw.value.value
    return {"name": "parameters", "data": data}
