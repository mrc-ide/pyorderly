import ast


# In the R version of this function we do a more involved read, trying
# to handle "static" versions of all of the orderly core
# functions. However,we then never actually used that for anything, so
# to avoid overcomplication we'll do the least possible here which is
# just to read the parameters from the file, verify that it is only
# called once, and that is called at the top-level.
#
# The return value does nod at future extension though.
def orderly_read(path):
    src = path.read_text()
    return _read_py(src)


def _read_py(src):
    module = ast.parse(src)
    v = Visitor()
    v.read_body(module.body)
    return {"parameters": v.parameters or {}}


class Visitor:
    def __init__(self):
        self.parameters = None

    def read_body(self, stmts):
        for stmt in stmts:
            self._read_stmt(stmt)

    def _read_stmt(self, stmt):
        if isinstance(stmt, (ast.Expr, ast.Assign)):
            self._read_expr(stmt.value)
        elif _match_name_check(stmt) == "__main__":
            self.read_body(stmt.body)

    def _read_expr(self, expr):
        name = _match_orderly_call(expr)
        if name == "parameters":
            self._read_parameters(expr)

    def _read_parameters(self, call):
        if call.args:
            msg = "All arguments to 'parameters()' must be named"
            raise Exception(msg)

        data = {}
        for kw in call.keywords:
            nm = kw.arg
            if kw.arg is None:
                msg = "Passing parameters as **kwargs is not supported"
                raise Exception(msg)
            value = kw.value
            if nm in data:
                msg = f"Duplicate argument '{nm}' to 'parameters()'"
                raise Exception(msg)
            if not _is_valid_parameter_value(value):
                msg = f"Invalid value for argument '{nm}' to 'parameters()'"
                raise Exception(msg)
            data[nm] = value.value

        if self.parameters is not None:
            msg = f"Duplicate call to 'parameters()' on line {call.lineno}"
            raise Exception(msg)
        else:
            self.parameters = data


def _is_identifier(node, value):
    return isinstance(node, ast.Name) and node.id == value


def _match_name_check(stmt):
    if not isinstance(stmt, ast.If):
        return None

    if not isinstance(stmt.test, ast.Compare):
        return None

    if len(stmt.test.ops) != 1 or len(stmt.test.comparators) != 1:
        return None
    if not isinstance(stmt.test.ops[0], ast.Eq):
        return None

    lhs = stmt.test.left
    rhs = stmt.test.comparators[0]

    if _is_identifier(lhs, "__name__") and isinstance(rhs, ast.Constant):
        return rhs.value
    elif _is_identifier(rhs, "__name__") and isinstance(lhs, ast.Constant):
        return lhs.value
    else:
        return None


def _match_orderly_call(expr):
    if not isinstance(expr, ast.Call):
        return None

    if not isinstance(expr.func, ast.Attribute):
        return None

    if _is_identifier(expr.func.value, "pyorderly"):
        return expr.func.attr


def _is_valid_parameter_value(value):
    if not isinstance(value, ast.Constant):
        return False
    return value.value is None or isinstance(value.value, (float, int, str))
