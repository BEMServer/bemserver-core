import ast
import operator as op

from bemserver_core.exceptions import (
    BEMServerCoreExpressionEvaluationError,
    BEMServerCoreExpressionValidationError,
)
from bemserver_core.process import process

# https://stackoverflow.com/questions/2371436/evaluating-a-mathematical-expression-in-a-string/

OPERATORS = {
    ast.Add: op.add,
    ast.Sub: op.sub,
    ast.Mult: op.mul,
    ast.Div: op.truediv,
    ast.FloorDiv: op.floordiv,
    ast.Pow: op.pow,
    ast.USub: op.neg,
}


class ExprValidVisitor(ast.NodeTransformer):
    def __init__(self, variables):
        self._variables = variables

    def visit_Constant(self, node):
        pass

    def visit_Name(self, node):
        if node.id not in self._variables:
            raise BEMServerCoreExpressionValidationError("Unknown variable %s", node.id)

    def visit_UnaryOp(self, node):
        self.visit(node.operand)
        if type(node.op) not in OPERATORS:
            raise BEMServerCoreExpressionValidationError("Unknown operator %s", node.op)

    def visit_BinOp(self, node):
        self.visit(node.left)
        self.visit(node.right)
        if type(node.op) not in OPERATORS:
            raise BEMServerCoreExpressionValidationError("Unknown operator %s", node.op)

    def generic_visit(self, node):
        raise BEMServerCoreExpressionValidationError("Invalid node: %s", repr(node))


class ExprEvalVisitor(ast.NodeTransformer):
    def __init__(self, namespace):
        self._namespace = namespace

    def visit_Constant(self, node):
        return node.value

    def visit_Name(self, node):
        try:
            return self._namespace[node.id]
        except KeyError as exc:
            raise BEMServerCoreExpressionEvaluationError(
                "Unknown variable %s", node.id
            ) from exc

    def visit_UnaryOp(self, node):
        val = self.visit(node.operand)
        try:
            return OPERATORS[type(node.op)](val)
        except KeyError as exc:
            raise BEMServerCoreExpressionEvaluationError(
                "Unknown operator %s", node.op
            ) from exc

    def visit_BinOp(self, node):
        lhs = self.visit(node.left)
        rhs = self.visit(node.right)
        try:
            return OPERATORS[type(node.op)](lhs, rhs)
        except KeyError as exc:
            raise BEMServerCoreExpressionEvaluationError(
                "Unknown operator %s", node.op
            ) from exc

    def generic_visit(self, node):
        raise BEMServerCoreExpressionEvaluationError("Invalid node: %s", repr(node))


def validate(expr, variables):
    try:
        parsed_expr = ast.parse(expr, mode="eval").body
    except SyntaxError as exc:
        raise BEMServerCoreExpressionValidationError("Syntax error") from exc
    ExprValidVisitor(variables).visit(parsed_expr)


@process
def evaluate(expr, namespace):
    try:
        parsed_expr = ast.parse(expr, mode="eval").body
    except SyntaxError as exc:
        raise BEMServerCoreExpressionEvaluationError("Syntax error") from exc
    try:
        return ExprEvalVisitor(namespace).visit(parsed_expr)
    except ArithmeticError as exc:
        raise BEMServerCoreExpressionEvaluationError(exc) from exc
