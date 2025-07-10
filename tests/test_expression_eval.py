"""Test expression evaluation"""

from unittest import mock

import pytest

from bemserver_core import expression_eval
from bemserver_core.exceptions import (
    BEMServerCoreExpressionEvaluationError,
    BEMServerCoreExpressionValidationError,
    BEMServerCoreProcessTimeoutError,
)


@pytest.mark.parametrize(
    "expr",
    (
        "a+b",
        "a**b",
        "a**2",
        "69*a**2-42*b+12",
        "a/b",
        "a//b",
        "-a",
        "--a",
    ),
)
def test_expr_eval_validate_ok(expr):
    expression_eval.validate(expr, ("a", "b"))


@pytest.mark.parametrize(
    "expr",
    (
        "a b",
        "a+",
        "a-",
        "a*",
        "a**",
        "a/a***b",
        "a*/2",
        "69&a",
        "a==b",
        "a+=2",
        "z",
    ),
)
def test_expr_eval_validate_validation_error(expr):
    with pytest.raises(BEMServerCoreExpressionValidationError):
        expression_eval.validate(expr, ("a", "b"))


@pytest.mark.parametrize(
    ("expr", "result"),
    (
        ("a+b", 54),
        ("a**b", 2116471057875484488839167999221661362284396544),
        ("a**2", 144),
        ("69*a**2-42*b+12", 8184),
        ("a/b", 0.2857142857142857),
        ("a//b", 0),
        ("-a", -12),
        ("--a", 12),
    ),
)
def test_expr_eval_evaluate_ok(expr, result):
    namespace = {"a": 12, "b": 42}
    assert expression_eval.evaluate(expr, namespace) == result


@pytest.mark.parametrize(
    "expr",
    (
        "a b",
        "a***b",
        "a*/2",
        "69&a",
        "a==b",
        "a+=2",
        "a/0",
        "z",
    ),
)
def test_expr_eval_evaluate_evaluation_error(expr):
    namespace = {"a": 12, "b": 42}
    with pytest.raises(BEMServerCoreExpressionEvaluationError):
        expression_eval.evaluate(expr, namespace)


@mock.patch("bemserver_core.process.TIMEOUT", 0.1)
def test_expr_eval_evaluate_timeout_error():
    with pytest.raises(BEMServerCoreProcessTimeoutError):
        expression_eval.evaluate("42**42**42", {})
