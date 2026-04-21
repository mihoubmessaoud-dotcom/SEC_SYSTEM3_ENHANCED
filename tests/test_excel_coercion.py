from core.excel_coercion import coerce_excel_number


def test_coerce_thousands_and_dot_decimal():
    assert coerce_excel_number("1,630,338,779") == 1630338779.0
    assert abs(coerce_excel_number("26.0378378378") - 26.0378378378) < 1e-12
    assert abs(coerce_excel_number("1,691.00059427") - 1691.00059427) < 1e-12


def test_coerce_comma_decimal():
    assert abs(coerce_excel_number("26,03") - 26.03) < 1e-12


def test_coerce_parens_and_estimate_prefix():
    assert coerce_excel_number("(123.4)") == -123.4
    assert coerce_excel_number("˜123.4") == 123.4

