import ast
import re
import unittest
from pathlib import Path


class PercentFormatCIGuardrailTests(unittest.TestCase):
    """
    Permanent guardrail:
    - Percent conversion (*100, /100, '%' rendering) is forbidden in computation modules.
    - Allowed only in formatter modules.
    """

    ALLOWED_PERCENT_FILES = {
        Path('modules/ratio_formats.py').resolve(),
        Path('modules/formatters.py').resolve(),  # reserved optional formatter module
    }

    COMPUTATION_FILES = [
        Path('modules/sec_fetcher.py'),
        Path('modules/institutional/ratios.py'),
        Path('modules/institutional/validators.py'),
        Path('modules/institutional/engine.py'),
        Path('modules/institutional/computation.py'),
    ]

    def _is_hundred_constant(self, node):
        return isinstance(node, ast.Constant) and isinstance(node.value, (int, float)) and float(node.value) == 100.0

    def _scan_file(self, file_path: Path):
        code = file_path.read_text(encoding='utf-8')
        tree = ast.parse(code, filename=str(file_path))
        findings = []

        for node in ast.walk(tree):
            if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Mult, ast.Div)):
                if self._is_hundred_constant(node.left) or self._is_hundred_constant(node.right):
                    findings.append(f'{file_path}:{getattr(node, "lineno", 0)} forbidden_100_arithmetic')

        # Block explicit display-percent construction in computation files.
        for lineno, line in enumerate(code.splitlines(), start=1):
            if re.search(r'display_text.*%', line) or re.search(r'suffix\s*=\s*[\'"]%[\'"]', line):
                findings.append(f'{file_path}:{lineno} percent_rendering_outside_formatter')

        return findings

    def test_percent_conversion_blocked_outside_formatter(self):
        failures = []
        for file_path in self.COMPUTATION_FILES:
            resolved = file_path.resolve()
            if not file_path.exists():
                continue
            if resolved in self.ALLOWED_PERCENT_FILES:
                continue
            failures.extend(self._scan_file(file_path))
        self.assertFalse(
            failures,
            msg='Percent conversion/rendering must exist only in formatter modules. Violations:\n' + '\n'.join(failures),
        )


if __name__ == '__main__':
    unittest.main()
