import unittest
from main import get_good_data
from tests.test_cases import TEST_CASES
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class InventoryToolTestCase(unittest.TestCase):

    def setUp(self):
        """Начальные условия для тестов."""
        self.all_test_cases = TEST_CASES

    def test_data_for_query(self):
        """Тестирование извлечения данных о товаре из словаря."""

        for test_case in self.all_test_cases:
            test_input = test_case.get("test_input")
            expected = test_case.get("expected")

            self.assertEqual(get_good_data(test_input), expected)
