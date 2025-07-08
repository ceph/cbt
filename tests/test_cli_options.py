import unittest
from typing import Optional

import pytest

from cli_options import CliOptions


class TestCliOptions(unittest.TestCase):
    """
    Tests for the cli_options class
    """

    DEFAULT_DATA: dict[str, str] = {"iodepth": "12", "mode": "randrw"}
    DEFAULT_NEW_DATA: dict[str, str] = {"new": "value"}

    def _init_cli_options(self, options: Optional[dict[str, str]] = DEFAULT_DATA) -> CliOptions:
        """
        Create an initial CliOptions class to test against
        """
        return CliOptions(options)

    def _assert_cli_options_are_equal(self, actual_options: CliOptions, expected_options: CliOptions) -> None:
        """
        Validate that the expected and actual values are equal
        """
        # pytests assertEqual will check dictionary or list equality, including key
        # names and values.
        self.assertEqual(actual_options.keys(), expected_options.keys())
        self.assertEqual(len(actual_options), len(expected_options))
        self.assertEqual(actual_options, expected_options)

    def _test_update(self, update_data: dict[str, str], expected_options: dict[str, str]) -> None:
        """
        Common code for testing the update function
        """
        actual_options = self._init_cli_options()
        actual_options.update(update_data)
        self._assert_cli_options_are_equal(actual_options, self._init_cli_options(expected_options))

    def _test_add(self, key_to_add: str, value_to_add: str, expected_options: dict[str, str]) -> None:
        """
        Common code for testing the add function
        """
        actual_options = self._init_cli_options()
        actual_options[key_to_add] = value_to_add
        self._assert_cli_options_are_equal(actual_options, self._init_cli_options(expected_options))

    def test_update_new_value(self) -> None:
        """
        Test updating the CliOptions with a new value
        """
        expected_options: dict[str, str] = self.DEFAULT_DATA | self.DEFAULT_NEW_DATA

        self._test_update(self.DEFAULT_NEW_DATA, expected_options)

    def test_update_value_already_exists(self) -> None:
        """
        Validate that the values in the CliOptions are not overwritten
        when a set of new values is passed
        """
        expected_options: dict[str, str] = self.DEFAULT_DATA
        update_data: dict[str, str] = {"iodepth": "22"}

        self._test_update(update_data, expected_options)

    def test_add_new_value(self) -> None:
        """
        Validate adding a key/value pair to the CliOptions works
        """
        key: str = "added"
        value: str = "value"
        expected_options: dict[str, str] = {key: value}
        expected_options.update(self.DEFAULT_DATA)
        self._test_add(key, value, expected_options)

    def test_add_value_already_exists(self) -> None:
        """
        Validate adding a key that already exists in the CliOptions
        does not update the existing value for that key
        """
        key: str = "mode"
        value: str = "bob"
        expected_options: dict[str, str] = self.DEFAULT_DATA
        self._test_add(key, value, expected_options)

    def test_get_item_that_exists(self) -> None:
        """
        Validate that getting an item from the CliOptions that exists
        returns the correct value
        """
        actual_options = self._init_cli_options()
        try:
            test_value: Optional[str] = actual_options["iodepth"]
            self.assertEqual(test_value, self.DEFAULT_DATA["iodepth"])
        except KeyError:
            pytest.fail("KeyError exception raised!")

    def test_get_item_not_exist(self) -> None:
        """
        Validate that a KeyError exception is not thrown and None is returned
        when asking for the value of a key that doesn't exist.
        """
        actual_options = self._init_cli_options()
        try:
            test_value: Optional[str] = actual_options["bob"]
            self.assertIsNone(test_value)
        except KeyError:
            pytest.fail("KeyError exception raised!")

    def test_clear(self) -> None:
        """
        Validate that the clear method removes all options from CliOptions
        """
        actual_options = self._init_cli_options()
        self.assertEqual(actual_options, CliOptions(self.DEFAULT_DATA))
        actual_options.clear()
        self.assertEqual(actual_options, {})
        actual_options.update(self.DEFAULT_NEW_DATA)
        self.assertEqual(actual_options, CliOptions(self.DEFAULT_NEW_DATA))
