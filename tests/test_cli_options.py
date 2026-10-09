"""
Unit tests for the CliOptions configuration class.
"""

import unittest
from typing import ClassVar, Optional

import pytest

from cli_options import CliOptions


class TestCliOptions(unittest.TestCase):
    """
    Tests for the cli_options class
    """

    DEFAULT_DATA: ClassVar[dict[str, Optional[str]]] = {"iodepth": "12", "mode": "randrw"}
    DEFAULT_NEW_DATA: ClassVar[dict[str, Optional[str]]] = {"new": "value"}

    def _init_cli_options(self, options: Optional[dict[str, Optional[str]]] = None) -> CliOptions:
        """
        Create an initial CliOptions class to test against
        """
        if options is None:
            options = self.DEFAULT_DATA
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

    def _test_update(self, update_data: dict[str, Optional[str]], expected_options: dict[str, Optional[str]]) -> None:
        """
        Common code for testing the update function
        """
        actual_options = self._init_cli_options()
        actual_options.update(update_data)
        self._assert_cli_options_are_equal(actual_options, self._init_cli_options(expected_options))

    def _test_add(
        self, key_to_add: str, value_to_add: Optional[str], expected_options: dict[str, Optional[str]]
    ) -> None:
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
        expected_options: dict[str, Optional[str]] = self.DEFAULT_DATA | self.DEFAULT_NEW_DATA

        self._test_update(self.DEFAULT_NEW_DATA, expected_options)

    def test_update_existing_key_does_not_overwrite(self) -> None:
        """
        update() with a pre-existing key must not overwrite the existing value.
        The no-overwrite guard is implemented via __setitem__.
        """
        options = self._init_cli_options()
        original_iodepth = options["iodepth"]
        options.update({"iodepth": "999"})
        self.assertEqual(options["iodepth"], original_iodepth, "Existing key must not be overwritten by update()")

    def test_direct_setitem_existing_key_does_not_overwrite(self) -> None:
        """
        Direct assignment with [] on a pre-existing key must not overwrite.
        """
        options = self._init_cli_options()
        original_mode = options["mode"]
        options["mode"] = "newmode"
        self.assertEqual(options["mode"], original_mode, "Existing key must not be overwritten by direct assignment")

    def test_add_new_value(self) -> None:
        """
        Validate adding a key/value pair to the CliOptions works
        """
        key: str = "added"
        value: str = "value"
        expected_options: dict[str, Optional[str]] = {key: value}
        expected_options.update(self.DEFAULT_DATA)
        self._test_add(key, value, expected_options)

    def test_add_none_value(self) -> None:
        """
        Validate adding a key with None value to CliOptions works
        """
        key: str = "suppressed_flag"
        value: Optional[str] = None
        expected_options: dict[str, Optional[str]] = {key: value}
        expected_options.update(self.DEFAULT_DATA)
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
