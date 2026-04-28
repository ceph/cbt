"""
Unit tests for the MemoryPlotter class
"""

from unittest.mock import MagicMock, patch

import pytest

from post_processing.plotter.memory_plotter import (
    MEMORY_PLOT_DEFAULT_COLOUR,
    MEMORY_PLOT_LABEL,
    MEMORY_Y_LABEL,
    MemoryPlotter,
)


class TestMemoryPlotter:
    """Test suite for MemoryPlotter class"""

    def test_initialization(self) -> None:
        """Test that MemoryPlotter initializes correctly"""
        mock_axes = MagicMock()
        plotter = MemoryPlotter(main_axis=mock_axes)

        assert plotter._main_axes == mock_axes
        assert plotter._y_data == []
        assert plotter._label == ""
        assert plotter._y_label == ""

    def test_memory_constants(self) -> None:
        """Test that memory constants are defined correctly"""
        assert MEMORY_PLOT_DEFAULT_COLOUR == "#4b006e"
        assert MEMORY_Y_LABEL == "Memory use (Mb)"
        assert MEMORY_PLOT_LABEL == "Memory use"

    def test_add_y_data(self) -> None:
        """Test adding memory data points"""
        mock_axes = MagicMock()
        plotter = MemoryPlotter(main_axis=mock_axes)

        plotter.add_y_data("100.5")
        plotter.add_y_data("200.75")
        plotter.add_y_data("150.25")

        assert plotter._y_data == [100.5, 200.75, 150.25]

    def test_add_y_data_converts_string_to_float(self) -> None:
        """Test that add_y_data converts string values to float"""
        mock_axes = MagicMock()
        plotter = MemoryPlotter(main_axis=mock_axes)

        plotter.add_y_data("42")
        assert plotter._y_data == [42.0]
        assert isinstance(plotter._y_data[0], float)

    def test_plot(self) -> None:
        """Test plotting memory data"""
        mock_main_axes = MagicMock()
        mock_memory_axes = MagicMock()
        mock_main_axes.twinx.return_value = mock_memory_axes

        plotter = MemoryPlotter(main_axis=mock_main_axes)
        plotter.add_y_data("100")
        plotter.add_y_data("200")

        x_data: list[float] = [1.0, 2.0]
        plotter.plot(x_data=x_data)

        # Verify twinx was called to create secondary axis
        mock_main_axes.twinx.assert_called_once()

        # Verify labels were set
        assert plotter._label == MEMORY_PLOT_LABEL
        assert plotter._y_label == MEMORY_Y_LABEL

        # Verify plot was called on the memory axis
        mock_memory_axes.plot.assert_called_once()

    def test_plot_with_custom_colour_ignored(self) -> None:
        """Test that custom colour parameter is ignored and default is used"""
        mock_main_axes = MagicMock()
        mock_memory_axes = MagicMock()
        mock_main_axes.twinx.return_value = mock_memory_axes

        plotter = MemoryPlotter(main_axis=mock_main_axes)
        plotter.add_y_data("100")

        x_data: list[float] = [1.0]
        # Pass a custom colour, but it should be ignored
        plotter.plot(x_data=x_data, colour="#FF0000")

        # Verify the default colour is used
        call_args = mock_memory_axes.plot.call_args
        assert MEMORY_PLOT_DEFAULT_COLOUR in str(call_args)

    def test_plot_sets_y_label_on_axis(self) -> None:
        """Test that plot sets the y-axis label"""
        mock_main_axes = MagicMock()
        mock_memory_axes = MagicMock()
        mock_main_axes.twinx.return_value = mock_memory_axes

        plotter = MemoryPlotter(main_axis=mock_main_axes)
        plotter.add_y_data("100")

        x_data: list[float] = [1.0]
        plotter.plot(x_data=x_data)

        # Verify set_ylabel was called with correct label
        mock_memory_axes.set_ylabel.assert_called_once_with(MEMORY_Y_LABEL)

# Made with Bob
