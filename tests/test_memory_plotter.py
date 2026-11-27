"""
Unit tests for the MemoryPlotter class
"""

# pyright: strict, reportPrivateUsage=false
#
# We are OK to ignore private use in unit tests as the whole point of the tests
# is to validate the functions contained in the module

from unittest.mock import MagicMock

from post_processing.plotter.memory_plotter import (
    MEMORY_PLOT_LABEL,
    MEMORY_SOURCE_COLOURS,
    MEMORY_Y_LABEL,
    MemoryPlotter,
)


class TestMemoryPlotter:
    """Test suite for MemoryPlotter class"""

    def test_initialization(self) -> None:
        """Test that MemoryPlotter initializes correctly"""
        mock_axes = MagicMock()
        plotter = MemoryPlotter(main_axis=mock_axes)

        # With no data added there is nothing to plot, so no secondary axis is created
        plotter.plot(x_data=[1.0])

        mock_axes.twinx.assert_not_called()

    def test_memory_constants(self) -> None:
        """Test that memory constants are defined correctly"""
        assert MEMORY_Y_LABEL == "Memory use (Mb)"
        assert MEMORY_PLOT_LABEL == "Memory use"
        assert "fio" in MEMORY_SOURCE_COLOURS
        assert "collectl" in MEMORY_SOURCE_COLOURS
        assert "default" in MEMORY_SOURCE_COLOURS

    def test_add_y_data_legacy_format(self) -> None:
        """Test adding memory data points in legacy string format"""
        mock_axes = MagicMock()
        plotter = MemoryPlotter(main_axis=mock_axes)

        plotter.add_y_data("100.5")
        plotter.add_y_data("200.75")
        plotter.add_y_data("150.25")
        plotter.plot(x_data=[1.0, 2.0, 3.0])

        memory_axes = mock_axes.twinx.return_value
        memory_axes.plot.assert_called_once()
        assert memory_axes.plot.call_args.args[1] == [100.5, 200.75, 150.25]
        assert memory_axes.plot.call_args.kwargs["label"] == MEMORY_PLOT_LABEL

    def test_add_y_data_multi_source_format(self) -> None:
        """Test adding memory data in multi-source dict format"""
        mock_axes = MagicMock()
        plotter = MemoryPlotter(main_axis=mock_axes)

        plotter.add_y_data({"fio": "100.5", "collectl": "105.0"})
        plotter.add_y_data({"fio": "200.75", "collectl": "210.0"})
        plotter.plot(x_data=[1.0, 2.0])

        # Each source is plotted as its own line, labelled with the source name
        y_data_by_label = {
            call.kwargs["label"]: call.args[1] for call in mock_axes.twinx.return_value.plot.call_args_list
        }
        assert y_data_by_label == {
            f"{MEMORY_PLOT_LABEL} (collectl)": [105.0, 210.0],
            f"{MEMORY_PLOT_LABEL} (fio)": [100.5, 200.75],
        }

    def test_add_y_data_converts_string_to_float(self) -> None:
        """Test that add_y_data converts string values to float"""
        mock_axes = MagicMock()
        plotter = MemoryPlotter(main_axis=mock_axes)

        plotter.add_y_data("42")
        plotter.plot(x_data=[1.0])

        y_data = mock_axes.twinx.return_value.plot.call_args.args[1]
        assert y_data == [42.0]
        assert isinstance(y_data[0], float)

    def test_plot_legacy_single_source(self) -> None:
        """Test plotting memory data with legacy single source"""
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

        # Verify set_ylabel was called
        mock_memory_axes.set_ylabel.assert_called_once_with(MEMORY_Y_LABEL)

        # Verify plot was called on the memory axis
        mock_memory_axes.plot.assert_called_once()

    def test_plot_multi_source(self) -> None:
        """Test plotting memory data with multiple sources"""
        mock_main_axes = MagicMock()
        mock_memory_axes = MagicMock()
        mock_main_axes.twinx.return_value = mock_memory_axes

        plotter = MemoryPlotter(main_axis=mock_main_axes)
        plotter.add_y_data({"fio": "100", "collectl": "105"})
        plotter.add_y_data({"fio": "200", "collectl": "210"})

        x_data: list[float] = [1.0, 2.0]
        plotter.plot(x_data=x_data)

        # Verify twinx was called to create secondary axis
        mock_main_axes.twinx.assert_called_once()

        # Verify set_ylabel was called
        mock_memory_axes.set_ylabel.assert_called_once_with(MEMORY_Y_LABEL)

        # Verify plot was called twice (once per source)
        assert mock_memory_axes.plot.call_count == 2

        # Verify legend was added for multiple sources
        mock_memory_axes.legend.assert_called_once()

    def test_plot_with_custom_colour_ignored(self) -> None:
        """Test that custom colour parameter is ignored"""
        mock_main_axes = MagicMock()
        mock_memory_axes = MagicMock()
        mock_main_axes.twinx.return_value = mock_memory_axes

        plotter = MemoryPlotter(main_axis=mock_main_axes)
        plotter.add_y_data("100")

        x_data: list[float] = [1.0]
        # Pass a custom colour, but it should be ignored
        plotter.plot(x_data=x_data, colour="#FF0000")

        # The line is drawn in the colour for its source, not the one that was passed in
        mock_memory_axes.plot.assert_called_once()
        assert mock_memory_axes.plot.call_args.kwargs["color"] == MEMORY_SOURCE_COLOURS["default"]

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
