"""
Class for collecting and managing plot data points.
"""


class PlotDataCollector:
    """Collects and manages plot data points."""

    def __init__(self) -> None:
        self.x_data: list[float] = []
        self.error_bars: list[float] = []

    def add_point(self, x_value: float, error_bar: float) -> None:
        """Add a data point to the collection."""
        self.x_data.append(x_value)
        self.error_bars.append(error_bar)

    def is_empty(self) -> bool:
        """Check if no data points have been collected."""
        return len(self.x_data) == 0


# Made with Bob
