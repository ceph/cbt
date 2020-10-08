from enum import Enum
from typing import NamedTuple, Optional


class AnnotationLevel(Enum):
    NOTICE = 'notice'
    WARNING = 'warning'
    FAILURE = 'failure'


class Annotation(NamedTuple):
    path: str
    start_line: int
    end_line: int
    annotation_level: AnnotationLevel
    message: str
    start_column: Optional[int] = None
    end_column: Optional[int] = None
    title: Optional[str] = None
    raw_details: Optional[str] = None

    def to_dict(self):
        # https://developer.github.com/v3/checks/runs/#annotations-object
        assert(len(self.title) <= 255)
        assert(len(self.message) <= 64 * 1024)
        assert(self.raw_details is None or len(self.raw_details) <= 64 * 1024)

        if self.end_line is None:
            self.end_line = self.start_line
        # this is required even though it's not explicitly noted.
        if self.end_column is None:
            self.end_column = self.start_column

        # use the enum's str value in the dict
        d = self._replace(annotation_level=self.annotation_level.value)
        return {k: v for (k, v) in d._asdict().items() if v is not None}
