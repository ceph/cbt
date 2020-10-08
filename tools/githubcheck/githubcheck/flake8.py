import os.path
import re
import jinja2

from collections import namedtuple

from .annotation import AnnotationLevel, Annotation

StatsLine = namedtuple('StatsLine', ['total', 'error', 'details'])

summary_template = '''{{ headers | join('|') }}
{{ ( headers | length * '-') | join('|') }}
{%- for issue in issues -%}
{% set err_link = "[`{err}`](https://www.flake8rules.com/rules/{err}.html)".format(err=issue.error) %}
{{ [issue.total, err_link, issue.details] | join('|') }}
{%- endfor %}
'''


class Parser:
    def __init__(self, base_dir, title, preamble):
        self.title = title
        self.base_dir = base_dir
        # assume the test is launched by ctest
        # preamble looks like:
        testenv, command = preamble.split(':', 1)
        # pep8 run-test: commands[0] | flake8 --config=tox.ini .
        self.preample_re = f'{testenv} run-test: commands\[\d+\] \| {command}'
        # 67    E111 indentation is not a multiple of four
        self.statistics_re = r'(\d+)\s+(\w+)\s+(\S.*)$'
        self.ctest_id_re = r'\d+: '
        self.run_with_ctest = False

    def _match(self, pattern, line):
        if self.run_with_ctest:
            return re.match(self.ctest_id_re + ' ' + pattern, line)
        elif re.match(self.ctest_id_re + ' ' + pattern, line):
            self.run_with_ctest = True
            return re.match(self.ctest_id_re + ' ' + pattern, line)
        else:
            return re.match(pattern, line)

    def _annotation_from_line(self, line):
        # <ctest-id>: <path>:<line-no>:<col-no>: E128 <detailed message>
        # see also https://flake8.pycqa.org/en/latest/user/options.html#cmdoption-flake8-format
        if self.run_with_ctest:
            ctest_id, rel_path, row, col, message = line.split(':', 4)
        else:
            rel_path, row, col, message = line.split(':', 3)
        path = os.path.normpath(os.path.join(self.base_dir, rel_path))
        return Annotation(path,
                          int(row),
                          int(row),
                          AnnotationLevel.WARNING,
                          message.strip(),
                          start_column=int(col),
                          end_column=int(col),
                          title=self.title,
                          raw_details=line)
    WAIT_ERRORS = 0
    READ_ERRORS = 1
    READ_SUMMARY = 2

    def _check_statistics_line(self, line):
        matched = self._match(self.statistics_re, line)
        if matched:
            total, err, details = matched.groups()
            return self.READ_SUMMARY, StatsLine(total, err, details)
        elif self._match(self.preample_re, line):
            return self.READ_ERRORS, None
        else:
            return self.WAIT_ERRORS, None

    def scan(self, output):
        todo = self.WAIT_ERRORS
        all_stats = []
        for line in output:
            line = line.strip()
            if todo == self.WAIT_ERRORS:
                if self._match(self.preample_re, line):
                    todo = self.READ_ERRORS
            elif todo == self.READ_ERRORS:
                try:
                    yield self._annotation_from_line(line)
                except ValueError:
                    todo, stats = self._check_statistics_line(line)
                    if todo == self.READ_SUMMARY:
                        all_stats.append(stats)
            elif todo == self.READ_SUMMARY:
                todo, stats = self._check_statistics_line(line)
                if todo == self.READ_SUMMARY:
                    all_stats.append(stats)
        if all_stats:
            template = jinja2.Template(summary_template)
            yield template.render(headers=['total', 'error', 'details'],
                                  issues=all_stats)
