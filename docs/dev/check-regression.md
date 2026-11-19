# CBT Regression Detection Tool

`tools/check_regression.py` compares two CBT benchmark archive directories (a baseline and a candidate) and reports whether performance regressions occurred.

---

## Prerequisites

- **Intermediate JSON files:** The tool expects intermediate JSON files in a `visualisation/` directory within each archive.
  - In teuthology runs with a `report:` section in the CBT YAML (e.g., `results_file_root: "output"`), these files are generated automatically.
  - For archives without pre-existing intermediate files, generate them first using `tools/fio_common_output_wrapper.py`.
- **Matching Workloads:** Baseline and candidate runs should use the same or comparable workload configurations.
- *Note:* Automatic generation of missing intermediate files may be added in a post-MVP update.

---

## Usage

```bash
PYTHONPATH=. python3 tools/check_regression.py \
  --baseline <path/to/baseline_archive> \
  --candidate <path/to/candidate_archive> \
  [--weak 0.08] \
  [--strong 0.12] \
  [--no-color]
```

### Options

| Option | Type | Default | Description |
|---|---|---|---|
| `--baseline` | `Path` (required) | — | Path to the baseline CBT archive directory. |
| `--candidate` | `Path` (required) | — | Path to the candidate CBT archive directory. |
| `--weak` | `float` | `0.08` | Fractional worsening threshold for WEAK flags (default: 0.08 / 8%). Note: H3 low-depth latency uses fixed overriding thresholds (25% weak, 35% strong) due to higher noise floor. |
| `--strong` | `float` | `0.12` | Fractional worsening threshold for STRONG flags (default: 0.12 / 12%). |
| `--no-color` | `bool` | `False` | Disable ANSI terminal color codes in output. |

Thresholds are fractions, not percentages: `0.12` means 12%. Values of `1.0` or more are
accepted, since a latency or CPU rise can exceed 100%.

### Exit Codes

| Exit Code | Decision | Meaning |
|---|---|---|
| `0` | **PASS** | No significant regression detected. |
| `1` | **REVIEW** | Possible regression (or multiple weak signals) requiring human review. |
| `2` | **FAIL** | Clear regression detected exceeding the strong threshold. |
| `3` | **ERROR** | Input problem (e.g. missing directory, unparseable JSON, no matching workloads). |

The tool searches each archive recursively for `visualisation/` directories containing
intermediate JSON, so no particular archive layout is required.
