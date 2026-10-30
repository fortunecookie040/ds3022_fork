# Computing π to 20 Decimal Places

`pi.py` prints π to 20 decimal places (or any number you choose) using **exact integer
arithmetic**, then checks its own result three ways.

```
pi to 20 decimal places: 3.14159265358979323846
```

---

## 1. How to use the code

### Requirements
- Python **3.8+** (it needs `math.isqrt`). Tested on Python 3.13.
- **No third-party packages.** It uses only the standard library (`math`, `argparse`,
  `logging`, `sys`), so you don't need to install anything or create a virtual environment.
- Python is interpreted, so there's **no compile step**.

### Run it
From the `claude_pract/` directory:

```bash
python3 pi.py              # default: 20 decimal places
python3 pi.py --digits 50  # any positive number of decimal places
python3 pi.py -d 1000      # short flag; 1,000 digits takes about 20 ms
```

If you're already inside the project's virtual environment (`pipenv shell` from the repo
root), `python pi.py` works the same way.

### Output
```
pi to 20 decimal places: 3.14159265358979323846
[PASS] Chudnovsky matches Machin's formula
[PASS] Matches published reference value (OEIS A000796)
[PASS] First 15 decimals match math.pi (3.141592653589793)
```

- **Exit code `0`**: every check passed.
- **Exit code `1`**: a check failed or the input was invalid (for example `--digits 0`). A
  `[FAIL]` message goes to stderr.
- **Log file:** every run adds a line to `pi.log` in the current directory with a
  timestamp, both computed values, and the pass/fail result.

---

## 2. Calculation method

### Why not just use `math.pi` or floats?
`math.pi` is a 64-bit IEEE-754 double. It holds only about **15–17 significant digits**, so
digits past the 15th decimal place are not reliable. To get 20 *correct* decimals, the
program can't use floating point anywhere. Everything in `pi.py` is done with Python's
**arbitrary-precision integers**, which never round.

### Fixed-point integer arithmetic
To get *N* decimals, the program computes the integer `floor(π × 10^(N+10))`. The extra
10 **guard digits** soak up the small rounding errors from integer division, so they can't
reach the digits that get printed. The result is then **truncated** (not rounded) down to
*N* decimals, which matches how published digits of π are listed.

### Primary method: the Chudnovsky algorithm
This is the method used for world-record π computations (including the 100-trillion-digit
records):

$$
\frac{1}{\pi} = 12 \sum_{k=0}^{\infty}
\frac{(-1)^k\,(6k)!\,(13591409 + 545140134k)}{(3k)!\,(k!)^3\,640320^{3k+3/2}}
$$

- Each term adds about **14.18 correct digits**. Twenty decimals (+10 guard digits) need only
  **4 terms**.
- **Binary splitting** (`chudnovsky_bs`): instead of summing fractions one at a time, the
  term range `[a, b)` is split in half recursively and combined into three integers
  `P, Q, T`. The whole series collapses into one exact fraction `T/Q`, with no rounding
  until the final division.
- The final formula is `π = 426880 · √10005 · Q / T`, where `√10005` comes from
  `math.isqrt`, an **exact** integer square root.

### Cross-check method: Machin's formula (1706)

$$
\pi = 16\arctan\!\left(\tfrac{1}{5}\right) - 4\arctan\!\left(\tfrac{1}{239}\right)
$$

Each `arctan(1/x)` is evaluated with its Taylor series
`1/x − 1/(3x³) + 1/(5x⁵) − …` in integer arithmetic (`arctan_inv`). This method is
**mathematically independent** of Chudnovsky. It comes from a different identity, uses a
different series, and is computed by different code. If both agree, a bug in one of them
would be very unlikely to go unnoticed.

---

## 3. How the result is verified

The script runs these tests itself every time and fails with exit code `1` if any of them
don't hold:

| # | Test | What it proves |
|---|------|----------------|
| 1 | **Chudnovsky == Machin** | Two unrelated algorithms give the same digits, which rules out an implementation error in either one. |
| 2 | **Matches the published reference** | The output is compared character by character against the first 100 decimals of π from **OEIS A000796** (the standard online reference for integer sequences). This runs for up to 98 decimals. |
| 3 | **Matches `math.pi` to 15 decimals** | A sanity check against the platform's IEEE-754 constant, to the precision a double can actually hold. |

### Checking it yourself with standard references
- **OEIS A000796**: https://oeis.org/A000796, the decimal expansion of π.
- **Known value to 20 decimals:** `3.14159265358979323846` (the 21st decimal is `2`, so
  truncating and rounding give the same answer here).
- **Feynman point:** run `python3 pi.py -d 1000` and look for the well-known run
  `999999` starting at decimal place 762. That's a good spot check at high precision.
- **Optional third-party cross-check** (requires installing `mpmath`, which this project
  doesn't do by default):
  ```python
  from mpmath import mp; mp.dps = 30; print(mp.pi)
  ```

### Why the result is exact
- No floating point is used anywhere in the calculation, so there is no binary rounding
  error.
- The only inexact steps are the integer floor divisions. Each one is off by less than 1
  unit in the last place of a 30-digit number, and the 10 guard digits keep that error far
  away from the 20 printed digits.
- The truncation error of the Chudnovsky series after 4 terms is about 10⁻⁵⁶, which is far
  below 10⁻³⁰.
