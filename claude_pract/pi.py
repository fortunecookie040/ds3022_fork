"""Compute pi to N decimal places (default 20) using exact integer arithmetic.

Primary method : Chudnovsky series with binary splitting (~14.18 digits per term).
Cross-check    : Machin's formula  pi = 16*atan(1/5) - 4*atan(1/239).
Verification   : comparison against a published reference value of pi.

Standard library only -- no third-party packages required.
"""
import argparse
import logging
import math
import sys

# Log to a local file alongside the script
logging.basicConfig(
    filename="pi.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# First 100 decimals of pi (OEIS A000796) used as an independent reference
REFERENCE = (
    "3.14159265358979323846264338327950288419716939937510"
    "58209749445923078164062862089986280348253421170679"
)

GUARD = 10  # extra working digits so truncation/rounding errors never reach the output


def chudnovsky_bs(a, b):
    """Binary splitting of the Chudnovsky series over terms [a, b).

    Returns integers (P, Q, T) such that the partial sum equals T / Q
    (up to the constant factor applied in pi_chudnovsky). Splitting the range
    recursively keeps all numbers as exact integers -- no floating point at all.
    """
    if b - a == 1:
        if a == 0:
            p = q = 1
        else:
            p = (6 * a - 5) * (2 * a - 1) * (6 * a - 1)
            q = a ** 3 * 10939058860032000  # 640320^3 / 24
        t = p * (13591409 + 545140134 * a)
        return p, q, -t if a % 2 else t
    m = (a + b) // 2
    p1, q1, t1 = chudnovsky_bs(a, m)
    p2, q2, t2 = chudnovsky_bs(m, b)
    return p1 * p2, q1 * q2, t1 * q2 + p1 * t2


def pi_chudnovsky(digits):
    """Return floor(pi * 10**digits) as an integer via the Chudnovsky formula.

    pi = 426880 * sqrt(10005) * Q / T ; sqrt is taken exactly with math.isqrt.
    """
    one = 10 ** digits
    terms = digits // 14 + 2  # each term contributes ~14.18 correct digits
    _, q, t = chudnovsky_bs(0, terms)
    sqrt_10005 = math.isqrt(10005 * one * one)  # floor(sqrt(10005) * 10**digits)
    return (q * 426880 * sqrt_10005) // t


def arctan_inv(x, one):
    """Return arctan(1/x) * one using the Taylor series with integer arithmetic."""
    total = term = one // x
    x2, n, sign = x * x, 3, -1
    while term:
        term //= x2
        total += sign * (term // n)
        n, sign = n + 2, -sign
    return total


def pi_machin(digits):
    """Return floor(pi * 10**digits) (within a few ulps) via Machin's formula."""
    one = 10 ** digits
    return 16 * arctan_inv(5, one) - 4 * arctan_inv(239, one)


def to_string(pi_int, work_digits, digits):
    """Truncate a scaled pi integer to `digits` decimals and format it as '3.xxxx'."""
    s = str(pi_int // 10 ** (work_digits - digits))
    return f"{s[0]}.{s[1:]}"


def main():
    parser = argparse.ArgumentParser(description="Print pi to N decimal places.")
    parser.add_argument("-d", "--digits", type=int, default=20, help="decimal places (default 20)")
    args = parser.parse_args()

    try:
        if args.digits < 1:
            raise ValueError("--digits must be a positive integer")
        work = args.digits + GUARD

        # Two independent algorithms computed with guard digits, then truncated
        chud = to_string(pi_chudnovsky(work), work, args.digits)
        mach = to_string(pi_machin(work), work, args.digits)
        log.info("digits=%d chudnovsky=%s machin=%s", args.digits, chud, mach)

        print(f"pi to {args.digits} decimal places: {chud}")

        # Test 1: the two independent methods must agree
        assert chud == mach, f"Chudnovsky/Machin mismatch: {chud} vs {mach}"
        print("[PASS] Chudnovsky matches Machin's formula")

        # Test 2: compare against the published reference (when long enough)
        if args.digits <= len(REFERENCE) - 2:
            ref = REFERENCE[: args.digits + 2]
            assert chud == ref, f"Reference mismatch: {chud} vs {ref}"
            print("[PASS] Matches published reference value (OEIS A000796)")

        # Test 3: sanity check against the float math.pi (only ~15 digits are reliable)
        assert chud[:17] == f"{math.pi:.15f}", "Disagrees with math.pi"
        print(f"[PASS] First 15 decimals match math.pi ({math.pi:.15f})")

        log.info("All checks passed")
    except (AssertionError, ValueError) as e:
        log.error("Failure: %s", e)
        print(f"[FAIL] {e}", file=sys.stderr)
        sys.exit(1)
    except Exception:
        log.exception("Unexpected error")
        raise


if __name__ == "__main__":
    main()
