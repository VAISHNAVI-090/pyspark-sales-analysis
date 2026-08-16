"""
compare_runs.py
------------------
Runs basic_transform.py and optimized_transform.py back to back and
prints how long each took, so the optimization claim is backed by an
actual number instead of just an assertion.

Run:
    python src/compare_runs.py

Note: on a small dataset like this one (~20k rows), the difference between
basic and optimized may be small, or the optimized version can even be
slightly slower — caching and repartitioning have their own overhead, and
that overhead only pays off once the data or the number of reuses is large
enough. That's expected and worth noting honestly in the results rather
than hiding it.
"""

import basic_transform
import optimized_transform

if __name__ == "__main__":
    print("Running basic (unoptimized) version...\n")
    basic_time = basic_transform.main()

    print("\nRunning optimized version (cache + repartition)...\n")
    optimized_time = optimized_transform.main()

    print("\n=== Timing Comparison ===")
    print(f"Basic:     {basic_time}s")
    print(f"Optimized: {optimized_time}s")
