"""Build an offline Raft benchmark bundle for Ubuntu 24.04 x86-64."""

import argparse
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

from raftkv.config import load_cluster


ROOT = Path(__file__).resolve().parents[1]


def build(cluster: Path, output: Path) -> None:
    load_cluster(cluster)
    with tempfile.TemporaryDirectory(prefix="raft-wheelhouse-") as temporary:
        wheelhouse = Path(temporary)
        subprocess.run([
            sys.executable, "-m", "pip", "download", "--only-binary=:all:",
            "--platform", "manylinux2014_x86_64", "--platform", "manylinux_2_28_x86_64",
            "--python-version", "3.12", "--dest", str(wheelhouse),
            "-r", str(ROOT / "requirements.txt"),
        ], check=True)
        with tarfile.open(output, "w:gz") as archive:
            archive.add(cluster, arcname="cluster.json")
            for source in sorted((ROOT / "raftkv").glob("*.py")):
                archive.add(source, arcname=f"raftkv/{source.name}")
            for name in ("bootstrap.sh", "raft-benchmark.service"):
                normalized = wheelhouse / name
                content = (ROOT / "cloud" / name).read_text(encoding="utf-8")
                normalized.write_text(content, encoding="utf-8", newline="\n")
                archive.add(normalized, arcname=f"cloud/{name}")
            for wheel in sorted(wheelhouse.glob("*.whl")):
                archive.add(wheel, arcname=f"wheels/{wheel.name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cluster", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    build(args.cluster, args.output)


if __name__ == "__main__":
    main()
