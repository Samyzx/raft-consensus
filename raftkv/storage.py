import json
import logging
import sqlite3
from pathlib import Path

from .raft_pb2 import LogEntry


def operation(entry: LogEntry) -> str:
    if entry.noop:
        return "NO-OP"
    fields = [entry.key, entry.value]
    for index, value in enumerate(fields):
        if not value or not value.isprintable() or any(char.isspace() for char in value):
            fields[index] = json.dumps(value, ensure_ascii=False)
    return "SET " + " ".join(fields)


class Storage:
    """Keep Raft's durable state in one transaction and export readable files."""

    def __init__(self, directory: Path, identity: str):
        directory.mkdir(parents=True, exist_ok=True)
        self.directory = directory
        self.db = sqlite3.connect(directory / "state.sqlite3")
        try:
            self.db.execute("PRAGMA journal_mode = WAL")
            self.db.execute("PRAGMA synchronous = FULL")
            with self.db:
                self.db.execute(
                    "CREATE TABLE IF NOT EXISTS metadata ("
                    "id INTEGER PRIMARY KEY CHECK (id = 1), identity TEXT NOT NULL, "
                    "term INTEGER NOT NULL, voted_for TEXT, commit_index INTEGER NOT NULL)"
                )
                self.db.execute(
                    "CREATE TABLE IF NOT EXISTS entries (idx INTEGER PRIMARY KEY, entry BLOB NOT NULL)"
                )
                existing = self.db.execute("SELECT identity FROM metadata WHERE id = 1").fetchone()
                self.restarted = existing is not None
                if existing is None:
                    self.db.execute("INSERT INTO metadata VALUES (1, ?, 0, NULL, 0)", (identity,))
                elif existing[0] != identity:
                    raise ValueError(f"{directory} belongs to a different cluster configuration")
            self.export(include_log=True)
        except BaseException:
            self.db.close()
            raise

    def load(self) -> tuple[int, str | None, int, list[LogEntry]]:
        term, voted_for, commit_index = self.db.execute(
            "SELECT term, voted_for, commit_index FROM metadata WHERE id = 1"
        ).fetchone()
        return term, voted_for, commit_index, self._entries()

    def _entries(self) -> list[LogEntry]:
        rows = self.db.execute("SELECT entry FROM entries ORDER BY idx")
        return [LogEntry.FromString(payload) for (payload,) in rows]

    def save(
        self,
        term: int,
        voted_for: str | None,
        commit_index: int,
        *,
        keep: int | None = None,
        append: tuple[LogEntry, ...] = (),
    ) -> None:
        with self.db:
            self.db.execute(
                "UPDATE metadata SET term = ?, voted_for = ?, commit_index = ? WHERE id = 1",
                (term, voted_for, commit_index),
            )
            if keep is not None:
                self.db.execute("DELETE FROM entries WHERE idx > ?", (keep,))
                self.db.executemany(
                    "INSERT INTO entries VALUES (?, ?)",
                    ((index, entry.SerializeToString()) for index, entry in enumerate(append, keep + 1)),
                )

    def export(self, *, include_log: bool) -> None:
        try:
            term, voted_for, commit_index = self.db.execute(
                "SELECT term, voted_for, commit_index FROM metadata WHERE id = 1"
            ).fetchone()
            self._replace_text(
                "metadata.txt",
                f"Term: {term}\nVotedFor: {voted_for or 'NULL'}\nCommitLength: {commit_index}\n",
            )
            if include_log:
                self._replace_text(
                    "logs.txt", "".join(f"{operation(entry)} {entry.term}\n" for entry in self._entries())
                )
        except (OSError, sqlite3.Error) as error:
            logging.getLogger(__name__).warning("Could not export %s: %s", self.directory, error)

    def _replace_text(self, filename: str, content: str) -> None:
        temp_path = self.directory / f"{filename}.tmp"
        temp_path.write_text(content, encoding="utf-8")
        temp_path.replace(self.directory / filename)

    def close(self) -> None:
        self.db.close()
