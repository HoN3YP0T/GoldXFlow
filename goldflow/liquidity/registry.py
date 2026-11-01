"""One book per provider, venue, market, instrument and book kind (Phase 8).

There is deliberately no operation that combines books. Routing an event to a book with a different key raises
`MergedBookError`; creating two books with the same key returns the same book. RPI liquidity (book_kind="rpi") is a
separate key from the regular book.
"""

from __future__ import annotations

from collections.abc import Iterator

from .book import BookKey, LocalOrderBook, MergedBookError

BOOK_KINDS = ("regular", "rpi")


class BookRegistry:
    def __init__(self, max_buffer: int = 20_000, invalidate_on_cross: bool = True):
        self._books: dict[BookKey, LocalOrderBook] = {}
        self.max_buffer, self.invalidate_on_cross = max_buffer, invalidate_on_cross

    def book(self, key: BookKey) -> LocalOrderBook:
        if key.book_kind not in BOOK_KINDS:
            raise ValueError(f"unknown book kind {key.book_kind!r}")
        b = self._books.get(key)
        if b is None:
            b = LocalOrderBook(key, max_buffer=self.max_buffer, invalidate_on_cross=self.invalidate_on_cross)
            self._books[key] = b
        return b

    def keys(self) -> list[BookKey]:
        return sorted(self._books, key=lambda k: k.label)

    def __iter__(self) -> Iterator[LocalOrderBook]:
        return iter(self._books[k] for k in self.keys())

    def __len__(self) -> int:
        return len(self._books)


def assert_same_book(expected: BookKey, got: BookKey) -> None:
    """Refuse to mix data from two books (different venue, instrument, market or book kind)."""
    if expected != got:
        raise MergedBookError(f"refusing to combine book {got.label} with {expected.label}: books are never merged")
