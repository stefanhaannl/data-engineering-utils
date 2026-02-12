import re

import unicodedata


class TextSanitizer:

    def __init__(self, preserve_acronyms=False):
        self.preserve_acronyms = preserve_acronyms

    def _normalize(self, text: str) -> str:
        """Normalize Unicode, remove accents, collapse whitespace."""
        text = unicodedata.normalize("NFKD", text)
        text = "".join(c for c in text if not unicodedata.combining(c))
        text = text.replace("…", "")
        text = re.sub(r"\s+", " ", text)
        return text

    def _tokenize(self, text: str):
        """Convert separators, clean punctuation, split into word tokens."""
        text = re.sub(r"[^A-Za-z0-9]+", " ", text)  # keep only A-Z, 0-9
        return [w for w in text.strip().split() if w]

    def sanitize_camel(self, text: str) -> str:
        """Return sanitized CamelCase string.

        If there is more than one separator character (space or other)
        between two alphanumeric tokens, insert an underscore between
        those tokens in the CamelCase result.
        """
        # perform normalization that removes accents but preserves exact separators
        text_norm = unicodedata.normalize("NFKD", text)
        text_norm = "".join(c for c in text_norm if not unicodedata.combining(c))
        text_norm = text_norm.replace("…", "")

        # find all alphanumeric word tokens and detect separators between them
        word_matches = list(re.finditer(r"[A-Za-z0-9]+", text_norm))
        words = [m.group() for m in word_matches]

        if not words:
            return ""

        # determine whether to insert underscore between consecutive words
        underscores_between = []
        for prev, curr in zip(word_matches, word_matches[1:]):
            sep = text_norm[prev.end(): curr.start()]
            # if separator length > 1, we insert an underscore
            underscores_between.append(len(sep) > 1)

        # build result honoring preserve_acronyms and underscore markers
        parts = []
        for i, w in enumerate(words):
            if self.preserve_acronyms:
                part = w if w.isupper() else w.capitalize()
            else:
                part = w.capitalize()
            parts.append(part)
            if i < len(underscores_between) and underscores_between[i]:
                parts.append("_")

        return "".join(parts)
