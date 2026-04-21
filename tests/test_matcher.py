from src.receipts.pipeline import _tokens


def test_tokens_lowercases_and_drops_tiny():
    assert _tokens("Milk 1L Tnuva") == {"milk", "1l", "tnuva"}


def test_tokens_hebrew():
    # Hebrew tokens pass through the regex (unicode class includes Hebrew range)
    toks = _tokens("חלב תנובה 3%")
    assert "חלב" in toks and "תנובה" in toks


def test_jaccard_intuition():
    a = _tokens("Milk 1L Tnuva")
    b = _tokens("Milk 1L Tnuva 3% fat")
    inter = len(a & b)
    union = len(a | b)
    j = inter / union
    assert j >= 0.5
