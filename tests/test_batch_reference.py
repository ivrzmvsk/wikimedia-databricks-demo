import io
import json

from wikimedia_batch.transforms import fetch_and_parse_sitematrix


def test_sitematrix_includes_language_and_special_projects(monkeypatch):
    payload = {"sitematrix": {
        "count": 2,
        "0": {"code": "en", "site": [{
            "dbname": "enwiki", "sitename": "Wikipedia",
            "url": "https://en.wikipedia.org",
        }]},
        "specials": [{
            "dbname": "commonswiki", "sitename": "Wikimedia Commons",
            "url": "https://commons.wikimedia.org",
        }],
    }}
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda request: io.BytesIO(json.dumps(payload).encode()),
    )

    rows = {row.wiki_code: row for row in fetch_and_parse_sitematrix()}

    assert set(rows) == {"enwiki", "commonswiki"}
    assert rows["enwiki"].language_code == "en"
    assert rows["commonswiki"].language_code == "special"
