# Curated 40-article demo

The subsequent release adds 12 project documents to these forty articles; see
[the project documentation guide](rag-project-documents.md) for the current App collection.

The collection keeps the original ten demo articles and adds thirty articles from
existing English Wikipedia Silver events. The exact selection and UTC event
window are in [`rag/demo_corpus.json`](../rag/demo_corpus.json).
No new Wikimedia ingestion session or project-document ingestion is required.

The interface is English. Answers follow the question's language. Each supported
answer links to its source and shows the summary fetch time and revision.
The sidebar's Sources panel contains the full collection.

## Questions to try

| Capability | Question |
| --- | --- |
| Semantic search without a title | Find a storage device with no moving parts. |
| Unusual fact | Find a dessert made with canned tomato soup. Why is soup added? |
| Multiple sources | Compare natto and tomato soup cake: what are they made from? |
| Space | Which system in this collection has a planet orbiting two stars? |
| Scientific definition | What are the three IAU conditions for a planet, and which system does this definition apply to? |
| AI use case | Find a company using AI to analyze vessel movements. What does it do? |
| Biology | What does nitrogenase do? |
| DNA technology | What is transmission electron microscopy DNA sequencing? |
| Games | What can players create and share in Super Mario Maker 2? |
| Music | Which bands is Damon Albarn known for? |
| History | Who was Mansa Musa? |
| Participation | What is a citizens' assembly and how are its members selected? |
| Maps | What is the Flyover feature in Apple Maps? |
| Russian answer | В каком фильме люди используют виртуальный мир OASIS и кто его режиссёр? |
| Honest refusal | Why did the editor change the Mansa Musa article? |
| Missing detail | Give the exact Tomato soup cake recipe in grams for every ingredient. |

These are retrieved page summaries, not complete articles, edit diffs or historical
revision bodies. They cannot establish an editor's motive, exact platform rankings,
or facts absent from the retrieved text. A successful sample evaluation is not an
accuracy guarantee for arbitrary questions.

## Selected articles

1. Solid-state drive
2. Samsung Galaxy Z Fold 5
3. IAU definition of planet
4. Kepler-16
5. K2-155d
6. Natto
7. Tomato soup cake
8. Retreat of glaciers since 1850
9. Ready Player One (film)
10. Windward (maritime AI company)
11. Astronomical coordinate systems
12. Marathon (1994 video game)
13. IPhone 4
14. Generation Z
15. Green Day discography
16. Cinéma vérité
17. Anne Carson
18. Ford Fiesta (first generation)
19. Surinam Airways
20. Ytterbium
21. Transmission electron microscopy DNA sequencing
22. Gray whale
23. Great egret
24. Walking fern
25. Nitrogenase
26. Calypso music
27. Damon Albarn
28. Peter Gabriel
29. Super Mario Maker 2
30. Sierra Creative Interpreter
31. IAI Westwind
32. Cars (film)
33. Mansa Musa
34. Sentinelese
35. Appalachia
36. Balkans
37. Citizens' assembly
38. Error-driven learning
39. Flyover (Apple Maps)
40. Theia Mons

## Preparation evidence

- Profile `personal-env`, target `ivan_personal`; existing RAG resources reused.
- Preparation run `137766732687271` completed SUCCESS.
- Event interval: `[2026-10-11T23:50:48+00:00, 2026-10-15T03:01:48+00:00)`.
- The forty selected titles have 46 eligible Silver events in that interval.
- Exactly 40 canonical documents and 40 chunks were prepared; all original ten
  articles are retained.
- Eleven summaries were reused from the Delta cache; 29 HTTP requests fetched
  the remaining summaries. Cache reads and successful-response writes are batched.
- Corpus `2448ad308f38b0bbb0f000a2`, generation `1bfbdf2ddd031b7d80acbfec`.
- The local suite passed: 124 tests. Flake8 and Python compilation also passed.
- Machine-readable run reports are stored locally under `.state/rag-demo-40/`.

## Release checks

Managed index update `76536638-9ece-4faf-afe9-8b7824b93eac` completed. The index
contains 51 rows because the previous one-article and ten-article corpora remain;
retrieval filters both `corpus_id` and `generation`, so this App uses only the
forty-document collection. SQL statement `01f1c871-1cc2-153d-b847-a793462d301d`
confirmed 40 chunks, 40 distinct documents and one generation for this corpus.

During real evaluation we found and corrected two response issues: English
questions sometimes received Russian answers, and a Russian film answer invented
a translation of Ready Player One. The eval now checks English/Russian response
language and preserves that original film title. Historical intermediate eval
reports are retained locally; use the release report for the final outcome.

Final real-service evaluation: **30/30 passed** with the final prompt, including
expected source citations, supported/refused outcomes, English/Russian response
checks, the original film title, and two-source comparisons. See
`rag/demo_eval.json` and local `.state/rag-demo-40/eval-release.json`.
The answers were also reviewed against the stored summaries; a sample pass does
not guarantee every future model response.

The final App deployment `01f1c8725df71105ba96d23f7e302fda` reached SUCCEEDED.
Its own OAuth service principal retrieved the SSD source and generated a supported
English answer (`APP_SP_SMOKE` in `app-release-logs.txt`); the authenticated
Streamlit health endpoint returned HTTP 200 / `ok`.

The App is stopped after verification at the user's request. Its deployed source
and the corpus remain available for the next demo. To start and stop it:

```sh
databricks apps start wikimedia-assistant-dev -p personal-env
databricks apps stop wikimedia-assistant-dev -p personal-env
```

App URL: <https://wikimedia-assistant-dev-7474655630219513.aws.databricksapps.com>

Existing shared search/model resources were reused. No new search endpoint,
model endpoint, ingestion session, subscription change or scheduled job was added
for this expansion. Project Markdown files in `rag_docs/` were not ingested.

Final API check confirmed `compute_status=STOPPED`; evidence:
`.state/rag-demo-40/app-release-stopped.json`.
