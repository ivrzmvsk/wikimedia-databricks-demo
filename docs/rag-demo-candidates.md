# Articles already in Silver for a RAG demo

Inspected on 2026-10-15 using profile `personal-env`. This was a read-only review;
no ingestion session, embeddings, index sync, or App deployment was started.
The SQL warehouse was initially STOPPED, used for one query and stopped afterward.
The running/stopped state of the user's App was not changed.

## Actual pool

SQL statement `01f1c83c-0b75-13c0-b10a-940c949e098a` read
`workspace.wikimediademo_silver.silver_wikipedia_edits` with these conditions:
`wiki=enwiki`, namespace 0, event_type edit/new, nonempty title.
It found **81 distinct wiki/title pairs across 85 eligible events**. These counts
refer to this filter and current snapshot, not the entire Silver table or Wikipedia.
No Kafka audit fields were used. All 81 title aggregates were returned without
truncation, including first/last event timestamps.

The eligible pool spans `2026-10-11T23:50:48Z` through
`2026-10-15T00:27:16Z`. A covering half-open interval is
`[2026-10-11T23:50:48Z,2026-10-15T00:27:17Z)`.
This is an editorial shortlist from that pool, not the existing count DESC/title
ASC top-10. That deterministic top-10 over the broad interval would select a
different group dominated by sports and biographies. A future curated-demo mode
must record an explicit allowlist without changing the default selection rules.

## Recommended shortlist

Exactly ten public Wikipedia Summary API requests were made locally, sequentially,
with no retries. All returned standard pages with nonempty extracts. Responses
and revisions are kept in the ignored `.state/rag-demo-selection/` directory.
No Wikipedia preview response was written to cloud RAG tables in this review.

| Article from Silver | Why useful | Example question answerable from the inspected summary |
| --- | --- | --- |
| [IPhone 4](https://en.wikipedia.org/wiki/IPhone_4) | Familiar technology; product/date facts | Какой смартфон Apple есть в нашей выборке и когда его представили? |
| [Generation Z](https://en.wikipedia.org/wiki/Generation_Z) | Recognizable topic; terminology and generational relationships | Какое поколение идёт после миллениалов? |
| [Astronomical coordinate systems](https://en.wikipedia.org/wiki/Astronomical_coordinate_systems) | Science; retrieval using a space-related query | Для чего астрономам нужны системы координат? |
| [Marathon (1994 video game)](https://en.wikipedia.org/wiki/Marathon_(1994_video_game)) | Games, Bungie, Macintosh and a space setting | Найди игру про отражение вторжения пришельцев на космический корабль. |
| [Green Day discography](https://en.wikipedia.org/wiki/Green_Day_discography) | Music; richer summary mentioning the breakout album | Какая статья рассказывает о музыкальных альбомах? Какой альбом стал прорывом группы? |
| [Cinéma vérité](https://en.wikipedia.org/wiki/Cin%C3%A9ma_v%C3%A9rit%C3%A9) | Arts; explanatory summary and UTF-8 title | Найди материал о стиле документального кино без привычного закадрового рассказчика. |
| [Ford Fiesta (first generation)](https://en.wikipedia.org/wiki/Ford_Fiesta_(first_generation)) | Cars; straightforward historical product facts | Какая модель автомобиля появилась в 1976 году? |
| [Anne Carson](https://en.wikipedia.org/wiki/Anne_Carson) | Literature; semantic search for a person without naming her | Найди канадскую писательницу, связанную с античной литературой. |
| [Surinam Airways](https://en.wikipedia.org/wiki/Surinam_Airways) | Aviation; already validated end-to-end | Какая авиакомпания базируется в Парамарибо? |

Nine are recommended. The tenth checked candidate,
[Volcanic impacts on the oceans](https://en.wikipedia.org/wiki/Volcanic_impacts_on_the_oceans),
has only a single-sentence extract saying explosive eruptions affect global
climate in several ways. Its title is attractive but the summary does not explain
the mechanisms. Exclude it from the main demo, or use it deliberately to test
refusal to elaborate beyond the available context. Technical validity does not
establish sufficient answer coverage.

The summaries are short: all ten previews, including metadata-free title and
description text, total under 10,000 characters and each fits the current
1,800-character chunk size. That is a text-size observation, not a guarantee
about Free Edition compute quota or indexing time.

## Suggested demo sequence after indexing the shortlist

1. Semantic discovery without a title: “Найди игру про космический корабль”.
   Expected source: Marathon.
2. Direct factual question: “Когда представили iPhone 4?” Expected source: IPhone 4.
3. Different domain: “Какой альбом стал прорывом Green Day?” Expected source:
   Green Day discography; the saved summary names Dookie.
4. Cross-document search: “Какие материалы связаны с космосом?” Candidate sources:
   astronomical coordinates and Marathon. Test actual ranking and coverage after
   indexing; top-k is not an exhaustive inventory.
5. Honest refusal: “Почему редактор внёс конкретную правку?” Summaries do not
   explain historical edits. Another refusal: “Что означает byte_change?” until
   project documentation is separately indexed.

These are proposed questions grounded in inspected summaries, not completed
multi-document retrieval/LLM tests. **The App's corpus remains the original one
article, Surinam Airways.** The other candidates are not available in chat yet.
