# Runtime Config And Globals Reference

This file is the current reference for the live runtime surface in:

- [semantify.py](/c:/Users/User/Desktop/SDM-RDFIzer-Star-Plus/rdfizer_star/semantify.py)
- [tm_levels.py](/c:/Users/User/Desktop/SDM-RDFIzer-Star-Plus/rdfizer_star/tm_levels.py)

It is meant to answer two questions quickly:

- which config variables currently affect runtime behavior or output
- which module-level global variables currently exist and what they are for

## Current Runtime Model

- The live quoted-cache runtime is serialized-string based.
- Same-row caching uses the safe runtime key `(producer_tm_id, (source_name, row_position))` when a runtime token is available.
- Same-row keying falls back to normalized row content only when no runtime token exists.
- Join caching exists for repeated quoted join lookups.
- A branch-only `equivalent_statement_cache` can store row-level statement payloads for planner-detected non-asserted equivalence groups.
- Planner metadata is now part of runtime behavior, not just reporting.
- Planner metadata also includes conservative non-asserted statement-equivalence detection for exact single-statement support TMs.
- Time profiling and memory profiling are independent opt-in features.
- The evaluated quoted cross-source join path is currently scalar-oriented.

## Known Limitation: Composite Quoted Joins

- The current runtime and parser preserve and optimize scalar quoted joins, such as `userId` or `movieId`.
- Composite quoted joins, such as `(userId, movieId)`, are not yet represented end-to-end for quoted subject/object joins.
- This does not affect the current benchmark and thesis scenario set that was checked during development: no multi-field quoted joins were present there.
- Parent-triples-map composite joins are a different path and already have dedicated list-based helpers.

## Extension Path For Later Composite Support

- Preserve repeated quoted `rml:joinCondition` entries in `_build_triples_map_from_query_results(...)` instead of flattening them to one field.
- Let quoted `SubjectMap.child` / `SubjectMap.parent` and quoted `ObjectMap.child` / `ObjectMap.parent` stay as lists when a mapping uses multiple join fields.
- Extend quoted join key/value derivation in [semantify.py](/c:/Users/User/Desktop/SDM-RDFIzer-Star-Plus/rdfizer_star/semantify.py) so composite quoted joins use the same field-order-preserving semantics as `child_list(...)` and `child_list_value(...)`.
- Extend `_planning_join_signature_to_runtime_fields(...)` so eager prebuild can admit composite quoted signatures instead of skipping them.
- Keep the scalar fast path unchanged; add the composite path only behind `isinstance(join_fields, (list, tuple))`.

## Config Variables

### `[default]`

`main_directory`

- Base path used by config interpolation in mapping and output paths.

### `[datasets]` Core Execution

`number_of_datasets`

- Number of dataset sections the current run will process.

`output_folder`

- Directory where generated output files are written.

`remove_duplicate`

- Enables duplicate-elimination behavior in the engine.

`all_in_one_file`

- Chooses whether all dataset outputs are merged into one file or written separately.

`name`

- Base output name used by the all-in-one-file path.

`enrichment`

- Toggles enrichment-mode execution branches.

`large_file`

- Present in configs, but not currently consulted by the live `semantify.py` runtime.

`ordered`

- Controls whether `files_sort(...)` uses explicit source ordering.

`dbType`

- Selects the database backend branch for non-file mappings.

### `[datasets]` Planner, Cache, Debug, And Profiling

`show_planning_context: yes|no`

- Prints the planner summary.
- This only governs output.

`show_cache_sanity: yes|no`

- Prints cache-state and planner-cache sanity summaries.

`show_time_breakdown: yes|no`

- Enables named timing metrics and the final time report.

`show_memory_profile: yes|no`

- Enables RSS checkpoint sampling and the final memory report.

`enable_same_row_quoted_cache: yes|no`

- Enables same-source quoted result reuse through `quoted_triple_cache`.

`enable_join_quoted_cache: yes|no`

- Enables quoted join reuse through `quoted_join_cache`.

`enable_level_cache_flush: yes|no`

- Enables planner-driven conservative cache flushing.

`enable_level_execution_order: yes|no`

- Enables planner-aware asserted TM batching by level.

`enable_equivalent_statement_cache: yes|no`

- Enables planner-detected non-asserted statement-equivalence reuse.
- When disabled, the equivalence cache path fast-exits before alias and owner lookups.

`show_cache_flush_debug: yes|no`

- Prints one line per flush event.

`debug_tm_filters: comma,separated,substrings`

- Enables per-TM debug accumulation for matching triples-map names.

### `[datasetN]`

`name`

- Output file stem for that dataset.

`mapping`

- Mapping file path parsed for that dataset.

## Runtime Globals

### Core Engine State

`id_number`

- Monotonic counter used to assign compact IDs inside `dic_table`.

`g_triples`

- Duplicate-detection structure keyed by dictionary-encoded predicates and term pairs.

`number_triple`

- Triple counter used in array-based execution paths.

`triples`

- Legacy shared triple list kept for older branches.

`duplicate`

- Current duplicate-removal mode loaded from config.

`start_time`

- Whole-run start timestamp.

`user`, `password`, `port`, `host`

- Database connection settings reused by SQL-backed mappings.

`join_table`

- Legacy/shared join lookup table used by parent TM and quoted join branches.

`po_table`

- Predicate-object helper cache built during mapping parsing.

`enrichment`

- Current enrichment-mode flag loaded from config.

`ignore`

- Legacy substitution behavior flag used by string-substitution helpers.

`dic_table`

- Global dictionary from serialized runtime terms to compact IDs.

`base`

- Extracted base IRI for the active mapping.

`blank_message`

- Suppresses repeated blank-node warning output.

`general_predicates`

- Predicates that receive special duplicate-handling treatment.

### Planner And Cache Runtime State

`quoted_triple_cache`

- Same-row quoted producer cache keyed by producer TM and row key.

`quoted_join_cache`

- Join cache keyed by producer TM and normalized join signature/value.

`producer_row_materialization_cache`

- Shared producer-row cache for quoted join-index builds, keyed by producer TM and exact runtime row token.

`equivalent_statement_cache`

- Branch-only row-level cache for exact statement payload reuse across planner-detected non-asserted equivalence groups.
- Keyed by canonical owner TM, matched predicate-object index, and exact row identity.

`enable_equivalent_statement_cache`

- Runtime toggle that enables or disables the equivalence-cache path globally.

`active_planning_context`

- Planner metadata for the dataset currently being semantified.
- Includes exact non-asserted statement-equivalence metadata when a single-POM `NonAssertedTriplesMap` structurally matches a predicate-object statement elsewhere in the same mapping.

`enable_same_row_quoted_cache`

- Live runtime toggle for same-row cache behavior.

`enable_join_quoted_cache`

- Live runtime toggle for join-cache behavior.

`enable_level_cache_flush`

- Live runtime toggle for planner-driven cache flushing.

`enable_level_execution_order`

- Live runtime toggle for planner-aware batching.

### Cache And Planner Counters

`same_row_key_runtime_token_uses`

- Counts how often safe runtime row tokens were used for same-row keys.

`same_row_key_fallback_uses`

- Counts how often same-row keying fell back to normalized row content.

`same_row_cache_hits`

- Counts same-row cache hits.

`same_row_cache_misses`

- Counts same-row cache misses.

`quoted_join_cache_hits`

- Counts quoted join-cache hits.

`quoted_join_cache_misses`

- Counts quoted join-cache misses.

`quoted_join_cache_populates`

- Counts quoted join-cache writes after misses.

`producer_row_materialization_cache_hits`

- Counts reused producer-row materializations during quoted join-index builds.

`producer_row_materialization_cache_misses`

- Counts producer rows that still had to be materialized during quoted join-index builds.

`producer_row_materialization_cache_writes`

- Counts producer-row materialization cache writes.

`quoted_join_index_prebuilds`

- Counts quoted join-index groups eagerly prebuilt for planner-approved high producers.

`quoted_join_index_prebuild_skips`

- Counts planning-selected join signatures skipped by eager prebuild, currently for unsupported composite signatures.

`planner_same_row_cache_skips`

- Counts planner-driven same-row cache skips.

`planner_join_cache_skips`

- Counts planner-driven join-cache skips.

`legacy_quoted_join_reads`

- Counts fallbacks through the older non-cache join path.

`level_cache_flush_events`

- Counts cache flush events.

`flushed_same_row_cache_entries`

- Counts same-row cache entries removed by flushing.

`flushed_join_cache_groups`

- Counts join-cache groups removed by flushing.

`flushed_producer_row_cache_entries`

- Counts producer-row materialization entries removed by flushing.

### Debug And Profiling State

`show_cache_flush_debug`

- Live toggle for flush-event printing.

`cache_flush_debug_events`

- Structured flush events collected for optional debug output.

`debug_tm_filters`

- Active triples-map name filters for TM debug accumulation.

`debug_tm_stats`

- Aggregated TM-level debug counters for matching filters.

`show_time_breakdown`

- Live toggle for timing metrics.

`show_memory_profile`

- Live toggle for memory sampling.

`perf_time_stats`

- Named timing metrics accumulated during the run.

`perf_memory_samples`

- Labeled RSS checkpoints captured during the run.

`perf_memory_peak_bytes`

- Peak sampled process RSS.

`perf_memory_supported`

- Whether process RSS sampling succeeded on the current platform.

## Planning Context Fields

These are the main per-TM fields currently exposed by `build_planning_context(...)`
in [tm_levels.py](/c:/Users/User/Desktop/SDM-RDFIzer-Star-Plus/rdfizer_star/tm_levels.py).

### Existing Core Fields

`role`

- Lightweight planning role such as `asserted_base`, `quoted_consumer`, `quoted_support`, or `mixed`.

`source`

- Producer TM source path used for source-sharing comparisons.

`file_format`

- Producer TM file format used for runtime/source behavior decisions.

`join_fields`

- Flattened child fields involved in quoted or parent-join behavior.

`consumed_by`

- Consumer TM ids that depend on the producer.

`consumer_count`

- Number of unique consumer TMs for that producer.

`quoted_reference_count`

- Count of direct quoted references to the producer across the mapping graph.

`should_cache`

- Planner-level indication that the producer is a cache candidate.

`cache_kind`

- Planner classification of the main cache shape: `row`, `join`, or `None`.

`first_use_level`, `last_use_level`, `flush_after_level`

- Level metadata used for execution ordering and conservative flush decisions.

### Phase 1 Join-Selectivity Fields

`cross_source_consumer_count`

- Number of unique consumer TMs using the producer from a different source.

`same_source_consumer_count`

- Number of unique consumer TMs using the producer from the same source.

`consumers_by_source`

- Per-source count of unique consumer TMs using the producer.

`join_signatures`

- Distinct normalized join child-field signatures through which the producer is referenced.

`join_signature_count`

- Count of distinct join signatures for that producer.

`producer_source_shared_with_consumers`

- Whether at least one consumer shares the producer source.

`expected_join_reuse_strength`

- Conservative heuristic label for join reuse: `high`, `medium`, `low`, or `none`.
- Current Phase 1 refinement treats single-consumer cross-source joins as `low`
  so they stay on the cheaper non-join-cache path.

## Runtime Output Sections

### `TM dependency levels:`

- Printed from `analyze_tm_levels(...)`.
- Shows the quoted dependency layer of each TM.

### `TM planning context:`

- Printed when `show_planning_context: yes`.
- Shows planner metadata such as role, cache kind, and reference counts.

### `Cache foundation sanity:`

- Printed when `show_cache_sanity: yes`.
- Shows cache sizes, counters, planner skip counts, and flush totals.

### `Cache flush debug:`

- Printed when `show_cache_flush_debug: yes`.
- Shows one line per flush event.

### `TM debug:`

- Printed when `debug_tm_filters` is non-empty.
- Shows aggregated row and branch counts for matching TMs.

### `Time breakdown:`

- Printed when `show_time_breakdown: yes`.
- Reports absolute time, relative share, call count, and average time per call for named metrics.

### `Memory profile:`

- Printed when `show_memory_profile: yes`.
- Reports peak RSS and labeled checkpoints together with cache/table sizes.

## Important Historical Non-Features

These older ideas are not part of the live runtime anymore:

- `show_perf_breakdown`
- `quoted_runtime_mode`
- `quoted_cache_payload_mode`
- structural quoted-cache payload runtime
- `_encode_serialized_triples(...)`
- `_decode_serialized_triples(...)`
- reverse-dictionary / `inv_dic_table` runtime support

If they appear in older notes or branch history, treat them as historical context only.
