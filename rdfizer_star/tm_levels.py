"""
Utilities for deriving quoted-TM dependency levels from parsed TriplesMap objects.

This module computes dependency-oriented metadata from the already-parsed
`triples_map_list` produced by `mapping_parser()`.

That metadata now feeds runtime decisions in `semantify.py`, including:
- planner summaries
- level-aware execution batching
- cache lifetime metadata
- cache candidate classification

Current planning scope:
- quoted subject dependencies
- quoted object dependencies
- parentTriplesMap metadata (reported, but not used for level computation yet)
- non-assertive TM identification
- cycle detection for quoted-TM dependency chains

Primary in-memory artifacts:
- `tm_levels`: a dictionary keyed by `tm.triples_map_id`
- `planning_context`: a compact dictionary derived from `tm_levels` for
  cache-aware execution planning

The in-memory representation is the primary one because it fits the current
codebase style (`sorted_sources`, `join_table`, `g_triples`) and can be
consumed by later execution logic without file I/O.

Search tags for thesis-oriented planning additions:
- `TM-PLANNING-CTX`
- `TM-PLANNING-RUNTIME`
"""

import json
from collections import deque


def _dedupe(values):
	"""
	Return a list with duplicates removed while preserving first-seen order.

	Why it exists:
	- a TM can reference the same dependency more than once through different
	  predicate-object maps
	- execution planning needs a stable dependency list, not repeated edges

	Input:
	- `values`: iterable of dependency ids, typically TriplesMap ids

	Output:
	- ordered list without duplicates

	This helper is intentionally small because the main analyzer uses repeated
	"collect first, normalize later" steps for quoted and parent dependencies.
	"""
	seen = set()
	result = []
	for value in values:
		if value not in seen:
			seen.add(value)
			result.append(value)
	return result


def _normalize_fingerprint_value(value):
	"""
	Return one stable tuple-friendly representation for planning fingerprints.
	"""
	if value in (None, "None", ""):
		return None
	if isinstance(value, list):
		return tuple(_normalize_fingerprint_value(item) for item in value)
	if isinstance(value, tuple):
		return tuple(_normalize_fingerprint_value(item) for item in value)
	if isinstance(value, set):
		return tuple(sorted(_normalize_fingerprint_value(item) for item in value))
	return str(value)


def _fingerprint_subject_map(subject_map):
	"""
	`TM-PLANNING-CTX`

	Build one exact structural fingerprint for a subject map.
	"""
	return (
		_normalize_fingerprint_value(getattr(subject_map, "subject_mapping_type", None)),
		_normalize_fingerprint_value(getattr(subject_map, "value", None)),
		_normalize_fingerprint_value(getattr(subject_map, "condition", None)),
		_normalize_fingerprint_value(getattr(subject_map, "term_type", None)),
		_normalize_fingerprint_value(getattr(subject_map, "rdf_class", None)),
		_normalize_fingerprint_value(getattr(subject_map, "graph", None)),
		_normalize_fingerprint_value(getattr(subject_map, "child", None)),
		_normalize_fingerprint_value(getattr(subject_map, "parent", None)),
	)


def _fingerprint_predicate_map(predicate_map):
	"""
	`TM-PLANNING-CTX`

	Build one exact structural fingerprint for a predicate map.
	"""
	return (
		_normalize_fingerprint_value(getattr(predicate_map, "mapping_type", None)),
		_normalize_fingerprint_value(getattr(predicate_map, "value", None)),
		_normalize_fingerprint_value(getattr(predicate_map, "condition", None)),
	)


def _fingerprint_object_map(object_map):
	"""
	`TM-PLANNING-CTX`

	Build one exact structural fingerprint for an object map.
	"""
	return (
		_normalize_fingerprint_value(getattr(object_map, "mapping_type", None)),
		_normalize_fingerprint_value(getattr(object_map, "value", None)),
		_normalize_fingerprint_value(getattr(object_map, "datatype", None)),
		_normalize_fingerprint_value(getattr(object_map, "child", None)),
		_normalize_fingerprint_value(getattr(object_map, "parent", None)),
		_normalize_fingerprint_value(getattr(object_map, "term", None)),
		_normalize_fingerprint_value(getattr(object_map, "language", None)),
		_normalize_fingerprint_value(getattr(object_map, "language_map", None)),
	)


def _statement_shape_fingerprint(tm, predicate_object_map):
	"""
	`TM-PLANNING-CTX`

	Build one exact statement-shape fingerprint for a TM/POM combination.
	"""
	return (
		_normalize_fingerprint_value(getattr(tm, "data_source", None)),
		_normalize_fingerprint_value(getattr(tm, "file_format", None)),
		_normalize_fingerprint_value(getattr(tm, "iterator", None)),
		_normalize_fingerprint_value(getattr(tm, "tablename", None)),
		_normalize_fingerprint_value(getattr(tm, "query", None)),
		_fingerprint_subject_map(tm.subject_map),
		_fingerprint_predicate_map(predicate_object_map.predicate_map),
		_fingerprint_object_map(predicate_object_map.object_map),
	)


def _collect_nonasserted_statement_equivalence(triples_map_list, tm_index):
	"""
	`TM-PLANNING-CTX`

	Detect exact statement-shape equivalence between single-POM non-asserted TMs
	and predicate-object maps elsewhere in the mapping set.
	"""
	fingerprint_index = {}
	per_tm = {
		tm_id: {
			"single_pom_non_asserted": False,
			"statement_equivalent_match_count": 0,
			"statement_equivalent_owner_tm_ids": [],
			"statement_equivalent_owner_tm_count": 0,
			"statement_equivalent_multi_pom_owner_count": 0,
			"statement_equivalent_asserted_owner_count": 0,
			"statement_equivalent_matches": [],
			"isolated_support_equivalent_candidate": False,
			"isolated_support_equivalent_target": None,
			"equivalence_group_id": None,
			"equivalence_canonical_owner_tm_id": None,
			"equivalence_canonical_owner_po_index": None,
			"equivalence_canonical_owner_asserted": False,
		}
		for tm_id in tm_index
	}

	for tm in triples_map_list:
		po_count = len(getattr(tm, "predicate_object_maps_list", []))
		for pom_index, predicate_object_map in enumerate(getattr(tm, "predicate_object_maps_list", [])):
			fingerprint = _statement_shape_fingerprint(tm, predicate_object_map)
			entry = {
				"tm_id": str(tm.triples_map_id),
				"tm_name": str(tm.triples_map_name),
				"tm_type": str(getattr(tm, "mappings_type", "None")),
				"po_index": pom_index,
				"po_count": po_count,
				"fingerprint": fingerprint,
			}
			fingerprint_index.setdefault(fingerprint, []).append(entry)

	for tm in triples_map_list:
		tm_id = str(tm.triples_map_id)
		if "NonAssertedTriplesMap" not in str(getattr(tm, "mappings_type", "")):
			continue
		if len(getattr(tm, "predicate_object_maps_list", [])) != 1:
			continue

		per_tm[tm_id]["single_pom_non_asserted"] = True
		fingerprint = _statement_shape_fingerprint(tm, tm.predicate_object_maps_list[0])
		external_matches = [
			entry for entry in fingerprint_index.get(fingerprint, [])
			if entry["tm_id"] != tm_id
		]
		external_owner_ids = _dedupe(entry["tm_id"] for entry in external_matches)
		multi_pom_owners = {
			entry["tm_id"] for entry in external_matches
			if entry["po_count"] > 1
		}
		asserted_owners = {
			entry["tm_id"] for entry in external_matches
			if "NonAssertedTriplesMap" not in entry["tm_type"]
		}

		per_tm[tm_id]["statement_equivalent_match_count"] = len(external_matches)
		per_tm[tm_id]["statement_equivalent_owner_tm_ids"] = external_owner_ids
		per_tm[tm_id]["statement_equivalent_owner_tm_count"] = len(external_owner_ids)
		per_tm[tm_id]["statement_equivalent_multi_pom_owner_count"] = len(multi_pom_owners)
		per_tm[tm_id]["statement_equivalent_asserted_owner_count"] = len(asserted_owners)
		per_tm[tm_id]["statement_equivalent_matches"] = [
			{
				"tm_id": entry["tm_id"],
				"tm_name": entry["tm_name"],
				"po_index": entry["po_index"],
				"po_count": entry["po_count"],
				"tm_type": entry["tm_type"],
			}
			for entry in external_matches
		]

		if len(external_owner_ids) == 1 and len(multi_pom_owners) == 1:
			matching_entry = next(
				(
					entry for entry in external_matches
					if entry["tm_id"] == external_owner_ids[0] and entry["po_count"] > 1
				),
				None
			)
			if matching_entry is not None:
				group_id = f"{matching_entry['tm_id']}::pom::{matching_entry['po_index']}"
				per_tm[tm_id]["isolated_support_equivalent_candidate"] = True
				per_tm[tm_id]["isolated_support_equivalent_target"] = {
					"tm_id": matching_entry["tm_id"],
					"tm_name": matching_entry["tm_name"],
					"po_index": matching_entry["po_index"],
					"po_count": matching_entry["po_count"],
				}
				per_tm[tm_id]["equivalence_group_id"] = group_id
				per_tm[tm_id]["equivalence_canonical_owner_tm_id"] = matching_entry["tm_id"]
				per_tm[tm_id]["equivalence_canonical_owner_po_index"] = matching_entry["po_index"]
				per_tm[tm_id]["equivalence_canonical_owner_asserted"] = (
					"NonAssertedTriplesMap" not in matching_entry["tm_type"]
				)

	equivalence_groups = []
	for fingerprint, entries in fingerprint_index.items():
		if len(entries) < 2:
			continue
		nonasserted_tm_ids = [
			entry["tm_id"] for entry in entries
			if "NonAssertedTriplesMap" in entry["tm_type"]
		]
		if not nonasserted_tm_ids:
			continue
		owner_candidates = [
			entry for entry in entries
			if "NonAssertedTriplesMap" not in entry["tm_type"] and entry["po_count"] > 1
		]
		canonical_owner = owner_candidates[0] if owner_candidates else entries[0]
		equivalence_groups.append({
			"group_id": f"{canonical_owner['tm_id']}::pom::{canonical_owner['po_index']}",
			"entry_count": len(entries),
			"tm_ids": _dedupe(entry["tm_id"] for entry in entries),
			"nonasserted_tm_ids": _dedupe(nonasserted_tm_ids),
			"canonical_owner_tm_id": canonical_owner["tm_id"],
			"canonical_owner_tm_name": canonical_owner["tm_name"],
			"canonical_owner_po_index": canonical_owner["po_index"],
		})

	return per_tm, equivalence_groups


def _normalize_join_signature(join_fields):
	"""
	`TM-PLANNING-CTX`

	Normalize join child fields into one stable tuple shape.

	Why this matters for planning:
	- one producer reused through one join signature is a simpler, stronger join
	  candidate
	- one producer reused through many different signatures is a more fragmented
	  and potentially weaker join-cache candidate

	Examples:
	- `"movie_id"` -> `("movie_id",)`
	- `["user_id", "movie_id"]` -> `("user_id", "movie_id")`
	- `None` / `"None"` -> `()`
	"""
	if join_fields in (None, "None"):
		return ()
	if isinstance(join_fields, (list, tuple)):
		return tuple(str(field) for field in join_fields)
	return (str(join_fields),)


def _collect_join_fields(tm):
	"""
	`TM-PLANNING-CTX`

	Collect join-relevant child fields from quoted and parent-triples-map metadata.
	"""
	join_fields = []

	subject_child = getattr(tm.subject_map, "child", None)
	if subject_child not in (None, "None"):
		if isinstance(subject_child, list):
			join_fields.extend(subject_child)
		else:
			join_fields.append(subject_child)

	for predicate_object_map in tm.predicate_object_maps_list:
		object_map = predicate_object_map.object_map
		if object_map.mapping_type in ("quoted triples map", "parent triples map"):
			child = getattr(object_map, "child", None)
			if child not in (None, "None"):
				if isinstance(child, list):
					join_fields.extend(child)
				else:
					join_fields.append(child)

	return _dedupe(join_fields)


def _collect_join_usage_metadata(triples_map_list, tm_index, tm_levels):
	"""
	`TM-PLANNING-CTX`

	Collect producer-centric join usage metadata from quoted references.

	This is Phase 1 planning support for join selectivity. The goal is to answer
	questions like:
	- does a producer have any real cross-source quoted consumers?
	- are those consumers concentrated behind one join signature or scattered
	  across many different signatures?
	- do the consumers mostly share the producer source or come from foreign
	  sources?

	Why this is useful:
	- same-row optimization is already fairly mature
	- join optimization is where the engine still needs better selectivity
	- this metadata helps runtime avoid treating every `cache_kind=join` producer
	  as equally valuable
	"""
	usage = {
		tm_id: {
			"cross_source_consumers": set(),
			"same_source_consumers": set(),
			"consumers_by_source": {},
			"join_signatures": set(),
		}
		for tm_id in tm_levels
	}

	def _record_quoted_reference(consumer_tm, producer_tm_id, child_fields):
		if producer_tm_id not in usage or producer_tm_id not in tm_index:
			return

		producer_source = str(getattr(tm_index[producer_tm_id], "data_source", ""))
		consumer_source = str(getattr(consumer_tm, "data_source", ""))
		record = usage[producer_tm_id]
		record["consumers_by_source"].setdefault(consumer_source, set()).add(str(consumer_tm.triples_map_id))

		if consumer_source == producer_source:
			record["same_source_consumers"].add(str(consumer_tm.triples_map_id))
		else:
			record["cross_source_consumers"].add(str(consumer_tm.triples_map_id))

		signature = _normalize_join_signature(child_fields)
		if signature:
			record["join_signatures"].add(signature)

	for tm in triples_map_list:
		subject_map = getattr(tm, "subject_map", None)
		if subject_map is not None and "quoted triples map" in str(getattr(subject_map, "subject_mapping_type", "")):
			_record_quoted_reference(tm, getattr(subject_map, "value", None), getattr(subject_map, "child", None))

		for predicate_object_map in getattr(tm, "predicate_object_maps_list", []):
			object_map = getattr(predicate_object_map, "object_map", None)
			if object_map is not None and getattr(object_map, "mapping_type", None) == "quoted triples map":
				_record_quoted_reference(tm, getattr(object_map, "value", None), getattr(object_map, "child", None))

	normalized_usage = {}
	for tm_id, record in usage.items():
		consumers_by_source = {
			source: len(consumer_ids)
			for source, consumer_ids in record["consumers_by_source"].items()
		}
		normalized_usage[tm_id] = {
			"cross_source_consumer_count": len(record["cross_source_consumers"]),
			"same_source_consumer_count": len(record["same_source_consumers"]),
			"consumers_by_source": consumers_by_source,
			"join_signatures": sorted(record["join_signatures"]),
			"join_signature_count": len(record["join_signatures"]),
			"producer_source_shared_with_consumers": len(record["same_source_consumers"]) > 0,
		}

	return normalized_usage


def _infer_join_reuse_strength(cache_kind, consumer_count, cross_source_consumer_count, same_source_consumer_count, join_signature_count):
	"""
	`TM-PLANNING-CTX`

	Estimate whether a join producer looks weak or strong enough to justify the
	current join-cache machinery.

	Heuristic intent:
	- `high`: strong cross-source reuse pattern
	- `medium`: plausible reusable join pattern
	- `low`: planner sees little reason to pay join-cache overhead

	Examples:
	- two foreign consumers reusing one signature -> `high`
	- one foreign consumer with one signature -> `low`
	- no foreign consumers, or fragmented signatures without clear reuse -> `low`

	Phase 1 refinement:
	- single-consumer cross-source joins stay on the cheaper path for now
	- we only keep `medium` when there is already some broader cross-source reuse
	  signal, such as multiple foreign consumers or a larger overall consumer set
	"""
	if cache_kind != "join":
		return "none"

	if cross_source_consumer_count == 0:
		return "low"

	if cross_source_consumer_count >= 2 and join_signature_count <= 1:
		return "high"

	if cross_source_consumer_count >= 2 and join_signature_count <= 2:
		return "medium"

	if consumer_count >= 3 and cross_source_consumer_count >= 1 and join_signature_count <= 1:
		return "medium"

	return "low"


def _infer_tm_role(tm_metadata):
	"""
	`TM-PLANNING-CTX`

	Classify a TM for planning purposes using a lightweight heuristic.
	"""
	if tm_metadata["is_non_assertive"]:
		if tm_metadata["quoted_deps"]:
			return "mixed"
		return "quoted_support"

	if tm_metadata["quoted_deps"]:
		return "quoted_consumer"

	return "asserted_base"


def _infer_cache_kind(tm_id, tm_metadata, tm_index, consumed_by):
	"""
	`TM-PLANNING-CTX`

	Decide whether a TM should use row-cache, join-cache, or no cache.
	"""
	if not consumed_by.get(tm_id):
		return None

	if tm_metadata["join_fields"]:
		return "join"

	producer_source = str(getattr(tm_index[tm_id], "data_source", ""))
	consumer_sources = {
		str(getattr(tm_index[consumer_id], "data_source", ""))
		for consumer_id in consumed_by.get(tm_id, [])
		if consumer_id in tm_index
	}

	if consumer_sources and consumer_sources == {producer_source}:
		return "row"

	return "join"


def _count_direct_quoted_uses(triples_map_list, tm_levels):
	"""
	`TM-PLANNING-CTX`

	Count direct quoted producer references across the mapping graph.

	This keeps multiplicity, unlike `quoted_deps`, so the runtime can distinguish:
	- producer referenced once total -> same-row cache usually not worth populating
	- producer referenced multiple times -> same-row cache may pay off
	"""
	quoted_use_counts = {tm_id: 0 for tm_id in tm_levels}

	for tm in triples_map_list:
		subject_map = getattr(tm, "subject_map", None)
		if subject_map is not None and "quoted triples map" in str(getattr(subject_map, "subject_mapping_type", "")):
			producer_tm_id = getattr(subject_map, "value", None)
			if producer_tm_id in quoted_use_counts:
				quoted_use_counts[producer_tm_id] += 1

		for predicate_object_map in getattr(tm, "predicate_object_maps_list", []):
			object_map = getattr(predicate_object_map, "object_map", None)
			if object_map is not None and getattr(object_map, "mapping_type", None) == "quoted triples map":
				producer_tm_id = getattr(object_map, "value", None)
				if producer_tm_id in quoted_use_counts:
					quoted_use_counts[producer_tm_id] += 1

	return quoted_use_counts


def analyze_tm_levels(triples_map_list):
	"""
	Build the in-memory TM dependency-level structure from parsed TriplesMaps.

	Concept:
	- each parsed TriplesMap already contains enough metadata to identify whether
	  it depends on another TM via `quotedTriplesMap`
	- this function converts that flat list into a dictionary indexed by TM id
	  and assigns each TM a dependency level

	What "level" means here:
	- level 0: no quoted-TM dependencies
	- level 1: depends on one or more level-0 quoted producers
	- level 2: depends on one or more level-1 quoted producers
	- and so on

	What "level" does NOT mean yet:
	- current runtime recursion depth
	- materialization order currently enforced by the engine
	- cache readiness by itself

	Current policy:
	- level computation uses only quoted dependencies
	- `parentTriplesMap` dependencies are still collected and stored, but they are
	  not used to assign levels in this first analysis version

	Input:
	- `triples_map_list`: list of parsed `TriplesMap` objects returned by
	  `mapping_parser()`

	Output:
	- dictionary of the form:

	  {
	    tm_id: {
	      "tm_id": ...,
	      "tm_name": ...,
	      "is_non_assertive": bool,
	      "quoted_subject_dep": tm_id or None,
	      "quoted_object_deps": [tm_id, ...],
	      "parent_tm_deps": [tm_id, ...],
	      "quoted_deps": [tm_id, ...],
	      "level": int or None,
	      "in_cycle": bool
	    }
	  }

	Field meanings:
	- `tm_id`: canonical key used by the current engine
	- `tm_name`: display-oriented name for summaries
	- `is_non_assertive`: whether `mappings_type` contains
	  `NonAssertedTriplesMap`
	- `quoted_subject_dep`: TM referenced by quoted subject, if any
	- `quoted_object_deps`: TMs referenced by quoted objects
	- `parent_tm_deps`: TMs referenced by parentTriplesMap joins
	- `quoted_deps`: normalized union used to compute the level
	- `level`: computed quoted dependency level
	- `in_cycle`: true if the TM remains unresolved after topological traversal

	Algorithm:
	1. Scan each TM and collect dependency metadata.
	2. Build indegree and reverse-edge structures over quoted dependencies.
	3. Run a Kahn-style traversal to assign levels.
	4. Mark unresolved nodes as cyclic.

	Future use:
	- precomputing TM execution layers
	- identifying support TMs before materialization
	- driving quoted-term cache warm-up
	- checking whether a non-assertive TM is upstream of an asserted one
	"""
	report = {}

	for tm in triples_map_list:
		quoted_subject_dep = None
		quoted_object_deps = []
		parent_tm_deps = []

		if "quoted triples map" in tm.subject_map.subject_mapping_type:
			quoted_subject_dep = tm.subject_map.value

		for predicate_object_map in tm.predicate_object_maps_list:
			object_map = predicate_object_map.object_map
			if "quoted triples map" in object_map.mapping_type:
				quoted_object_deps.append(object_map.value)
			elif object_map.mapping_type == "parent triples map":
				parent_tm_deps.append(object_map.value)

		quoted_object_deps = _dedupe(quoted_object_deps)
		parent_tm_deps = _dedupe(parent_tm_deps)

		quoted_deps = []
		if quoted_subject_dep is not None:
			quoted_deps.append(quoted_subject_dep)
		quoted_deps.extend(quoted_object_deps)
		quoted_deps = _dedupe(quoted_deps)

		report[tm.triples_map_id] = {
			"tm_id": tm.triples_map_id,
			"tm_name": tm.triples_map_name,
			"is_non_assertive": "NonAssertedTriplesMap" in str(tm.mappings_type),
			"quoted_subject_dep": quoted_subject_dep,
			"quoted_object_deps": quoted_object_deps,
			"parent_tm_deps": parent_tm_deps,
			"quoted_deps": quoted_deps,
			"level": None,
			"in_cycle": False
		}

	indegree = {tm_id: 0 for tm_id in report}
	reverse_edges = {tm_id: [] for tm_id in report}

	for tm_id, metadata in report.items():
		for dep in metadata["quoted_deps"]:
			if dep in report:
				indegree[tm_id] += 1
				reverse_edges[dep].append(tm_id)

	queue = deque()
	for tm_id, degree in indegree.items():
		if degree == 0:
			report[tm_id]["level"] = 0
			queue.append(tm_id)

	while queue:
		current_tm = queue.popleft()
		current_level = report[current_tm]["level"]

		for dependent_tm in reverse_edges[current_tm]:
			indegree[dependent_tm] -= 1
			if indegree[dependent_tm] == 0:
				dependency_levels = []
				for dep in report[dependent_tm]["quoted_deps"]:
					if dep in report and report[dep]["level"] is not None:
						dependency_levels.append(report[dep]["level"])
				report[dependent_tm]["level"] = max(dependency_levels) + 1 if dependency_levels else current_level + 1
				queue.append(dependent_tm)

	for tm_id, degree in indegree.items():
		if degree > 0:
			report[tm_id]["in_cycle"] = True

	return report


def build_planning_context(triples_map_list, tm_levels=None):
	"""
	`TM-PLANNING-CTX`

	Build a compact planning context on top of the TM level analysis.
	"""
	if tm_levels is None:
		tm_levels = analyze_tm_levels(triples_map_list)

	tm_index = {tm.triples_map_id: tm for tm in triples_map_list}
	consumed_by = {tm_id: [] for tm_id in tm_levels}
	quoted_use_counts = _count_direct_quoted_uses(triples_map_list, tm_levels)
	join_usage_metadata = _collect_join_usage_metadata(triples_map_list, tm_index, tm_levels)
	statement_equivalence, statement_equivalence_groups = _collect_nonasserted_statement_equivalence(
		triples_map_list,
		tm_index
	)

	for consumer_id, metadata in tm_levels.items():
		for dep in metadata["quoted_deps"]:
			if dep in consumed_by:
				consumed_by[dep].append(consumer_id)

	flush_after_level_cache = {}

	def _compute_flush_after_level(tm_id, visiting=None):
		"""
		`TM-PLANNING-CTX`

		Compute the latest level at which a TM may still be needed, including
		transitive quoted consumers.
		"""
		if tm_id in flush_after_level_cache:
			return flush_after_level_cache[tm_id]

		if tm_levels[tm_id]["in_cycle"]:
			flush_after_level_cache[tm_id] = None
			return None

		if visiting is None:
			visiting = set()
		if tm_id in visiting:
			return None

		visiting.add(tm_id)
		candidate_levels = []
		current_level = tm_levels[tm_id]["level"]
		if current_level is not None:
			candidate_levels.append(current_level)

		for consumer_id in consumed_by.get(tm_id, []):
			consumer_flush_level = _compute_flush_after_level(consumer_id, visiting)
			if consumer_flush_level is not None:
				candidate_levels.append(consumer_flush_level)

		visiting.remove(tm_id)
		flush_after_level_cache[tm_id] = max(candidate_levels) if candidate_levels else current_level
		return flush_after_level_cache[tm_id]

	triples_maps = {}
	levels = {}

	for tm_id, metadata in tm_levels.items():
		tm = tm_index[tm_id]
		join_fields = _collect_join_fields(tm)
		role = _infer_tm_role(metadata)
		tm_consumers = _dedupe(consumed_by.get(tm_id, []))
		direct_asserted_consumers = _dedupe(
			consumer_id
			for consumer_id in tm_consumers
			if consumer_id in tm_levels and not tm_levels[consumer_id]["is_non_assertive"]
		)
		direct_non_asserted_consumers = _dedupe(
			consumer_id
			for consumer_id in tm_consumers
			if consumer_id in tm_levels and tm_levels[consumer_id]["is_non_assertive"]
		)
		consumer_levels = [
			tm_levels[consumer_id]["level"]
			for consumer_id in tm_consumers
			if consumer_id in tm_levels and tm_levels[consumer_id]["level"] is not None
		]
		cache_kind = _infer_cache_kind(
			tm_id,
			{
				**metadata,
				"join_fields": join_fields,
			},
			tm_index,
			consumed_by
		)
		should_cache = cache_kind is not None and (
			bool(tm_consumers)
			or role in {"quoted_support", "mixed", "quoted_consumer"}
		)
		join_usage = join_usage_metadata.get(tm_id, {})
		expected_join_reuse_strength = _infer_join_reuse_strength(
			cache_kind,
			len(tm_consumers),
			join_usage.get("cross_source_consumer_count", 0),
			join_usage.get("same_source_consumer_count", 0),
			join_usage.get("join_signature_count", 0),
		)

		first_use_level = min(consumer_levels) if consumer_levels else None
		last_use_level = max(consumer_levels) if consumer_levels else metadata["level"]
		flush_after_level = _compute_flush_after_level(tm_id)

		triples_maps[tm_id] = {
			**metadata,
			"role": role,
			"source": str(getattr(tm, "data_source", "")),
			"file_format": str(getattr(tm, "file_format", "")),
			"join_fields": join_fields,
			"consumed_by": tm_consumers,
			"consumer_count": len(tm_consumers),
			"direct_asserted_consumers": direct_asserted_consumers,
			"direct_asserted_consumer_count": len(direct_asserted_consumers),
			"direct_non_asserted_consumers": direct_non_asserted_consumers,
			"direct_non_asserted_consumer_count": len(direct_non_asserted_consumers),
			"quoted_reference_count": quoted_use_counts.get(tm_id, 0),
			"cross_source_consumer_count": join_usage.get("cross_source_consumer_count", 0),
			"same_source_consumer_count": join_usage.get("same_source_consumer_count", 0),
			"consumers_by_source": join_usage.get("consumers_by_source", {}),
			"join_signatures": join_usage.get("join_signatures", []),
			"join_signature_count": join_usage.get("join_signature_count", 0),
			"producer_source_shared_with_consumers": join_usage.get("producer_source_shared_with_consumers", False),
			"expected_join_reuse_strength": expected_join_reuse_strength,
			"single_pom_non_asserted": statement_equivalence.get(tm_id, {}).get("single_pom_non_asserted", False),
			"statement_equivalent_match_count": statement_equivalence.get(tm_id, {}).get("statement_equivalent_match_count", 0),
			"statement_equivalent_owner_tm_ids": statement_equivalence.get(tm_id, {}).get("statement_equivalent_owner_tm_ids", []),
			"statement_equivalent_owner_tm_count": statement_equivalence.get(tm_id, {}).get("statement_equivalent_owner_tm_count", 0),
			"statement_equivalent_multi_pom_owner_count": statement_equivalence.get(tm_id, {}).get("statement_equivalent_multi_pom_owner_count", 0),
			"statement_equivalent_asserted_owner_count": statement_equivalence.get(tm_id, {}).get("statement_equivalent_asserted_owner_count", 0),
			"statement_equivalent_matches": statement_equivalence.get(tm_id, {}).get("statement_equivalent_matches", []),
			"isolated_support_equivalent_candidate": statement_equivalence.get(tm_id, {}).get("isolated_support_equivalent_candidate", False),
			"isolated_support_equivalent_target": statement_equivalence.get(tm_id, {}).get("isolated_support_equivalent_target", None),
			"equivalence_group_id": statement_equivalence.get(tm_id, {}).get("equivalence_group_id"),
			"equivalence_canonical_owner_tm_id": statement_equivalence.get(tm_id, {}).get("equivalence_canonical_owner_tm_id"),
			"equivalence_canonical_owner_po_index": statement_equivalence.get(tm_id, {}).get("equivalence_canonical_owner_po_index"),
			"equivalence_canonical_owner_asserted": statement_equivalence.get(tm_id, {}).get("equivalence_canonical_owner_asserted", False),
			"should_cache": should_cache,
			"cache_kind": cache_kind,
			"first_use_level": first_use_level,
			"last_use_level": last_use_level,
			"flush_after_level": flush_after_level
		}

		level = metadata["level"]
		if level is not None:
			levels.setdefault(level, []).append(tm_id)

	for level in levels:
		levels[level] = sorted(levels[level], key=lambda tm_id: triples_maps[tm_id]["tm_name"])

	equivalence_group_index = {}
	equivalent_alias_tm_index = {}
	owner_pom_equivalence_index = {}
	owner_tm_equivalence_index = {}
	for group in statement_equivalence_groups:
		group_id = group["group_id"]
		member_flush_levels = [
			triples_maps[tm_id]["flush_after_level"]
			for tm_id in group["tm_ids"]
			if tm_id in triples_maps and triples_maps[tm_id]["flush_after_level"] is not None
		]
		group_flush_after_level = max(member_flush_levels) if member_flush_levels else None
		equivalence_group_index[group_id] = {
			**group,
			"flush_after_level": group_flush_after_level,
		}
		owner_pom_equivalence_index[
			(group["canonical_owner_tm_id"], int(group["canonical_owner_po_index"]))
		] = {
			"group_id": group_id,
			"flush_after_level": group_flush_after_level,
			"member_tm_ids": list(group["tm_ids"]),
			"canonical_owner_tm_id": group["canonical_owner_tm_id"],
			"canonical_owner_po_index": int(group["canonical_owner_po_index"]),
		}
		owner_tm_equivalence_index.setdefault(group["canonical_owner_tm_id"], {})[
			int(group["canonical_owner_po_index"])
		] = owner_pom_equivalence_index[
			(group["canonical_owner_tm_id"], int(group["canonical_owner_po_index"]))
		]

	for tm_id, metadata in triples_maps.items():
		group_id = metadata.get("equivalence_group_id")
		if group_id and group_id in equivalence_group_index:
			metadata["equivalence_group_flush_after_level"] = equivalence_group_index[group_id]["flush_after_level"]
			equivalent_alias_tm_index[tm_id] = {
				"group_id": group_id,
				"canonical_owner_tm_id": metadata.get("equivalence_canonical_owner_tm_id"),
				"canonical_owner_po_index": metadata.get("equivalence_canonical_owner_po_index"),
				"flush_after_level": equivalence_group_index[group_id]["flush_after_level"],
			}
		else:
			metadata["equivalence_group_flush_after_level"] = None

	quoted_execution_order = []
	for level in sorted(levels):
		quoted_execution_order.extend(levels[level])

	return {
		"tm_index": tm_index,
		"triples_maps": triples_maps,
		"levels": levels,
		"quoted_execution_order": quoted_execution_order,
		"statement_equivalence_groups": statement_equivalence_groups,
		"has_statement_equivalence_groups": bool(statement_equivalence_groups),
		"statement_equivalence_group_index": equivalence_group_index,
		"equivalent_alias_tm_index": equivalent_alias_tm_index,
		"owner_pom_equivalence_index": owner_pom_equivalence_index,
		"owner_tm_equivalence_index": owner_tm_equivalence_index,
	}


def format_tm_levels_summary(tm_levels):
	"""
	Render a compact terminal summary for the in-memory TM level dictionary.

	Purpose:
	- make the analysis visible during normal runs without altering execution
	- keep the current engine behavior unchanged while exposing planner metadata

	Input:
	- `tm_levels`: dictionary returned by `analyze_tm_levels()`

	Output:
	- multiline string suitable for `print(...)`

	Format:
	- `L0 | TM_NAME`
	- `L1 | TM_NAME`
	- `Lcycle | TM_NAME` for cyclic/unresolved nodes

	Sorting:
	- resolved levels first
	- lower levels before higher ones
	- names as final tiebreaker
	"""
	lines = ["TM dependency levels:"]
	for tm_id, metadata in sorted(tm_levels.items(), key=lambda item: (item[1]["level"] is None, item[1]["level"], item[1]["tm_name"])):
		level = metadata["level"]
		level_text = "cycle" if metadata["in_cycle"] else str(level)
		lines.append(f"  L{level_text} | {metadata['tm_name']}")
	return "\n".join(lines)


def format_planning_context_summary(planning_context):
	"""
	`TM-PLANNING-CTX`

	Render a compact terminal summary for the planning context.
	"""
	lines = ["TM planning context:"]
	for tm_id in planning_context["quoted_execution_order"]:
		metadata = planning_context["triples_maps"][tm_id]
		cache_text = metadata["cache_kind"] if metadata["should_cache"] else "none"
		join_detail = ""
		if cache_text == "join":
			join_detail = (
				f" | cross={metadata['cross_source_consumer_count']} | "
				f"same={metadata['same_source_consumer_count']} | "
				f"signatures={metadata['join_signature_count']} | "
				f"join_strength={metadata['expected_join_reuse_strength']}"
			)
		equivalence_detail = ""
		if metadata.get("statement_equivalent_match_count", 0) > 0:
			equivalence_detail = (
				f" | stmt_eq={metadata['statement_equivalent_match_count']} "
				f"(owners={metadata['statement_equivalent_owner_tm_count']})"
			)
			if metadata.get("isolated_support_equivalent_candidate"):
				target = metadata.get("isolated_support_equivalent_target") or {}
				equivalence_detail += f" | isolated_alias={target.get('tm_name', target.get('tm_id', 'yes'))}"
		lines.append(
			f"  L{metadata['level']} | {metadata['tm_name']} | role={metadata['role']} | "
			f"cache={cache_text} | uses={metadata['consumer_count']}{join_detail}{equivalence_detail}"
		)
	if planning_context.get("statement_equivalence_groups"):
		lines.append(
			f"  statement_equivalence_groups={len(planning_context['statement_equivalence_groups'])}"
		)
	return "\n".join(lines)


def write_tm_levels(triples_map_list, output_path):
	"""
	Export the analyzed TM level structure to JSON.

	This function is currently a debugging/reporting helper, not part of the
	normal execution path. The engine now keeps `tm_levels` in memory and prints a
	compact summary instead.

	Why keep this function:
	- useful for thesis artifacts and offline inspection
	- useful for regression snapshots while refining the dependency model

	Input:
	- `triples_map_list`: parsed TMs
	- `output_path`: file path for the JSON dump

	Output:
	- returns the same dictionary as `analyze_tm_levels()`
	- writes the following JSON shape:

	  {
	    "triples_maps": {
	      "tm_id": { ...metadata... }
	    }
	  }
	"""
	report = analyze_tm_levels(triples_map_list)
	with open(output_path, "w", encoding="utf-8") as output_file:
		json.dump({"triples_maps": report}, output_file, indent=2)
	return report
