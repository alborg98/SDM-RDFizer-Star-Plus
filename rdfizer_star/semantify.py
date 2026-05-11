import os
import re
import csv
import sys
import uuid
import rdflib
import urllib
import getopt
import subprocess
from rdflib.plugins.sparql import prepareQuery
from configparser import ConfigParser, ExtendedInterpolation
import traceback
from mysql import connector
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import time
import json
import xml.etree.ElementTree as ET
import psycopg2
import types
import pandas as pd
from .functions import *
from .tm_levels import analyze_tm_levels, build_planning_context, format_tm_levels_summary, format_planning_context_summary

try:
	from triples_map import TriplesMap as tm
except:
	from .triples_map import TriplesMap as tm	

# Work in the rml:sqlQuery (change mapping parser query, add sqlite3 support, etc)
# Work in the "when subject is empty" thing (uuid.uuid4(), dependency graph over the ) 

# This counter assigns compact IDs inside the global RDF term dictionary.
global id_number
id_number = 0
# This table stores duplicate-detection state keyed by predicate dictionary IDs.
global g_triples 
g_triples = {}
# This counter tracks emitted triples for array-based execution paths.
global number_triple
number_triple = 0
# This list keeps the legacy shared triple buffer used by older branches.
global triples
triples = []
# This flag mirrors the config choice for duplicate removal behavior.
global duplicate
duplicate = ""
# This timestamp anchors whole-run runtime reporting.
global start_time
start_time = time.time()
# These connection settings are reused by database-backed mappings.
global user, password, port, host
user, password, port, host = "", "", "", ""
# This join table stores materialized parent and quoted lookup groups.
global join_table 
join_table = {}
# This staging table accumulates planner-guided quoted join indexes while a
# producer TM is being scanned through the top-level exact-row path.
global staged_join_table
staged_join_table = {}
# This table caches predicate-object helper structures built during parsing.
global po_table
po_table = {}
# This flag mirrors whether enrichment mode is enabled for the current run.
global enrichment
enrichment = ""
# This flag preserves the engine's legacy substitution-ignore behavior.
global ignore
ignore = "yes"
# This dictionary maps serialized runtime terms to compact IDs.
global dic_table
dic_table = {}
# This cache stores canonical annotated results by producer TM and exact row token.
global annotated_result_cache
annotated_result_cache = {}
# This table records which runtime path first populated each annotated-result entry.
global annotated_result_cache_origins
annotated_result_cache_origins = {}
# This cache stores canonical owner-POM statement payloads for alias/owner reuse.
global equivalent_statement_cache
equivalent_statement_cache = {}
# This groups equivalence cache entries by canonical owner-POM group.
global equivalent_statement_cache_groups
equivalent_statement_cache_groups = {}
# This cache stores canonical owner-POM join payloads by join signature/value.
global equivalent_join_cache
equivalent_join_cache = {}
# This groups join-equivalence cache entries by canonical owner-POM group.
global equivalent_join_cache_groups
equivalent_join_cache_groups = {}
# This staging table accumulates row-token equivalence join indexes while a
# source is already being scanned through a safe full-row execution path.
global staged_equivalent_join_cache
staged_equivalent_join_cache = {}
# This variable holds the active planner metadata for the current dataset.
global active_planning_context
active_planning_context = None
# This toggle enables same-row quoted cache reuse in runtime code paths.
global enable_same_row_quoted_cache
enable_same_row_quoted_cache = False
# This threshold controls how many quoted references a producer needs before
# same-row cache privilege is granted. `2` means `>1`, `1` means `>0`.
global same_row_cache_min_reference_count
same_row_cache_min_reference_count = 2
# This toggle enables quoted join-cache reuse in runtime code paths.
global enable_join_quoted_cache
enable_join_quoted_cache = False
# This toggle controls whether planner-selected join indexes are prebuilt
# eagerly before execution starts.
global enable_eager_join_prebuild
enable_eager_join_prebuild = True
# This toggle enables owner-POM statement equivalence reuse.
global enable_equivalent_statement_cache
enable_equivalent_statement_cache = True
# This toggle enables conservative level-based cache flushing.
global enable_level_cache_flush
enable_level_cache_flush = False
# This toggle enables planner-aware asserted TM execution batching.
global enable_level_execution_order
enable_level_execution_order = False
# This counter tracks safe runtime-token same-row key usage.
global same_row_key_runtime_token_uses
same_row_key_runtime_token_uses = 0
# This counter tracks fallback same-row key normalization usage.
global same_row_key_fallback_uses
same_row_key_fallback_uses = 0
# This toggle enables per-event cache flush debug printing.
global show_cache_flush_debug
show_cache_flush_debug = False
# This counter tracks successful same-row quoted cache reuse.
global same_row_cache_hits
same_row_cache_hits = 0
# This counter tracks same-row quoted cache misses before rebuilding.
global same_row_cache_misses
same_row_cache_misses = 0
# This counter tracks exact annotated-result cache hits across quoted paths.
global annotated_result_cache_hits
annotated_result_cache_hits = 0
# This counter tracks exact annotated-result cache misses across quoted paths.
global annotated_result_cache_misses
annotated_result_cache_misses = 0
# This counter tracks exact annotated-result cache writes across quoted paths.
global annotated_result_cache_writes
annotated_result_cache_writes = 0
# This counter tracks annotated-result cache writes originating from same-row recursion.
global annotated_result_cache_same_row_writes
annotated_result_cache_same_row_writes = 0
# This counter tracks annotated-result cache writes originating from join-index builds.
global annotated_result_cache_join_materialization_writes
annotated_result_cache_join_materialization_writes = 0
# This counter tracks annotated-result cache hits from same-row recursion.
global annotated_result_cache_same_row_hits
annotated_result_cache_same_row_hits = 0
# This counter tracks annotated-result cache hits while materializing join indexes.
global annotated_result_cache_join_materialization_hits
annotated_result_cache_join_materialization_hits = 0
# This counter tracks annotated-result cache hits during join-result resolution.
global annotated_result_cache_join_resolution_hits
annotated_result_cache_join_resolution_hits = 0
# This counter tracks exact-row accesses that reused a payload first built on a different path.
global annotated_result_cache_cross_path_hits
annotated_result_cache_cross_path_hits = 0
# This counter tracks join-resolution misses where a row token existed but the unified cache entry did not.
global annotated_result_cache_join_resolution_misses
annotated_result_cache_join_resolution_misses = 0
# This counter tracks owner/alias equivalence cache hits.
global equivalent_statement_cache_hits
equivalent_statement_cache_hits = 0
# This counter tracks owner/alias equivalence cache misses.
global equivalent_statement_cache_misses
equivalent_statement_cache_misses = 0
# This counter tracks owner/alias equivalence cache populates.
global equivalent_statement_cache_populates
equivalent_statement_cache_populates = 0
# This counter tracks owner-side owner-POM cache populates.
global equivalent_statement_cache_owner_populates
equivalent_statement_cache_owner_populates = 0
# This counter tracks alias-side whole-TM cache populates.
global equivalent_statement_cache_alias_populates
equivalent_statement_cache_alias_populates = 0
# This counter tracks alias late-comer whole-TM skips through equivalence.
global equivalent_statement_cache_alias_hits
equivalent_statement_cache_alias_hits = 0
# This counter tracks owner late-comer POM skips through equivalence.
global equivalent_statement_cache_owner_hits
equivalent_statement_cache_owner_hits = 0
# This counter tracks join-side equivalence cache hits.
global equivalent_join_cache_hits
equivalent_join_cache_hits = 0
# This counter tracks join-side equivalence cache misses.
global equivalent_join_cache_misses
equivalent_join_cache_misses = 0
# This counter tracks join-side equivalence cache populates.
global equivalent_join_cache_populates
equivalent_join_cache_populates = 0
# This counter tracks quoted join-index groups prebuilt from planning metadata.
global quoted_join_index_prebuilds
quoted_join_index_prebuilds = 0
# This counter tracks planning-selected join signatures skipped by prebuild.
global quoted_join_index_prebuild_skips
quoted_join_index_prebuild_skips = 0
# This counter tracks planner-driven same-row cache skips.
global planner_same_row_cache_skips
planner_same_row_cache_skips = 0
# This counter tracks planner-driven quoted join memo skips.
global planner_join_cache_skips
planner_join_cache_skips = 0
# This counter tracks how many producer-level flush events ran.
global level_cache_flush_events
level_cache_flush_events = 0
# This counter tracks how many same-row cache entries were flushed.
global flushed_same_row_cache_entries
flushed_same_row_cache_entries = 0
# This counter tracks how many join-table groups were flushed.
global flushed_join_table_groups
flushed_join_table_groups = 0
# This counter tracks how many exact annotated-result cache entries were flushed.
global flushed_annotated_result_cache_entries
flushed_annotated_result_cache_entries = 0
# This counter tracks flushed equivalence statement entries.
global flushed_equivalent_statement_cache_entries
flushed_equivalent_statement_cache_entries = 0
# This counter tracks flushed equivalence join entries.
global flushed_equivalent_join_cache_entries
flushed_equivalent_join_cache_entries = 0
# This set tracks which equivalence groups were already flushed.
global flushed_equivalent_statement_group_ids
flushed_equivalent_statement_group_ids = set()
# This list stores structured flush events for optional debug printing.
global cache_flush_debug_events
cache_flush_debug_events = []
# These substring filters select which TMs get extra debug accumulation.
global debug_tm_filters
debug_tm_filters = []
# This table stores aggregated per-TM debug counters for matching filters.
global debug_tm_stats
debug_tm_stats = {}
# Profiling controls are intentionally split so timing and memory sampling can
# be enabled independently. This keeps measurement scope explicit and avoids a
# debug/reporting flag from implicitly changing unrelated profiler behavior.
# This toggle enables the opt-in timing profiler output.
global show_time_breakdown
show_time_breakdown = False
# This toggle enables the opt-in memory checkpoint profiler output.
global show_memory_profile
show_memory_profile = False
# This table accumulates named timing metrics for the current run.
global perf_time_stats
perf_time_stats = {}
# This list stores labeled memory checkpoints for the current run.
global perf_memory_samples
perf_memory_samples = []
# This value stores the peak observed process RSS across checkpoints.
global perf_memory_peak_bytes
perf_memory_peak_bytes = 0
# This flag records whether RSS sampling worked on the current platform.
global perf_memory_supported
perf_memory_supported = False
# This string stores the extracted base IRI of the active mapping.
global base
base = ""
# This flag suppresses repeated blank-node warning messages.
global blank_message
blank_message = True
# This table lists predicates that need special duplicate-handling treatment.
global general_predicates
general_predicates = {"http://www.w3.org/2000/01/rdf-schema#subClassOf":"",
						"http://www.w3.org/2002/07/owl#sameAs":"",
						"http://www.w3.org/2000/01/rdf-schema#seeAlso":"",
						"http://www.w3.org/2000/01/rdf-schema#subPropertyOf":""}


def _record_time_metric(metric_name, elapsed_seconds):
	"""
	PROFILING-TIME

	Accumulate elapsed time for a named runtime metric.

	This helper is a no-op unless `show_time_breakdown` is enabled, so normal
	runs do not pay repeated dictionary-update overhead for timing counters.
	"""
	if not show_time_breakdown:
		return
	stats = perf_time_stats.setdefault(metric_name, {"seconds": 0.0, "calls": 0})
	stats["seconds"] += elapsed_seconds
	stats["calls"] += 1


@contextmanager
def _time_metric(metric_name):
	"""
	PROFILING-TIME

	Context manager wrapper for timing a code region under a named metric.

	Using a context manager keeps the instrumentation readable around the new
	cache/planner branches and guarantees the timing update happens even if the
	region exits via an exception.
	"""
	if not show_time_breakdown:
		yield
		return
	start = time.perf_counter()
	try:
		yield
	finally:
		_record_time_metric(metric_name, time.perf_counter() - start)


def _get_process_memory_bytes():
	"""
	PROFILING-MEMORY

	Return the current process RSS/working-set size in bytes when a supported
	platform-specific mechanism is available.

	Current implementation:
	- Windows: `GetProcessMemoryInfo` working-set size
	- Linux: `/proc/self/statm` resident pages

	The function returns `None` when sampling is unsupported or a probe fails.
	That keeps memory profiling optional and non-fatal.
	"""
	try:
		if os.name == "nt":
			import ctypes
			from ctypes import wintypes

			class PROCESS_MEMORY_COUNTERS_EX(ctypes.Structure):
				_fields_ = [
					("cb", wintypes.DWORD),
					("PageFaultCount", wintypes.DWORD),
					("PeakWorkingSetSize", ctypes.c_size_t),
					("WorkingSetSize", ctypes.c_size_t),
					("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
					("QuotaPagedPoolUsage", ctypes.c_size_t),
					("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
					("QuotaNonPagedPoolUsage", ctypes.c_size_t),
					("PagefileUsage", ctypes.c_size_t),
					("PeakPagefileUsage", ctypes.c_size_t),
					("PrivateUsage", ctypes.c_size_t),
				]

			kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
			psapi = ctypes.WinDLL("psapi", use_last_error=True)
			kernel32.GetCurrentProcess.restype = wintypes.HANDLE
			psapi.GetProcessMemoryInfo.argtypes = [
				wintypes.HANDLE,
				ctypes.POINTER(PROCESS_MEMORY_COUNTERS_EX),
				wintypes.DWORD,
			]
			psapi.GetProcessMemoryInfo.restype = wintypes.BOOL

			process = kernel32.GetCurrentProcess()
			counters = PROCESS_MEMORY_COUNTERS_EX()
			counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS_EX)
			if psapi.GetProcessMemoryInfo(process, ctypes.byref(counters), counters.cb):
				return int(counters.WorkingSetSize)
			return None
		if sys.platform.startswith("linux"):
			with open("/proc/self/statm", "r", encoding="utf-8") as statm_file:
				fields = statm_file.read().split()
			if len(fields) > 1:
				page_size = os.sysconf("SC_PAGE_SIZE")
				return int(fields[1]) * int(page_size)
			return None
	except Exception:
		return None
	return None


def _capture_memory_sample(label):
	"""
	PROFILING-MEMORY

	Record one labeled memory sample together with lightweight cache-size
	context.

	The stored cache/table sizes make the RSS samples easier to interpret during
	analysis without requiring a second debug mode.
	"""
	global perf_memory_peak_bytes
	global perf_memory_supported

	if not show_memory_profile:
		return

	rss_bytes = _get_process_memory_bytes()
	if rss_bytes is None:
		return

	perf_memory_supported = True
	perf_memory_peak_bytes = max(perf_memory_peak_bytes, rss_bytes)
	perf_memory_samples.append({
		"label": label,
		"rss_bytes": rss_bytes,
		"annotated_result_cache_entries": len(annotated_result_cache),
		"join_table_groups": len(join_table),
		"dictionary_entries": len(dic_table),
	})


def _format_bytes(byte_count):
	"""
	PROFILING-MEMORY

	Render a byte count in a compact human-readable unit for debug output.
	"""
	if byte_count is None:
		return "n/a"
	units = ["B", "KiB", "MiB", "GiB", "TiB"]
	value = float(byte_count)
	for unit in units:
		if value < 1024.0 or unit == units[-1]:
			return f"{value:.2f} {unit}"
		value /= 1024.0


def format_time_breakdown(total_runtime_seconds):
	"""
	PROFILING-TIME

	Render the aggregated timing report for the current run.

	Each metric includes:
	- absolute elapsed time
	- relative share of the whole run
	- number of timed calls
	- average time per call
	"""
	lines = ["Time breakdown:"]
	if not perf_time_stats:
		lines.append("  no time metrics recorded")
		return "\n".join(lines)

	lines.append(f"  total_runtime={total_runtime_seconds:.6f} seconds")
	for metric_name in sorted(perf_time_stats):
		stats = perf_time_stats[metric_name]
		seconds = stats["seconds"]
		calls = stats["calls"]
		share = (seconds / total_runtime_seconds * 100.0) if total_runtime_seconds > 0 else 0.0
		avg_ms = (seconds / calls * 1000.0) if calls > 0 else 0.0
		lines.append(
			f"  {metric_name}: {seconds:.6f} seconds | {share:.2f}% | calls={calls} | avg={avg_ms:.3f} ms"
		)
	return "\n".join(lines)


def format_memory_profile():
	"""
	PROFILING-MEMORY

	Render the memory-sampling report for the current run.

	The report is intentionally checkpoint-based instead of allocation-trace-based
	so it stays cheap enough for opt-in runtime diagnostics on the engine.
	"""
	lines = ["Memory profile:"]
	if not show_memory_profile:
		lines.append("  memory profiling disabled")
		return "\n".join(lines)
	if not perf_memory_supported:
		lines.append("  process RSS sampling not supported on this platform")
		return "\n".join(lines)

	lines.append(f"  peak_rss={_format_bytes(perf_memory_peak_bytes)}")
	if not perf_memory_samples:
		lines.append("  no memory samples recorded")
		return "\n".join(lines)

	for sample in perf_memory_samples:
		lines.append(
			f"  {sample['label']}: rss={_format_bytes(sample['rss_bytes'])} | "
			f"annotated_result_cache_entries={sample['annotated_result_cache_entries']} | "
			f"join_table_groups={sample['join_table_groups']} | "
			f"dic_entries={sample['dictionary_entries']}"
		)
	return "\n".join(lines)

def release_PTT(triples_map,predicate_list):
	for po in triples_map.predicate_object_maps_list:
		if po.predicate_map.value in general_predicates:
			if po.predicate_map.value in predicate_list:
				predicate_list[po.predicate_map.value + "_" + po.object_map.value] -= 1
				if predicate_list[po.predicate_map.value + "_" + po.object_map.value] == 0:
					predicate_list.pop(po.predicate_map.value + "_" + po.object_map.value)
					resource = "<" + po.predicate_map.value + ">" + "_" + po.object_map.value
					if resource in dic_table:
						if dic_table[resource] in g_triples:
							g_triples.pop(dic_table[resource])
		else:
			if po.predicate_map.value in predicate_list:
				predicate_list[po.predicate_map.value] -= 1
				if predicate_list[po.predicate_map.value] == 0:
					predicate_list.pop(po.predicate_map.value)
					resource = "<" + po.predicate_map.value + ">"
					if resource in dic_table:
						if dic_table[resource] in g_triples:
							g_triples.pop(dic_table[resource])
	if triples_map.subject_map.rdf_class != None:
		for rdf_type in triples_map.subject_map.rdf_class:
			resource = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" + "_" + "<{}>".format(rdf_type)
			if resource in predicate_list:
				predicate_list[resource] -= 1
				if predicate_list[resource] == 0:
					predicate_list.pop(resource)
					rdf_class = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" + "_" + "<{}>".format(rdf_type)
					if rdf_class in dic_table:
						if dic_table[rdf_class] in g_triples:
							g_triples.pop(dic_table[rdf_class])
	return predicate_list

def dictionary_table_update(resource):
	if resource not in dic_table:
		global id_number
		dic_table[resource] = base36encode(id_number)
		id_number += 1


def _normalize_row_key(source_name, row_identifier):
	"""
	TM-CACHE-STRUCTURES

	Return a stable same-row cache key for future quoted cache usage.

	This helper is intentionally introduced before the cache is wired into
	execution so later refactors do not invent different key shapes inside many
	branches of `semantify_file()`.
	"""
	return (str(source_name), str(row_identifier))


def _normalize_join_signature(join_fields):
	"""
	TM-CACHE-STRUCTURES

	Return a stable join signature for future quoted join-cache usage.

	Current normalization policy:
	- scalar field -> one-item tuple
	- list/tuple of fields -> tuple of string-normalized fields
	"""
	if isinstance(join_fields, (list, tuple)):
		return tuple(str(field) for field in join_fields)
	return (str(join_fields),)


def _normalize_join_value(join_value):
	"""
	TM-CACHE-STRUCTURES

	Return a stable join value for future quoted join-cache usage.

	Current normalization policy:
	- scalar value -> string
	- list/tuple value -> tuple of strings
	"""
	if isinstance(join_value, (list, tuple)):
		return tuple(str(value) for value in join_value)
	return str(join_value)


def _derive_row_identifier(row):
	"""
	TM-CACHE-STRUCTURES

	Derive a deterministic row identifier from the current row payload.

	Current policy:
	- for dictionary rows, sort items by key and normalize both keys and values
	- for non-dictionary rows, fall back to string conversion

	This keeps the first live same-row cache integration independent of row object
	identity, which makes it easier to debug and safer across repeated calls.
	"""
	if isinstance(row, dict):
		return tuple(
			(str(key), "" if value is None else str(value))
			for key, value in sorted(row.items(), key=lambda item: str(item[0]))
	)
	return str(row)


def _make_same_row_runtime_token(source_name, row_position):
	"""
	TM-CACHE-SAME-ROW

	Build a cheap deterministic identity for a streamed row within a source.
	"""
	return (str(source_name), int(row_position))


def _annotated_result_cache_key(triples_map_element, runtime_row_token):
	"""
	TM-CACHE-ANNOTATED

	Return the canonical exact-row key for unified annotated-result reuse.

	Phase 1 intentionally accepts only safe runtime row tokens. If a caller does
	not have exact `(source_name, row_position)` identity, this cache is skipped
	rather than falling back to content-derived row keys.
	"""
	if runtime_row_token is None:
		return None
	return (
		str(triples_map_element.triples_map_id),
		(str(runtime_row_token[0]), int(runtime_row_token[1]))
	)


def _annotated_result_cache_get(triples_map_element, runtime_row_token):
	"""
	TM-CACHE-ANNOTATED

	Read one exact-row annotated-result cache entry when a safe runtime token is
	available.
	"""
	cache_key = _annotated_result_cache_key(triples_map_element, runtime_row_token)
	if cache_key is None:
		return None
	return annotated_result_cache.get(cache_key)


def _annotated_result_cache_origin_label(access_context):
	"""
	TM-CACHE-ANNOTATED

	Coarsen detailed call-site labels into stable origin/access classes so reuse
	can be counted across different runtime paths without exploding the metric
	surface.
	"""
	if access_context is None:
		return "unknown"
	if access_context.startswith("same_row"):
		return "same_row"
	if access_context == "join_materialization":
		return "join_materialization"
	if access_context == "join_resolution":
		return "join_resolution"
	return str(access_context)


def _record_annotated_result_cache_hit(access_context, origin_label):
	"""
	TM-CACHE-ANNOTATED

	Update detailed reuse counters for one exact-row cache hit.
	"""
	global annotated_result_cache_hits
	global annotated_result_cache_same_row_hits
	global annotated_result_cache_join_materialization_hits
	global annotated_result_cache_join_resolution_hits
	global annotated_result_cache_cross_path_hits

	annotated_result_cache_hits += 1
	access_label = _annotated_result_cache_origin_label(access_context)
	if access_label == "same_row":
		annotated_result_cache_same_row_hits += 1
	elif access_label == "join_materialization":
		annotated_result_cache_join_materialization_hits += 1
	elif access_label == "join_resolution":
		annotated_result_cache_join_resolution_hits += 1

	if origin_label is not None and origin_label != access_label:
		annotated_result_cache_cross_path_hits += 1


def _annotated_result_cache_set(triples_map_element, runtime_row_token, payload, access_context):
	"""
	TM-CACHE-ANNOTATED

	Write one exact-row annotated-result cache entry.
	"""
	global annotated_result_cache_same_row_writes
	global annotated_result_cache_join_materialization_writes

	cache_key = _annotated_result_cache_key(triples_map_element, runtime_row_token)
	if cache_key is None:
		return
	annotated_result_cache[cache_key] = list(payload)
	origin_label = _annotated_result_cache_origin_label(access_context)
	annotated_result_cache_origins[cache_key] = origin_label
	if origin_label == "same_row":
		annotated_result_cache_same_row_writes += 1
	elif origin_label == "join_materialization":
		annotated_result_cache_join_materialization_writes += 1


def _get_or_build_annotated_result(triples_map_element, triples_map_list, delimiter, row, runtime_row_token, metric_name, access_context):
	"""
	TM-CACHE-ANNOTATED

	Return the final quoted result for one exact producer row.

	This is the migration-safe Phase 1 entry point for the future unified cache:
	- if a safe exact-row token is available and the result already exists,
	  return it directly
	- otherwise materialize once through `semantify_file(...)`
	- store the result under the canonical exact-row key for later reuse
	"""
	global annotated_result_cache_misses
	global annotated_result_cache_writes

	cache_key = _annotated_result_cache_key(triples_map_element, runtime_row_token)
	cached_payload = _annotated_result_cache_get(triples_map_element, runtime_row_token)
	if cached_payload is not None:
		_record_annotated_result_cache_hit(access_context, annotated_result_cache_origins.get(cache_key))
		return list(cached_payload), True

	if runtime_row_token is not None:
		annotated_result_cache_misses += 1

	with _time_metric(metric_name):
		materialized_triples = [
			triples
			for triples in semantify_file(
				triples_map_element,
				triples_map_list,
				delimiter,
				row,
				False,
				runtime_row_token
			)
			if triples is not None
		]

	if runtime_row_token is not None:
		_annotated_result_cache_set(triples_map_element, runtime_row_token, materialized_triples, access_context)
		annotated_result_cache_writes += 1

	return materialized_triples, False


def _same_row_cache_key(triples_map_element, row, runtime_row_token=None):
	"""
	TM-CACHE-SAME-ROW

	Build the canonical same-row cache key for a quoted producer TM.
	"""
	global same_row_key_runtime_token_uses
	global same_row_key_fallback_uses

	with _time_metric("same_row_cache_key"):
		if runtime_row_token is not None:
			same_row_key_runtime_token_uses += 1
			row_key = runtime_row_token
		else:
			same_row_key_fallback_uses += 1
			row_key = _normalize_row_key(str(triples_map_element.data_source), _derive_row_identifier(row))
		return (
			str(triples_map_element.triples_map_id),
			row_key
		)


def _resolve_quoted_join_payload_from_tokens(triples_map_element, join_key, join_value):
	"""
	TM-CACHE-JOIN

	Resolve a quoted join result through exact producer row tokens when the join
	index recorded them directly inside `join_table` during materialization.
	"""
	global annotated_result_cache_join_resolution_misses

	join_entries = join_table.get(join_key)
	if join_entries is None or join_value not in join_entries:
		return None

	row_tokens = join_entries[join_value]
	if isinstance(row_tokens, dict):
		# Compatibility path for any older payload-shaped quoted join indexes that
		# may still be encountered during the migration.
		return row_tokens

	payload = {}
	for runtime_row_token in row_tokens:
		cache_key = _annotated_result_cache_key(triples_map_element, runtime_row_token)
		cached_payload = _annotated_result_cache_get(triples_map_element, runtime_row_token)
		if cached_payload is None:
			annotated_result_cache_join_resolution_misses += 1
			return None
		_record_annotated_result_cache_hit("join_resolution", annotated_result_cache_origins.get(cache_key))
		for triples in cached_payload:
			payload[triples] = "subject"

	return payload


def _planning_tm_metadata(triples_map_element):
	"""
	TM-CACHE-PLANNED-USE

	Return planner metadata for a producer TM when runtime planning context is
	available.
	"""
	if active_planning_context is None:
		return None
	return active_planning_context["triples_maps"].get(str(triples_map_element.triples_map_id))


def _planning_equivalent_alias_metadata(triples_map_element):
	"""
	TM-CACHE-EQUIVALENCE

	Return equivalence metadata when the current TM is a single-POM alias of an
	owner TM + POM statement slice.
	"""
	if active_planning_context is None or not enable_equivalent_statement_cache:
		return None
	if not active_planning_context.get("has_statement_equivalence_groups", False):
		return None
	return active_planning_context.get("equivalent_alias_tm_index", {}).get(
		str(triples_map_element.triples_map_id)
	)


def _planning_owner_pom_equivalence(triples_map_element, pom_index):
	"""
	TM-CACHE-EQUIVALENCE

	Return equivalence metadata for one exact owner TM/POM pair.
	"""
	if active_planning_context is None or not enable_equivalent_statement_cache:
		return None
	if not active_planning_context.get("has_statement_equivalence_groups", False):
		return None
	return active_planning_context.get("owner_tm_equivalence_index", {}).get(
		str(triples_map_element.triples_map_id),
		{}
	).get(int(pom_index))


def _equivalent_statement_cache_key(owner_tm_id, owner_po_index, source_name, row, runtime_row_token=None):
	"""
	TM-CACHE-EQUIVALENCE

	Build the canonical cache key for one owner-TM + POM row payload.
	"""
	if runtime_row_token is not None:
		row_key = runtime_row_token
	else:
		row_key = _normalize_row_key(str(source_name), _derive_row_identifier(row))
	return (str(owner_tm_id), int(owner_po_index), row_key)


def _equivalent_statement_cache_get(owner_tm_id, owner_po_index, source_name, row, runtime_row_token=None):
	"""
	TM-CACHE-EQUIVALENCE

	Read one canonical owner-POM equivalence payload.
	"""
	cache_key = _equivalent_statement_cache_key(owner_tm_id, owner_po_index, source_name, row, runtime_row_token)
	return cache_key, equivalent_statement_cache.get(cache_key)


def _equivalent_statement_cache_set(group_id, owner_tm_id, owner_po_index, source_name, row, payload, runtime_row_token=None):
	"""
	TM-CACHE-EQUIVALENCE

	Write one canonical owner-POM equivalence payload.
	"""
	cache_key = _equivalent_statement_cache_key(owner_tm_id, owner_po_index, source_name, row, runtime_row_token)
	equivalent_statement_cache[cache_key] = list(payload)
	equivalent_statement_cache_groups.setdefault(str(group_id), set()).add(cache_key)


def _equivalent_statement_cache_get_by_row_token(owner_tm_id, owner_po_index, runtime_row_token):
	"""
	TM-CACHE-EQUIVALENCE

	Read one canonical owner-POM equivalence payload directly through the runtime
	row token stored in the row-keyed equivalence cache.
	"""
	cache_key = (str(owner_tm_id), int(owner_po_index), runtime_row_token)
	return cache_key, equivalent_statement_cache.get(cache_key)


def _equivalent_join_cache_group_key(owner_tm_id, owner_po_index, join_fields):
	"""
	TM-CACHE-EQUIVALENCE

	Build the canonical group key for one owner-POM join-equivalence bucket.
	"""
	return (
		str(owner_tm_id),
		int(owner_po_index),
		_normalize_join_signature(join_fields),
	)


def _equivalent_join_cache_get(owner_tm_id, owner_po_index, join_fields, join_value):
	"""
	TM-CACHE-EQUIVALENCE

	Read one canonical owner-POM join-equivalence payload.
	"""
	group_key = _equivalent_join_cache_group_key(owner_tm_id, owner_po_index, join_fields)
	lookup_key = _normalize_join_value(join_value)
	return group_key, lookup_key, equivalent_join_cache.get(group_key, {}).get(lookup_key)


def _equivalent_join_cache_set(group_id, owner_tm_id, owner_po_index, join_fields, join_value, payload):
	"""
	TM-CACHE-EQUIVALENCE

	Write one canonical owner-POM join-equivalence payload.
	"""
	group_key = _equivalent_join_cache_group_key(owner_tm_id, owner_po_index, join_fields)
	lookup_key = _normalize_join_value(join_value)
	if group_key not in equivalent_join_cache:
		equivalent_join_cache[group_key] = {}
	equivalent_join_cache[group_key][lookup_key] = list(payload)
	equivalent_join_cache_groups.setdefault(str(group_id), set()).add((group_key, lookup_key))


def _resolve_equivalent_join_payload(owner_tm_id, owner_po_index, join_fields, join_value):
	"""
	TM-CACHE-EQUIVALENCE

	Resolve one join-equivalence hit through the row-keyed equivalence cache.

	Optimized runs store runtime row tokens in `equivalent_join_cache` so the
	same canonical slice can be exposed in join-keyed form without duplicating the
	payload already retained in `equivalent_statement_cache`.
	"""
	group_key, lookup_key, cached_entry = _equivalent_join_cache_get(
		owner_tm_id,
		owner_po_index,
		join_fields,
		join_value
	)
	if cached_entry is None:
		return group_key, lookup_key, None

	if isinstance(cached_entry, dict):
		return group_key, lookup_key, cached_entry

	payload = {}
	for runtime_row_token in cached_entry:
		_, cached_payload = _equivalent_statement_cache_get_by_row_token(
			owner_tm_id,
			owner_po_index,
			runtime_row_token
		)
		if cached_payload is None:
			return group_key, lookup_key, None
		for triples in cached_payload:
			payload[triples] = "subject"

	return group_key, lookup_key, payload


def _backfill_annotated_result_cache_if_eligible(triples_map_element, runtime_row_token, payload, access_context):
	"""
	TM-CACHE-EQUIVALENCE

	Backfill the unified exact-row cache only when the cached thing is a whole-TM
	payload and the caller already has a safe runtime row token.

	Current intended use:
	- safe for alias late-comers, because a single-POM alias TM returns its whole
	  TM payload from equivalence
	- not safe for owner-side single-POM reuse, because one owner POM hit does
	  not imply that the whole owner TM has been materialized
	"""
	if runtime_row_token is None:
		return
	if not (_should_use_same_row_cache(triples_map_element) or _should_use_join_cache(triples_map_element)):
		return
	cache_key = _annotated_result_cache_key(triples_map_element, runtime_row_token)
	if cache_key is None or cache_key in annotated_result_cache:
		return
	_annotated_result_cache_set(triples_map_element, runtime_row_token, payload, access_context)
	global annotated_result_cache_writes
	annotated_result_cache_writes += 1


def _planning_effective_statement_tag(triples_map_element):
	"""
	TM-CACHE-PLANNED-USE

	Return one effective statement-slice tag for a quoted producer when runtime
	can identify the target slice unambiguously.

	Current safe rule:
	- one-POM producers -> use that one exact tag
	- multi-POM producers with identical tag shape -> use the shared shape
	- otherwise return `None` and fall back to TM-level planning
	"""
	metadata = _planning_tm_metadata(triples_map_element)
	if metadata is None:
		return None

	pom_tags = metadata.get("pom_tags") or []
	if not pom_tags:
		return None
	if len(pom_tags) == 1:
		return pom_tags[0]

	first_tag = pom_tags[0]
	comparable_fields = (
		"path_profile",
		"row_cache_candidate",
		"join_cache_candidate",
	)
	if all(
		all(tag.get(field) == first_tag.get(field) for field in comparable_fields)
		for tag in pom_tags[1:]
	):
		return first_tag

	return None


def _should_use_same_row_cache(triples_map_element):
	"""
	TM-CACHE-PLANNED-USE

	Same-row caching only pays off when the producer is referenced more than once
	at the mapping level. Planner metadata gives us a safe way to skip cache
	population for one-use producers while preserving the unified exact-row
	implementation for real reuse cases.
	"""
	if not enable_same_row_quoted_cache:
		return False

	metadata = _planning_tm_metadata(triples_map_element)
	if metadata is None:
		return True

	if not metadata.get("should_cache"):
		return False

	effective_tag = _planning_effective_statement_tag(triples_map_element)
	if effective_tag is not None:
		if not effective_tag.get("row_cache_candidate"):
			return False
	else:
		if metadata.get("cache_kind") != "row":
			return False

	return metadata.get("quoted_reference_count", 0) >= same_row_cache_min_reference_count


def _should_use_join_cache(triples_map_element):
	"""
	TM-CACHE-PLANNED-USE

	Join-cache stays enabled only for planner-approved join producers whose join
	shape looks strong enough to justify the current lookup/index overhead.

	Unified-cache mode keeps the same conservative policy as `main`:
	- `high` and `medium` join reuse strength -> keep join cache enabled
	- `low` join reuse strength -> skip join cache and stay on the token-backed
	  direct path without the extra memo layer
	"""
	if not enable_join_quoted_cache:
		return False

	metadata = _planning_tm_metadata(triples_map_element)
	if metadata is None:
		return True

	if not metadata.get("should_cache"):
		return False

	effective_tag = _planning_effective_statement_tag(triples_map_element)
	if effective_tag is not None:
		if not effective_tag.get("join_cache_candidate"):
			return False
	else:
		if metadata.get("cache_kind") != "join":
			return False

	join_strength = metadata.get("expected_join_reuse_strength")
	if join_strength is None:
		return True

	return join_strength in {"high", "medium"}


def _quoted_join_table_key(triples_map_element, join_fields):
	"""
	TM-CACHE-JOIN

	Build the legacy join-table key used by quoted cross-source join lookups.

	The runtime still stores quoted join indexes inside `join_table` under this
	string key, so both the on-demand and prebuilt paths need one shared helper
	to avoid duplicating the key logic.
	"""
	if isinstance(join_fields, list):
		return triples_map_element.triples_map_id + "_" + join_fields[0]
	return "quoted_" + triples_map_element.triples_map_id + "_" + join_fields


def _planning_join_signature_to_runtime_fields(join_signature):
	"""
	TM-CACHE-JOIN-PREBUILD

	Convert a planning-context join signature into the runtime field shape used
	by the quoted join-index builder.

	Phase 2 currently prebuilds only scalar signatures. Composite quoted join
	signatures stay on the existing on-demand path until they get a dedicated
	builder that matches the runtime semantics safely.
	"""
	if join_signature is None:
		return None
	if isinstance(join_signature, tuple):
		if len(join_signature) == 1:
			return join_signature[0]
		return None
	if isinstance(join_signature, list):
		if len(join_signature) == 1:
			return join_signature[0]
		return None
	return join_signature


def _stage_mixed_access_join_indexes_for_row(triples_map_element, row, runtime_row_token):
	"""
	TM-CACHE-JOIN

	While a mixed-access producer is already being scanned through the top-level
	exact-row path, stage quoted join-index entries for its planner-known join
	signatures so later cross-source access can reuse the same scan.
	"""
	if runtime_row_token is None:
		return

	metadata = _planning_tm_metadata(triples_map_element)
	if metadata is None:
		return
	if metadata.get("same_source_consumer_count", 0) <= 0:
		return
	if metadata.get("cross_source_consumer_count", 0) <= 0:
		return

	runtime_join_fields_list = []
	seen_signatures = set()
	for join_signature in metadata.get("join_signatures", []):
		runtime_join_fields = _planning_join_signature_to_runtime_fields(join_signature)
		if runtime_join_fields is None:
			continue
		normalized_signature = _normalize_join_signature(runtime_join_fields)
		if normalized_signature in seen_signatures:
			continue
		seen_signatures.add(normalized_signature)
		runtime_join_fields_list.append(runtime_join_fields)

	if not runtime_join_fields_list:
		return

	tm_stage = staged_join_table.setdefault(str(triples_map_element.triples_map_id), {})
	for runtime_join_fields in runtime_join_fields_list:
		join_value = _resolve_join_value_from_row(row, runtime_join_fields)
		if join_value is None:
			continue
		join_key = _quoted_join_table_key(triples_map_element, runtime_join_fields)
		stage_bucket = tm_stage.setdefault(join_key, {})
		row_tokens = stage_bucket.setdefault(join_value, [])
		if runtime_row_token not in row_tokens:
			row_tokens.append(runtime_row_token)


def _publish_staged_join_indexes_for_tm(triples_map):
	"""
	TM-CACHE-JOIN

	Publish any planner-guided quoted join indexes staged during the producer's
	top-level scan once that TM has fully completed.
	"""
	tm_stage = staged_join_table.pop(str(triples_map.triples_map_id), None)
	if not tm_stage:
		return

	for join_key, staged_bucket in tm_stage.items():
		live_bucket = join_table.setdefault(join_key, {})
		for join_value, staged_row_tokens in staged_bucket.items():
			live_entry = live_bucket.get(join_value)
			if live_entry is None:
				live_bucket[join_value] = list(staged_row_tokens)
				continue
			if not isinstance(live_entry, list):
				continue
			for runtime_row_token in staged_row_tokens:
				if runtime_row_token not in live_entry:
					live_entry.append(runtime_row_token)


def _planning_equivalence_join_fields(group_id):
	"""
	TM-CACHE-EQUIVALENCE

	Collect the planner-known cross-source join signatures used by members of one
	equivalence group.

	Only scalar signatures are staged eagerly, matching the current quoted join
	index behavior.
	"""
	if active_planning_context is None or group_id is None:
		return []

	group_metadata = active_planning_context.get("statement_equivalence_group_index", {}).get(group_id)
	if group_metadata is None:
		return []

	runtime_join_fields_list = []
	seen_signatures = set()
	for tm_id in group_metadata.get("tm_ids", []):
		member_metadata = active_planning_context.get("triples_maps", {}).get(tm_id)
		if member_metadata is None:
			continue
		if member_metadata.get("cross_source_consumer_count", 0) <= 0:
			continue
		for join_signature in member_metadata.get("join_signatures", []):
			runtime_join_fields = _planning_join_signature_to_runtime_fields(join_signature)
			if runtime_join_fields is None:
				continue
			normalized_signature = _normalize_join_signature(runtime_join_fields)
			if normalized_signature in seen_signatures:
				continue
			seen_signatures.add(normalized_signature)
			runtime_join_fields_list.append(runtime_join_fields)

	return runtime_join_fields_list


def _stage_equivalent_join_indexes_for_row(scan_tm_id, equivalence_metadata, row, runtime_row_token):
	"""
	TM-CACHE-EQUIVALENCE

	While a source is already being scanned through a safe full-row path, stage
	join-keyed access entries for any cross-source consumers of the same
	equivalence group.

	The staged structure stores runtime row tokens, and the payload itself stays
	in `equivalent_statement_cache`.
	"""
	if runtime_row_token is None or equivalence_metadata is None:
		return

	runtime_join_fields_list = _planning_equivalence_join_fields(equivalence_metadata.get("group_id"))
	if not runtime_join_fields_list:
		return

	tm_stage = staged_equivalent_join_cache.setdefault(str(scan_tm_id), {})
	group_id = equivalence_metadata["group_id"]
	owner_tm_id = equivalence_metadata["canonical_owner_tm_id"]
	owner_po_index = equivalence_metadata["canonical_owner_po_index"]

	for runtime_join_fields in runtime_join_fields_list:
		join_value = _resolve_join_value_from_row(row, runtime_join_fields)
		if join_value is None:
			continue
		group_key = _equivalent_join_cache_group_key(owner_tm_id, owner_po_index, runtime_join_fields)
		lookup_key = _normalize_join_value(join_value)
		stage_bucket = tm_stage.setdefault(group_key, {})
		row_tokens = stage_bucket.setdefault(lookup_key, [])
		if runtime_row_token not in row_tokens:
			row_tokens.append(runtime_row_token)
		equivalent_join_cache_groups.setdefault(str(group_id), set()).add((group_key, lookup_key))


def _publish_staged_equivalent_join_indexes_for_tm(triples_map):
	"""
	TM-CACHE-EQUIVALENCE

	Publish any staged join-keyed equivalence entries once the scanning TM has
	finished its full-row execution path.
	"""
	global equivalent_join_cache_populates

	tm_stage = staged_equivalent_join_cache.pop(str(triples_map.triples_map_id), None)
	if not tm_stage:
		return

	for group_key, staged_bucket in tm_stage.items():
		live_bucket = equivalent_join_cache.setdefault(group_key, {})
		for lookup_key, staged_row_tokens in staged_bucket.items():
			live_entry = live_bucket.get(lookup_key)
			if live_entry is None:
				live_bucket[lookup_key] = list(staged_row_tokens)
				equivalent_join_cache_populates += 1
				continue
			if isinstance(live_entry, dict):
				continue
			for runtime_row_token in staged_row_tokens:
				if runtime_row_token not in live_entry:
					live_entry.append(runtime_row_token)


def _materialize_producer_row_for_join_index(triples_map_element, triples_map_list, row, row_position):
	"""
	TM-CACHE-JOIN-MATERIALIZATION

	Materialize one producer row for quoted join-index population.

	For selected high-value producers, this reuses a shared producer-row cache so
	multiple join-index builds can reuse the same serialized quoted result without
	rematerializing the producer row.
	"""
	runtime_row_token = _make_same_row_runtime_token(triples_map_element.data_source, row_position)
	materialized_triples, _ = _get_or_build_annotated_result(
		triples_map_element,
		triples_map_list,
		",",
		row,
		runtime_row_token,
		"annotated_result_join_materialization_build",
		"join_materialization"
	)
	return materialized_triples


def _normalize_csv_header(header_name):
	"""
	Return one stable column name across CSV readers.

	This keeps the runtime resilient to UTF-8 BOM-prefixed first headers and
	accidentally quoted header names so top-level row iteration and helper/join
	build paths see the same keys.
	"""
	normalized = str(header_name).lstrip("\ufeff")
	if len(normalized) >= 2 and normalized[0] == normalized[-1] and normalized[0] in {"\"", "'"}:
		normalized = normalized[1:-1]
	return normalized.strip()


def _load_csv_rows(source, delimiter=",", drop_duplicates=False):
	"""
	Load one CSV/TSV source through a single normalized reader path.
	"""
	read_kwargs = {
		"dtype": str,
		"sep": delimiter,
		"encoding": "utf-8-sig",
	}
	try:
		reader = pd.read_csv(source, **read_kwargs)
	except UnicodeDecodeError:
		read_kwargs["encoding"] = "ISO-8859-1"
		reader = pd.read_csv(source, **read_kwargs)
	reader.columns = [_normalize_csv_header(column_name) for column_name in reader.columns]
	reader = reader.where(pd.notnull(reader), None)
	if drop_duplicates:
		reader = reader.drop_duplicates(keep='first')
	return reader.to_dict(orient='records')


@contextmanager
def _stream_csv_rows(source, delimiter=","):
	"""
	Yield one normalized streaming CSV reader.

	This keeps the hot row-by-row execution path lightweight while still fixing
	the BOM/header inconsistency that broke quoted joins across sources.
	"""
	input_file_descriptor = None
	for encoding in ("utf-8-sig", "ISO-8859-1"):
		try:
			input_file_descriptor = open(source, "r", encoding=encoding, newline="")
			input_file_descriptor.read(4096)
			input_file_descriptor.seek(0)
			break
		except UnicodeDecodeError:
			if input_file_descriptor is not None:
				input_file_descriptor.close()
				input_file_descriptor = None
	if input_file_descriptor is None:
		input_file_descriptor = open(source, "r", encoding="ISO-8859-1", newline="")
	try:
		reader = csv.DictReader(input_file_descriptor, delimiter=delimiter)
		if reader.fieldnames is not None:
			reader.fieldnames = [_normalize_csv_header(field_name) for field_name in reader.fieldnames]
		yield reader
	finally:
		input_file_descriptor.close()


def _ensure_quoted_join_index(triples_map_element, join_fields, metric_name, triples_map_list):
	"""
	TM-CACHE-JOIN

	Ensure that the producer-owned quoted join index exists for one join
	signature.

	This is the first Phase 2 extraction point:
	- subject/object join branches no longer open and scan producer sources
	  inline
	- quoted join-index builds now share one helper and one producer-row
	  materialization path
	"""
	join_key = _quoted_join_table_key(triples_map_element, join_fields)
	if join_key in join_table:
		return join_key

	if str(triples_map_element.file_format).lower() == "csv":
		with _time_metric(metric_name):
			data = _load_csv_rows(str(triples_map_element.data_source), drop_duplicates=True)
			hash_maker(
				data,
				triples_map_element,
				types.SimpleNamespace(child=join_fields, parent=join_fields),
				"quoted",
				triples_map_list
			)
	elif triples_map_element.file_format == "JSONPath":
		# Keep JSONPath behavior aligned with the existing runtime until a
		# dedicated quoted-join JSON path is added.
		pass

	return join_key


def _prebuild_high_join_indexes(planning_context, triples_map_list):
	"""
	TM-CACHE-JOIN-PREBUILD

	Prebuild quoted join indexes for the strongest planner-approved producers.

	The unified-cache branch keeps the same conservative eager-prebuild rule as
	`main` so comparison stays apples-to-apples:
	- only `high` join-strength producers are considered
	- only scalar join signatures are prebuilt
	"""
	global quoted_join_index_prebuilds
	global quoted_join_index_prebuild_skips

	if planning_context is None or not enable_join_quoted_cache or not enable_eager_join_prebuild:
		return

	with _time_metric("prebuild_high_join_indexes"):
		for tm_id, metadata in planning_context["triples_maps"].items():
			if not metadata.get("should_cache"):
				continue
			if metadata.get("cache_kind") != "join":
				continue
			if metadata.get("expected_join_reuse_strength") != "high":
				continue

			triples_map_element = planning_context["tm_index"].get(tm_id)
			if triples_map_element is None:
				continue

			for join_signature in metadata.get("join_signatures", []):
				runtime_join_fields = _planning_join_signature_to_runtime_fields(join_signature)
				if runtime_join_fields is None:
					quoted_join_index_prebuild_skips += 1
					continue

				join_key = _quoted_join_table_key(triples_map_element, runtime_join_fields)
				if join_key in join_table:
					continue

				_ensure_quoted_join_index(
					triples_map_element,
					runtime_join_fields,
					"prebuild_high_join_index_group",
					triples_map_list
				)
				quoted_join_index_prebuilds += 1


def _collect_asserted_tm_execution_entries(sorted_sources, order_list):
	"""
	TM-CACHE-LEVEL-EXECUTION

	Collect the current files_sort()-driven asserted TM order as flat entries.

	Each entry retains:
	- `source_type`
	- `source`
	- the dictionary key used inside `sorted_sources`
	- the actual TM object

	This gives us one neutral representation that can either:
	- preserve the original runtime order
	- or be regrouped by TM level while still preserving files_sort() order
	  inside each level.
	"""
	entries = []

	def _append_source_entries(source_type, source, tm_map):
		for triples_map in tm_map:
			tm = tm_map[triples_map]
			if "NonAssertedTriplesMap" not in tm.mappings_type:
				entries.append((source_type, source, triples_map, tm))

	if order_list:
		for source_type in order_list:
			if source_type not in sorted_sources:
				continue
			for source in order_list[source_type]:
				if source in sorted_sources[source_type]:
					_append_source_entries(source_type, source, sorted_sources[source_type][source])
	else:
		for source_type in sorted_sources:
			for source in sorted_sources[source_type]:
				_append_source_entries(source_type, source, sorted_sources[source_type][source])

	return entries


def _group_execution_entries_into_batches(entries):
	"""
	TM-CACHE-LEVEL-EXECUTION

	Group consecutive execution entries by source so the existing runtime can
	still read a source once per batch and iterate its TMs in order.
	"""
	batches = []
	current_source_key = None
	current_tm_keys = []

	for source_type, source, triples_map_key, _ in entries:
		source_key = (source_type, source)
		if current_source_key != source_key:
			if current_source_key is not None:
				batches.append((current_source_key[0], current_source_key[1], current_tm_keys))
			current_source_key = source_key
			current_tm_keys = []
		current_tm_keys.append(triples_map_key)

	if current_source_key is not None:
		batches.append((current_source_key[0], current_source_key[1], current_tm_keys))

	return batches


def _build_execution_batches(sorted_sources, order_list, planning_context):
	"""
	TM-CACHE-LEVEL-EXECUTION

	Build runtime execution batches.

	Default behavior:
	- preserve the original files_sort() ordering exactly

	Optional level-aware behavior:
	- group asserted TMs by planning level
	- preserve files_sort() order within each level
	- keep source batching so the existing row-reading logic changes as little as
	  possible
	"""
	with _time_metric("build_execution_batches"):
		entries = _collect_asserted_tm_execution_entries(sorted_sources, order_list)

		if not enable_level_execution_order or planning_context is None:
			return _group_execution_entries_into_batches(entries)

		level_entries = {}
		for entry in entries:
			tm_id = str(entry[3].triples_map_id)
			level = planning_context["triples_maps"].get(tm_id, {}).get("level")
			if level is None:
				level = 0
			level_entries.setdefault(level, []).append(entry)

		ordered_entries = []
		for level in sorted(level_entries):
			ordered_entries.extend(level_entries[level])

		return _group_execution_entries_into_batches(ordered_entries)


def _build_asserted_tm_execution_order_from_batches(sorted_sources, execution_batches):
	"""
	TM-CACHE-LEVEL-EXECUTION

	Extract the asserted TM execution order from already-built execution batches.
	This lets the flush logic follow the real runtime order, including when
	level-aware execution is enabled.
	"""
	execution_order = []
	for source_type, source, tm_keys in execution_batches:
		for tm_key in tm_keys:
			execution_order.append(str(sorted_sources[source_type][source][tm_key].triples_map_id))
	return execution_order


def _flush_tm_cache_entries(tm_id):
	"""
	TM-CACHE-FLUSH

	Flush all quoted cache entries owned by a producer TM.
	"""
	with _time_metric("flush_tm_cache_entries"):
		annotated_result_keys = [cache_key for cache_key in annotated_result_cache if cache_key[0] == str(tm_id)]
		for cache_key in annotated_result_keys:
			annotated_result_cache.pop(cache_key, None)
			annotated_result_cache_origins.pop(cache_key, None)

		join_table_keys = [
			join_key
			for join_key in join_table
			if join_key.startswith("quoted_" + str(tm_id) + "_")
			or join_key.startswith(str(tm_id) + "_")
		]
		for join_key in join_table_keys:
			join_table.pop(join_key, None)

		return {
			"same_row_entries": 0,
			"join_groups": len(join_table_keys),
			"annotated_result_entries": len(annotated_result_keys),
			"quoted_join_row_token_groups": 0,
			"equivalent_statement_entries": 0,
			"equivalent_join_entries": 0,
		}


def _flush_equivalent_group_cache_entries(group_id):
	"""
	TM-CACHE-FLUSH

	Flush row-level statement equivalence entries for one canonical owner-POM
	group.
	"""
	statement_keys = list(equivalent_statement_cache_groups.get(str(group_id), set()))
	for cache_key in statement_keys:
		equivalent_statement_cache.pop(cache_key, None)
	equivalent_statement_cache_groups.pop(str(group_id), None)
	return len(statement_keys)


def _flush_equivalent_group_join_cache_entries(group_id):
	"""
	TM-CACHE-FLUSH

	Flush join-level equivalence entries for one canonical owner-POM group.
	"""
	join_entries = list(equivalent_join_cache_groups.get(str(group_id), set()))
	for group_key, lookup_key in join_entries:
		group_bucket = equivalent_join_cache.get(group_key)
		if group_bucket is None:
			continue
		group_bucket.pop(lookup_key, None)
		if not group_bucket:
			equivalent_join_cache.pop(group_key, None)
	equivalent_join_cache_groups.pop(str(group_id), None)
	return len(join_entries)


def _flush_completed_tm_caches(planning_context, remaining_execution_tm_ids, flushed_tm_ids):
	"""
	TM-CACHE-FLUSH

	Conservatively flush caches once all still-to-run asserted TMs are above the
	TM's transitive flush level.
	"""
	global level_cache_flush_events
	global flushed_same_row_cache_entries
	global flushed_join_table_groups
	global flushed_annotated_result_cache_entries
	global flushed_equivalent_statement_cache_entries
	global flushed_equivalent_join_cache_entries
	global flushed_equivalent_statement_group_ids
	global cache_flush_debug_events

	def _direct_non_asserted_consumers_equivalence_ready(metadata):
		"""
		TM-CACHE-FLUSH

		Allow certain direct non-asserted consumers to stop blocking an early TM
		flush when they are single-POM equivalence aliases whose canonical asserted
		owner slice has already completed.
		"""
		direct_non_asserted_consumers = metadata.get("direct_non_asserted_consumers", [])
		if not direct_non_asserted_consumers:
			return True

		triples_map_index = planning_context.get("triples_maps", {})
		for consumer_id in direct_non_asserted_consumers:
			consumer_metadata = triples_map_index.get(consumer_id)
			if consumer_metadata is None:
				return False
			if not consumer_metadata.get("single_pom_non_asserted", False):
				return False
			if not consumer_metadata.get("equivalence_group_id"):
				return False
			if not consumer_metadata.get("equivalence_canonical_owner_asserted", False):
				return False

			canonical_owner_tm_id = consumer_metadata.get("equivalence_canonical_owner_tm_id")
			if canonical_owner_tm_id is None:
				return False
			if canonical_owner_tm_id in remaining_execution_tm_ids:
				return False

		return True

	def _direct_consumers_completion_ready(metadata):
		"""
		TM-CACHE-FLUSH

		Allow an earlier TM-level flush only when every direct asserted consumer is
		done and every direct non-asserted consumer is equivalence-backed by a
		completed asserted owner slice.
		"""
		direct_asserted_consumers = metadata.get("direct_asserted_consumers", [])
		direct_non_asserted_consumers = metadata.get("direct_non_asserted_consumers", [])
		if not direct_asserted_consumers and not direct_non_asserted_consumers:
			return False

		if not all(consumer_id not in remaining_execution_tm_ids for consumer_id in direct_asserted_consumers):
			return False

		return _direct_non_asserted_consumers_equivalence_ready(metadata)

	with _time_metric("flush_completed_tm_caches"):
		if not enable_level_cache_flush or planning_context is None:
			return

		remaining_levels = [
			planning_context["triples_maps"][tm_id]["level"]
			for tm_id in remaining_execution_tm_ids
			if tm_id in planning_context["triples_maps"]
			and planning_context["triples_maps"][tm_id]["level"] is not None
		]
		min_remaining_level = min(remaining_levels) if remaining_levels else None

		for tm_id, metadata in planning_context["triples_maps"].items():
			if tm_id in flushed_tm_ids or not metadata["should_cache"]:
				continue

			flush_after_level = metadata.get("flush_after_level")
			if flush_after_level is None:
				continue

			direct_consumer_completion_ready = _direct_consumers_completion_ready(metadata)
			if (
				not direct_consumer_completion_ready
				and min_remaining_level is not None
				and flush_after_level >= min_remaining_level
			):
				continue

			flush_stats = _flush_tm_cache_entries(tm_id)
			flushed_tm_ids.add(tm_id)
			level_cache_flush_events += 1
			flushed_same_row_cache_entries += flush_stats["same_row_entries"]
			flushed_join_table_groups += flush_stats["join_groups"]
			flushed_annotated_result_cache_entries += flush_stats["annotated_result_entries"]
			flushed_equivalent_statement_cache_entries += flush_stats.get("equivalent_statement_entries", 0)
			flushed_equivalent_join_cache_entries += flush_stats.get("equivalent_join_entries", 0)
			cache_flush_debug_events.append({
				"tm_id": str(tm_id),
				"tm_name": metadata["tm_name"],
				"tm_level": metadata["level"],
				"flush_after_level": flush_after_level,
				"min_remaining_level": min_remaining_level,
				"direct_consumer_completion_ready": direct_consumer_completion_ready,
				"same_row_entries": flush_stats["same_row_entries"],
				"join_groups": flush_stats["join_groups"],
				"annotated_result_entries": flush_stats["annotated_result_entries"],
				"equivalent_statement_entries": flush_stats.get("equivalent_statement_entries", 0),
				"equivalent_join_entries": flush_stats.get("equivalent_join_entries", 0),
			})
			_capture_memory_sample(f"flush:{metadata['tm_name']}")

		for group_id, group_metadata in planning_context.get("statement_equivalence_group_index", {}).items():
			if group_id in flushed_equivalent_statement_group_ids:
				continue
			group_flush_after_level = group_metadata.get("flush_after_level")
			if group_flush_after_level is None:
				continue
			if min_remaining_level is not None and group_flush_after_level >= min_remaining_level:
				continue

			flushed_count = _flush_equivalent_group_cache_entries(group_id)
			flushed_equivalent_statement_group_ids.add(group_id)
			flushed_equivalent_statement_cache_entries += flushed_count
			flushed_equivalent_join_cache_entries += _flush_equivalent_group_join_cache_entries(group_id)


def _mark_tm_complete_and_maybe_flush(triples_map, planning_context, remaining_execution_tm_ids, flushed_tm_ids):
	"""
	TM-CACHE-FLUSH

	Mark the current asserted TM as completed under the existing runtime order
	and trigger conservative cache flushing.
	"""
	_publish_staged_join_indexes_for_tm(triples_map)
	_publish_staged_equivalent_join_indexes_for_tm(triples_map)

	if planning_context is None:
		return

	remaining_execution_tm_ids.discard(str(triples_map.triples_map_id))
	_flush_completed_tm_caches(planning_context, remaining_execution_tm_ids, flushed_tm_ids)


def format_cache_flush_debug_events():
	"""
	TM-CACHE-FLUSH

	Render per-event flush details collected during a run. This is intentionally
	separate from the aggregate sanity output so normal runs can keep the summary
	while debug runs opt into the event trace.
	"""
	lines = ["Cache flush debug:"]
	if not cache_flush_debug_events:
		lines.append("  no flush events recorded")
		return "\n".join(lines)

	for event in cache_flush_debug_events:
		lines.append(
			"  "
			f"L{event['tm_level']} | {event['tm_name']} | flush_after={event['flush_after_level']} | "
			f"min_remaining={event['min_remaining_level']} | same_row={event['same_row_entries']} | "
			f"join_groups={event['join_groups']} | annotated_results={event['annotated_result_entries']} | "
			f"eq_stmt={event.get('equivalent_statement_entries', 0)} | "
			f"eq_join={event.get('equivalent_join_entries', 0)}"
		)

	return "\n".join(lines)


def _tm_debug_enabled(triples_map_name):
	return any(debug_filter in triples_map_name for debug_filter in debug_tm_filters)


def _record_tm_debug(triples_map_name, branch_label, subject_present, object_present, object_list_size, subject_list_size, emitted_count):
	if not debug_tm_filters or not _tm_debug_enabled(triples_map_name):
		return

	stats = debug_tm_stats.setdefault(triples_map_name, {
		"rows": 0,
		"branch_hits": 0,
		"subject_present": 0,
		"object_present": 0,
		"object_list_rows": 0,
		"object_list_items": 0,
		"subject_list_rows": 0,
		"subject_list_items": 0,
		"triples_emitted": 0,
		"branches": {}
	})
	stats["rows"] += 1
	if subject_present:
		stats["subject_present"] += 1
	if object_present:
		stats["object_present"] += 1
	if object_list_size:
		stats["object_list_rows"] += 1
		stats["object_list_items"] += object_list_size
	if subject_list_size:
		stats["subject_list_rows"] += 1
		stats["subject_list_items"] += subject_list_size
	stats["triples_emitted"] += emitted_count
	if branch_label:
		stats["branch_hits"] += 1
		stats["branches"][branch_label] = stats["branches"].get(branch_label, 0) + 1


def format_tm_debug_stats():
	lines = ["TM debug:"]
	if not debug_tm_stats:
		lines.append("  no TM debug data recorded")
		return "\n".join(lines)

	for tm_name in sorted(debug_tm_stats):
		stats = debug_tm_stats[tm_name]
		branch_summary = ", ".join(
			f"{label}={count}"
			for label, count in sorted(stats["branches"].items())
		) or "none"
		lines.append(
			f"  {tm_name} | rows={stats['rows']} | branch_hits={stats['branch_hits']} | "
			f"subject_present={stats['subject_present']} | object_present={stats['object_present']} | "
			f"object_list_rows={stats['object_list_rows']} | object_list_items={stats['object_list_items']} | "
			f"subject_list_rows={stats['subject_list_rows']} | subject_list_items={stats['subject_list_items']} | "
			f"triples_emitted={stats['triples_emitted']} | branches={branch_summary}"
		)
	return "\n".join(lines)


def format_cache_foundation_sanity(planning_context=None):
	"""
	TM-CACHE-SANITY

	Render a compact read-only sanity summary for the current cache foundations.

	Purpose:
	- confirm that the forward and reverse dictionaries are in sync
	- confirm that the empty cache structures exist and report their size
	- optionally relate the current runtime state to the planning metadata

	This helper is intentionally observational only. It does not mutate runtime
	state and should remain safe to call during debugging runs.
	"""
	key_helpers_ready = (
		_normalize_row_key("source.csv", 1) == ("source.csv", "1")
		and _normalize_join_signature("claim_id") == ("claim_id",)
		and _normalize_join_value(10) == "10"
	)
	join_key_helpers_ready = (
		_quoted_join_table_key(types.SimpleNamespace(triples_map_id="TM_Claim"), "claim_id")
		== "quoted_TM_Claim_claim_id"
		and _normalize_join_value(10) == "10"
		and _normalize_join_value(["10", "20"]) == ("10", "20")
	)

	lines = [
		"Cache foundation sanity:",
		f"  dic_entries={len(dic_table)}",
		f"  annotated_result_cache_entries={len(annotated_result_cache)} | "
		f"join_table_groups={len(join_table)} | "
		f"equivalent_statement_cache_entries={len(equivalent_statement_cache)} | "
		f"equivalent_join_cache_groups={len(equivalent_join_cache)}",
		f"  key_helpers_ready={key_helpers_ready} | join_key_helpers_ready={join_key_helpers_ready}",
		f"  same_row_cache_hits={same_row_cache_hits} | same_row_cache_misses={same_row_cache_misses}",
		f"  same_row_key_runtime_tokens={same_row_key_runtime_token_uses} | same_row_key_fallbacks={same_row_key_fallback_uses}",
		f"  annotated_result_cache_hits={annotated_result_cache_hits} | "
		f"annotated_result_cache_misses={annotated_result_cache_misses} | "
		f"annotated_result_cache_writes={annotated_result_cache_writes}",
		f"  annotated_result_cache_same_row_hits={annotated_result_cache_same_row_hits} | "
		f"annotated_result_cache_join_materialization_hits={annotated_result_cache_join_materialization_hits} | "
		f"annotated_result_cache_join_resolution_hits={annotated_result_cache_join_resolution_hits}",
		f"  annotated_result_cache_same_row_writes={annotated_result_cache_same_row_writes} | "
		f"annotated_result_cache_join_materialization_writes={annotated_result_cache_join_materialization_writes} | "
		f"annotated_result_cache_cross_path_hits={annotated_result_cache_cross_path_hits} | "
		f"annotated_result_cache_join_resolution_misses={annotated_result_cache_join_resolution_misses}",
		f"  equivalent_statement_cache_hits={equivalent_statement_cache_hits} | "
		f"equivalent_statement_cache_misses={equivalent_statement_cache_misses} | "
		f"equivalent_statement_cache_populates={equivalent_statement_cache_populates} | "
		f"owner_populates={equivalent_statement_cache_owner_populates} | "
		f"alias_populates={equivalent_statement_cache_alias_populates} | "
		f"owner_hits={equivalent_statement_cache_owner_hits} | "
		f"alias_hits={equivalent_statement_cache_alias_hits}",
		f"  equivalent_join_cache_hits={equivalent_join_cache_hits} | "
		f"equivalent_join_cache_misses={equivalent_join_cache_misses} | "
		f"equivalent_join_cache_populates={equivalent_join_cache_populates}",
		f"  quoted_join_index_prebuilds={quoted_join_index_prebuilds} | "
		f"quoted_join_index_prebuild_skips={quoted_join_index_prebuild_skips}",
		f"  planner_same_row_cache_skips={planner_same_row_cache_skips} | "
		f"planner_join_cache_skips={planner_join_cache_skips}",
		f"  level_cache_flush_events={level_cache_flush_events} | flushed_same_row_cache_entries={flushed_same_row_cache_entries} | "
		f"flushed_join_table_groups={flushed_join_table_groups} | "
		f"flushed_annotated_result_cache_entries={flushed_annotated_result_cache_entries} | "
		f"flushed_equivalent_statement_cache_entries={flushed_equivalent_statement_cache_entries} | "
		f"flushed_equivalent_join_cache_entries={flushed_equivalent_join_cache_entries}",
		f"  same_row_cache_min_reference_count={same_row_cache_min_reference_count}",
	]

	if planning_context is not None:
		row_candidates = sum(
			1 for metadata in planning_context["triples_maps"].values()
			if metadata["should_cache"] and metadata["cache_kind"] == "row"
		)
		join_candidates = sum(
			1 for metadata in planning_context["triples_maps"].values()
			if metadata["should_cache"] and metadata["cache_kind"] == "join"
		)
		lines.append(
			f"  planned_cache_candidates={row_candidates + join_candidates} | "
			f"row={row_candidates} | join={join_candidates}"
		)

	return "\n".join(lines)

def _resolve_join_value_from_row(row, join_fields):
	"""
	Return the runtime join value from the current row when the child side of a
	join condition is available, otherwise `None`.

	This keeps quoted-join handling aligned with the older parent-triples-map
	path, which already treats missing child keys as "no match" instead of
	crashing the whole materialization.
	"""
	if row is None or not hasattr(row, "keys"):
		return None

	if isinstance(join_fields, (list, tuple)):
		if len(join_fields) == 1:
			field_name = join_fields[0]
			return row[field_name] if field_name in row else None
		if sublist(join_fields, row.keys()):
			return child_list_value(join_fields, row)
		return None

	return row[join_fields] if join_fields in row else None

def hash_update(parent_data, parent_subject, child_object,join_id):
	hash_table = {}
	for row_position, row in enumerate(parent_data):
		if child_object.parent[0] in row.keys():
			if row[child_object.parent[0]] in hash_table:
				if duplicate == "yes":
					if parent_subject.subject_map.subject_mapping_type == "reference":
						value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
						if value != None:
							if "http" in value and "<" not in value:
								value = "<" + value[1:-1] + ">"
							elif "http" in value and "<" in value:
								value = value[1:-1] 
						if value not in hash_table[row[child_object.parent[0]]]:
							hash_table[row[child_object.parent[0]]].update({value : "object"})
					else:
						if string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) is not None:
							if "<" + string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) + ">" not in hash_table[row[child_object.parent[0]]]:
								hash_table[row[child_object.parent[0]]].update({"<" + string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) + ">" : "object"}) 
				else:
					if parent_subject.subject_map.subject_mapping_type == "reference":
						value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore)
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1] 
						hash_table[row[child_object.parent[0]]].update({value : "object"})
					else:
						if string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) is not None:
							hash_table[row[child_object.parent[0]]].update({"<" + string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) + ">" : "object"})

			else:
				if parent_subject.subject_map.subject_mapping_type == "reference":
					value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
					if value != None:
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1] 
					hash_table.update({row[child_object.parent[0]] : {value : "object"}}) 
				else:
					if string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) is not None:
						hash_table.update({row[child_object.parent[0]] : {"<" + string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) + ">" : "object"}})
	join_table[join_id].update(hash_table)

def hash_maker(parent_data, parent_subject, child_object, quoted, triples_map_list):
	global blank_message
	hash_table = {}
	for row_position, row in enumerate(parent_data):
		if quoted == "":
			if child_object.parent[0] in row.keys():
				if row[child_object.parent[0]] in hash_table:
					if duplicate == "yes":
						if parent_subject.subject_map.subject_mapping_type == "reference":
							value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
							if value != None:
								if "http" in value and "<" not in value:
									value = "<" + value[1:-1] + ">"
								elif "http" in value and "<" in value:
									value = value[1:-1] 
							if value not in hash_table[row[child_object.parent[0]]]:
								hash_table[row[child_object.parent[0]]].update({value : "object"})
						else:
							if string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) != None:
								value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator)
								if value != None:
									if parent_subject.subject_map.term_type != None:
										if "BlankNode" in parent_subject.subject_map.term_type:
											if "/" in value:
												value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
												if "." in value:
													value = value.replace(".","2E")
												if blank_message:
													print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
													blank_message = False
											else:
												value = "_:" + encode_char(value).replace("%","")
												if "." in value:
													value = value.replace(".","2E")
									else:
										value = "<" + value + ">"
									hash_table[row[child_object.parent[0]]].update({value : "object"})
					else:
						if parent_subject.subject_map.subject_mapping_type == "reference":
							value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
							if "http" in value and "<" not in value:
								value = "<" + value[1:-1] + ">"
							elif "http" in value and "<" in value:
								value = value[1:-1] 
							hash_table[row[child_object.parent[0]]].update({value : "object"})
						else:
							value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator)
							if value != None:
								if parent_subject.subject_map.term_type != None:
									if "BlankNode" in parent_subject.subject_map.term_type:
										if "/" in value:
											value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
											if "." in value:
												value = value.replace(".","2E")
											if blank_message:
												print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
												blank_message = False
										else:
											value = "_:" + encode_char(value).replace("%","")
											if "." in value:
												value = value.replace(".","2E")
								else:
									value = "<" + value + ">"
								hash_table[row[child_object.parent[0]]].update({value : "object"})

				else:
					if parent_subject.subject_map.subject_mapping_type == "reference":
						value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
						if value != None:
							if "http" in value and "<" not in value:
								value = "<" + value[1:-1] + ">"
							elif "http" in value and "<" in value:
								value = value[1:-1] 
						hash_table.update({row[child_object.parent[0]] : {value : "object"}}) 
					else:
						value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator)
						if value != None:
							if parent_subject.subject_map.term_type != None:
								if "BlankNode" in parent_subject.subject_map.term_type:
									if "/" in value:
										value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
										if "." in value:
											value = value.replace(".","2E")
										if blank_message:
											print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
											blank_message = False
									else:
										value = "_:" + encode_char(value).replace("%","")
										if "." in value:
											value = value.replace(".","2E")
							else:
								value = "<" + value + ">"
							hash_table.update({row[child_object.parent[0]] : {value : "object"}})
		else:
			join_id = _quoted_join_table_key(parent_subject, child_object.child)
			# TM-CACHE-JOIN:
			# Build quoted join indexes from safe producer row tokens while
			# materializing the exact annotated payload once. Using
			# `no_inner_cycle=False` here keeps this internal producer
			# materialization side-effect free, so multiple quoted join
			# signatures (for example `userId` and `movieId`) can coexist
			# against the same producer TM in one run.
			runtime_row_token = _make_same_row_runtime_token(parent_subject.data_source, row_position)
			join_value = _resolve_join_value_from_row(row, child_object.parent)
			if join_value is None:
				continue
			materialized_triples = _materialize_producer_row_for_join_index(parent_subject, triples_map_list, row, row_position)
			if materialized_triples:
				if join_value not in hash_table:
					hash_table[join_value] = []
				if runtime_row_token not in hash_table[join_value]:
					hash_table[join_value].append(runtime_row_token)
	if isinstance(child_object.child,list):
		join_id = parent_subject.triples_map_id + "_" + child_object.child[0]
	else:
		join_id = "quoted_" + parent_subject.triples_map_id + "_" + child_object.child
	join_table.update({join_id : hash_table})

def hash_maker_list(parent_data, parent_subject, child_object):
	hash_table = {}
	global blank_message
	for row in parent_data:
		if sublist(child_object.parent,row.keys()):
			if child_list_value(child_object.parent,row) in hash_table:
				if duplicate == "yes":
					if parent_subject.subject_map.subject_mapping_type == "reference":
						value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
						if value != None:
							if "http" in value and "<" not in value:
								value = "<" + value[1:-1] + ">"
							elif "http" in value and "<" in value:
								value = value[1:-1] 
						hash_table[child_list_value(child_object.parent,row)].update({value : "object"})
					else:
						value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator)
						if value != None:
							if parent_subject.subject_map.term_type != None:
								if "BlankNode" in parent_subject.subject_map.term_type:
									if "/" in value:
										value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
										if "." in value:
											value = value.replace(".","2E")
										if blank_message:
											print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
											blank_message = False
									else:
										value = "_:" + encode_char(value).replace("%","")
										if "." in value:
											value = value.replace(".","2E")
							else:
								value = "<" + value + ">"
							hash_table[child_list_value(child_object.parent,row)].update({value: "object"})


				else:
					if parent_subject.subject_map.subject_mapping_type == "reference":
						value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1] 
						hash_table[child_list_value(child_object.parent,row)].update({value : "object"})
					else:
						value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator)
						if value != None:
							if parent_subject.subject_map.term_type != None:
								if "BlankNode" in parent_subject.subject_map.term_type:
									if "/" in value:
										value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
										if "." in value:
											value = value.replace(".","2E")
										if blank_message:
											print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
											blank_message = False
									else:
										value = "_:" + encode_char(value).replace("%","")
										if "." in value:
											value = value.replace(".","2E")
							else:
								value = "<" + value + ">"
							hash_table[child_list_value(child_object.parent,row)].update({value: "object"})

			else:
				if parent_subject.subject_map.subject_mapping_type == "reference":
					value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
					if value != None:
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1] 
					hash_table.update({child_list_value(child_object.parent,row) : {value : "object"}}) 
				else:
					value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator)
					if value != None:
						if parent_subject.subject_map.term_type != None:
							if "BlankNode" in parent_subject.subject_map.term_type:
								if "/" in value:
									value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
									if blank_message:
										print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
										blank_message = False
								else:
									value = "_:" + encode_char(value).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
						else:
							value = "<" + value + ">"
						hash_table.update({child_list_value(child_object.parent,row) : {value : "object"}})
	join_table.update({parent_subject.triples_map_id + "_" + child_list(child_object.child) : hash_table})


def _normalize_mapping_type(mappings_type):
	"""
	TM-PARSER-DEFAULT-ASSERTED

	Normalize the parsed triples-map type.

	Current policy:
	- explicit `NonAssertedTriplesMap` stays non-asserted
	- explicit asserted types stay as-is
	- missing type defaults to `AssertedTriplesMap`

	This matches the supervisor-approved interpretation that an untyped TM
	should be treated as asserted unless stated otherwise.
	"""
	if mappings_type is None:
		return "http://w3id.org/rml/AssertedTriplesMap"

	mappings_type_str = str(mappings_type)
	if mappings_type_str == "None":
		return "http://w3id.org/rml/AssertedTriplesMap"

	return mappings_type_str


def _build_triples_map_from_query_results(mapping_graph, mapping_query_prepared, triples_map_id):
	"""
	TM-PARSER-DEFAULT-ASSERTED

	Build one TriplesMap instance from a bound query over a specific TM id.
	This is shared by the main parser pass and the fallback discovery pass for
	untyped asserted TMs.
	"""
	mapping_query_prepared_results = list(
		mapping_graph.query(mapping_query_prepared, initBindings={'triples_map_id': triples_map_id})
	)
	if not mapping_query_prepared_results:
		return None

	result_triples_map = mapping_query_prepared_results[0]

	if result_triples_map.subject_template != None:
		if result_triples_map.rdf_class is None:
			reference, condition = string_separetion(str(result_triples_map.subject_template))
			subject_map = tm.SubjectMap(str(result_triples_map.subject_template), condition, "template", "None","None", [result_triples_map.rdf_class], result_triples_map.termtype, [result_triples_map.graph])
		else:
			reference, condition = string_separetion(str(result_triples_map.subject_template))
			subject_map = tm.SubjectMap(str(result_triples_map.subject_template), condition, "template", "None","None", [str(result_triples_map.rdf_class)], result_triples_map.termtype, [result_triples_map.graph])
	elif result_triples_map.subject_reference != None:
		if result_triples_map.rdf_class is None:
			reference, condition = string_separetion(str(result_triples_map.subject_reference))
			subject_map = tm.SubjectMap(str(result_triples_map.subject_reference), condition, "reference", "None","None", [result_triples_map.rdf_class], result_triples_map.termtype, [result_triples_map.graph])
		else:
			reference, condition = string_separetion(str(result_triples_map.subject_reference))
			subject_map = tm.SubjectMap(str(result_triples_map.subject_reference), condition, "reference", "None","None", [str(result_triples_map.rdf_class)], result_triples_map.termtype, [result_triples_map.graph])
	elif result_triples_map.subject_constant != None:
		if result_triples_map.rdf_class is None:
			reference, condition = string_separetion(str(result_triples_map.subject_constant))
			subject_map = tm.SubjectMap(str(result_triples_map.subject_constant), condition, "constant", "None","None", [result_triples_map.rdf_class], result_triples_map.termtype, [result_triples_map.graph])
		else:
			reference, condition = string_separetion(str(result_triples_map.subject_constant))
			subject_map = tm.SubjectMap(str(result_triples_map.subject_constant), condition, "constant", "None","None", [str(result_triples_map.rdf_class)], result_triples_map.termtype, [result_triples_map.graph])
	elif result_triples_map.subject_quoted != None:
		if result_triples_map.rdf_class is None:
			reference, condition = string_separetion(str(result_triples_map.subject_quoted))
			subject_map = tm.SubjectMap(str(result_triples_map.subject_quoted), condition, "quoted triples map", result_triples_map.subject_parent_value, result_triples_map.subject_child_value, [result_triples_map.rdf_class], result_triples_map.termtype, [result_triples_map.graph])
		else:
			reference, condition = string_separetion(str(result_triples_map.subject_quoted))
			subject_map = tm.SubjectMap(str(result_triples_map.subject_quoted), condition, "quoted triples map", result_triples_map.subject_parent_value, result_triples_map.subject_child_value, [str(result_triples_map.rdf_class)], result_triples_map.termtype, [result_triples_map.graph])
	else:
		return None

	join_predicate = {}
	predicate_object_maps_list = []
	predicate_object_graph = {}
	for result_predicate_object_map in mapping_query_prepared_results:
		join = True
		if result_predicate_object_map.predicate_constant != None:
			predicate_map = tm.PredicateMap("constant", str(result_predicate_object_map.predicate_constant), "")
			predicate_object_graph[str(result_predicate_object_map.predicate_constant)] = result_triples_map.predicate_object_graph
		elif result_predicate_object_map.predicate_constant_shortcut != None:
			predicate_map = tm.PredicateMap("constant shortcut", str(result_predicate_object_map.predicate_constant_shortcut), "")
			predicate_object_graph[str(result_predicate_object_map.predicate_constant_shortcut)] = result_triples_map.predicate_object_graph
		elif result_predicate_object_map.predicate_template != None:
			template, condition = string_separetion(str(result_predicate_object_map.predicate_template))
			predicate_map = tm.PredicateMap("template", template, condition)
		elif result_predicate_object_map.predicate_reference != None:
			reference, condition = string_separetion(str(result_predicate_object_map.predicate_reference))
			predicate_map = tm.PredicateMap("reference", reference, condition)
		else:
			predicate_map = tm.PredicateMap("None", "None", "None")

		if result_predicate_object_map.object_constant != None:
			object_map = tm.ObjectMap("constant", str(result_predicate_object_map.object_constant), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language,result_predicate_object_map.language_value)
		elif result_predicate_object_map.object_template != None:
			object_map = tm.ObjectMap("template", str(result_predicate_object_map.object_template), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language,result_predicate_object_map.language_value)
		elif result_predicate_object_map.object_reference != None:
			object_map = tm.ObjectMap("reference", str(result_predicate_object_map.object_reference), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language,result_predicate_object_map.language_value)
		elif result_predicate_object_map.object_quoted != None:
			object_map = tm.ObjectMap("quoted triples map", str(result_predicate_object_map.object_quoted), str(result_predicate_object_map.object_datatype), str(result_predicate_object_map.object_child_value), str(result_predicate_object_map.object_parent_value), result_predicate_object_map.term, result_predicate_object_map.language,result_predicate_object_map.language_value)
		elif result_predicate_object_map.object_parent_triples_map != None:
			if predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map) not in join_predicate:
				join_predicate[predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map)] = {"predicate":predicate_map, "childs":[str(result_predicate_object_map.child_value)], "parents":[str(result_predicate_object_map.parent_value)], "triples_map":str(result_predicate_object_map.object_parent_triples_map)}
			else:
				join_predicate[predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map)]["childs"].append(str(result_predicate_object_map.child_value))
				join_predicate[predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map)]["parents"].append(str(result_predicate_object_map.parent_value))
			join = False
		elif result_predicate_object_map.object_constant_shortcut != None:
			object_map = tm.ObjectMap("constant shortcut", str(result_predicate_object_map.object_constant_shortcut), "None", "None", "None", result_predicate_object_map.term, result_predicate_object_map.language,result_predicate_object_map.language_value)
		else:
			object_map = tm.ObjectMap("None", "None", "None", "None", "None", "None", "None", "None")
		if join:
			predicate_object_maps_list += [tm.PredicateObjectMap(predicate_map, object_map,predicate_object_graph)]

	if join_predicate:
		for jp in join_predicate.keys():
			object_map = tm.ObjectMap("parent triples map", join_predicate[jp]["triples_map"], str(result_predicate_object_map.object_datatype), join_predicate[jp]["childs"], join_predicate[jp]["parents"],result_predicate_object_map.term, result_predicate_object_map.language,result_predicate_object_map.language_value)
			predicate_object_maps_list += [tm.PredicateObjectMap(join_predicate[jp]["predicate"], object_map,predicate_object_graph)]

	return tm.TriplesMap(
		str(result_triples_map.triples_map_id),
		str(result_triples_map.data_source),
		subject_map,
		predicate_object_maps_list,
		ref_form=str(result_triples_map.ref_form),
		iterator=str(result_triples_map.iterator),
		tablename=str(result_triples_map.tablename),
		query=str(result_triples_map.query),
		mappings_type=_normalize_mapping_type(result_triples_map.mappings_type)
	)

def mapping_parser(mapping_file):

	"""
	(Private function, not accessible from outside this package)

	Takes a mapping file in Turtle (.ttl) or Notation3 (.n3) format and parses it into a list of
	TriplesMap objects (refer to TriplesMap.py file)

	Parameters
	----------
	mapping_file : string
		Path to the mapping file

	Returns
	-------
	A list of TriplesMap objects containing all the parsed rules from the original mapping file
	"""

	mapping_graph = rdflib.Graph()

	try:
		mapping_graph.parse(mapping_file, format='n3')
	except Exception as n3_mapping_parse_exception:
		print(n3_mapping_parse_exception)
		print('Could not parse {} as a mapping file'.format(mapping_file))
		print('Aborting...')
		sys.exit(1)

	mapping_query = """
		prefix rml: <http://www.w3.org/ns/r2rml#> 
		prefix rml: <http://w3id.org/rml/> 
		prefix ql: <http://semweb.mmlab.be/ns/ql#> 
		prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> 
		SELECT DISTINCT *
		WHERE {

	# Subject -------------------------------------------------------------------------
			OPTIONAL{?triples_map_id a ?mappings_type}
			?triples_map_id rml:logicalSource ?_source .
			OPTIONAL{?_source rml:source ?data_source .}
			OPTIONAL {?_source rml:referenceFormulation ?ref_form .}
			OPTIONAL { ?_source rml:iterator ?iterator . }
			OPTIONAL { ?_source rml:tableName ?tablename .}
			OPTIONAL { ?_source rml:query ?query .}

			?triples_map_id rml:subjectMap ?_subject_map .
			OPTIONAL {?_subject_map rml:template ?subject_template .}
			OPTIONAL {?_subject_map rml:reference ?subject_reference .}
			OPTIONAL {?_subject_map rml:constant ?subject_constant}
			OPTIONAL {?_subject_map rml:quotedTriplesMap ?subject_quoted .
				OPTIONAL {
					?_subject_map rml:joinCondition ?subject_join_condition .
					?subject_join_condition rml:child ?subject_child_value;
										 rml:parent ?subject_parent_value.
				}
			}
			OPTIONAL { ?_subject_map rml:class ?rdf_class . }
			OPTIONAL { ?_subject_map rml:termType ?termtype . }
			OPTIONAL { ?_subject_map rml:graph ?graph . }
			OPTIONAL { ?_subject_map rml:graphMap ?_graph_structure .
					   ?_graph_structure rml:constant ?graph . }
			OPTIONAL { ?_subject_map rml:graphMap ?_graph_structure .
					   ?_graph_structure rml:template ?graph . }		   

	# Predicate -----------------------------------------------------------------------
			OPTIONAL {
			?triples_map_id rml:predicateObjectMap ?_predicate_object_map .
			
			OPTIONAL {
				?triples_map_id rml:predicateObjectMap ?_predicate_object_map .
				?_predicate_object_map rml:predicateMap ?_predicate_map .
				?_predicate_map rml:constant ?predicate_constant .
			}
			OPTIONAL {
				?_predicate_object_map rml:predicateMap ?_predicate_map .
				?_predicate_map rml:template ?predicate_template .
			}
			OPTIONAL {
				?_predicate_object_map rml:predicateMap ?_predicate_map .
				?_predicate_map rml:reference ?predicate_reference .
			}
			OPTIONAL {
				?_predicate_object_map rml:predicate ?predicate_constant_shortcut .
			 }
			

	# Object --------------------------------------------------------------------------
			OPTIONAL {
				?_predicate_object_map rml:objectMap ?_object_map .
				?_object_map rml:constant ?object_constant .
				OPTIONAL {
					?_object_map rml:datatype ?object_datatype .
				}
			}
			OPTIONAL {
				?_predicate_object_map rml:objectMap ?_object_map .
				?_object_map rml:template ?object_template .
				OPTIONAL {?_object_map rml:termType ?term .}
				OPTIONAL {?_object_map rml:languageMap ?language_map.
						  ?language_map rml:reference ?language_value.}
				OPTIONAL {
					?_object_map rml:datatype ?object_datatype .
				}
			}
			OPTIONAL {
				?_predicate_object_map rml:objectMap ?_object_map .
				?_object_map rml:quotedTriplesMap ?object_quoted .
				OPTIONAL {
					?_object_map rml:joinCondition ?object_join_condition .
					?object_join_condition rml:child ?object_child_value;
										 rml:parent ?object_parent_value.
				}
			}
			OPTIONAL {
				?_predicate_object_map rml:objectMap ?_object_map .
				?_object_map rml:reference ?object_reference .
				OPTIONAL { ?_object_map rml:language ?language .}
				OPTIONAL {?_object_map rml:languageMap ?language_map.
						  ?language_map rml:reference ?language_value.}
				OPTIONAL {?_object_map rml:termType ?term .}
				OPTIONAL {
					?_object_map rml:datatype ?object_datatype .
				}
			}
			OPTIONAL {
				?_predicate_object_map rml:objectMap ?_object_map .
				?_object_map rml:parentTriplesMap ?object_parent_triples_map .
				OPTIONAL {
					?_object_map rml:joinCondition ?parent_join_condition .
					?parent_join_condition rml:child ?child_value;
										 rml:parent ?parent_value.
					OPTIONAL {?_object_map rml:termType ?term .}
				}
			}
			OPTIONAL {
				?_predicate_object_map rml:object ?object_constant_shortcut .
			}
			OPTIONAL {?_predicate_object_map rml:graph ?predicate_object_graph .}
			OPTIONAL { ?_predicate_object_map  rml:graphMap ?_graph_structure .
					   ?_graph_structure rml:constant ?predicate_object_graph  . }
			OPTIONAL { ?_predicate_object_map  rml:graphMap ?_graph_structure .
					   ?_graph_structure rml:template ?predicate_object_graph  . }	
			}
			OPTIONAL {
				?_source a d2rq:Database;
  				d2rq:jdbcDSN ?jdbcDSN; 
  				d2rq:jdbcDriver ?jdbcDriver; 
			    d2rq:username ?user;
			    d2rq:password ?password .
			}
		} """

	triples_map_list = []
	mapping_query_prepared = prepareQuery(mapping_query)
	discovery_query = """
		prefix rml: <http://w3id.org/rml/>
		SELECT DISTINCT ?triples_map_id
		WHERE {
			?triples_map_id rml:logicalSource ?_source .
			?triples_map_id rml:subjectMap ?_subject_map .
		}
	"""

	# TM-PARSER-DEFAULT-ASSERTED
	#
	# Discover candidate TM identifiers with the smallest reliable query, then
	# reuse the existing per-TM builder to fetch the full optional structure.
	# This keeps the parser behavior close to the original implementation while
	# avoiding the fragility of using the large all-in-one OPTIONAL query as the
	# only discovery mechanism for untyped/simple triples maps.
	for discovery_result in mapping_graph.query(discovery_query):
		current_triples_map = _build_triples_map_from_query_results(
			mapping_graph,
			mapping_query_prepared,
			discovery_result.triples_map_id
		)
		if current_triples_map is not None:
			triples_map_list += [current_triples_map]

	return triples_map_list

def semantify_file(triples_map, triples_map_list, delimiter, row, no_inner_cycle, runtime_row_token=None):
	global same_row_cache_hits
	global same_row_cache_misses
	global same_row_key_runtime_token_uses
	global same_row_key_fallback_uses
	global planner_same_row_cache_skips
	global planner_join_cache_skips
	global equivalent_statement_cache_hits
	global equivalent_statement_cache_misses
	global equivalent_statement_cache_populates
	global equivalent_statement_cache_owner_populates
	global equivalent_statement_cache_alias_populates
	global equivalent_statement_cache_alias_hits
	global equivalent_statement_cache_owner_hits
	global equivalent_join_cache_hits
	global equivalent_join_cache_misses
	global equivalent_join_cache_populates
	semantify_file_start = time.perf_counter() if show_time_breakdown else None
	debug_branch_label = None
	debug_object_list_size = 0
	debug_subject_list_size = 0
	object_list = []
	subject_list = []
	triples_list = []
	current_tm_owner_equivalences = {}
	current_tm_alias_equivalence = None
	equivalent_statement_cache_active = (
		enable_equivalent_statement_cache
		and active_planning_context is not None
		and active_planning_context.get("has_statement_equivalence_groups", False)
	)
	if equivalent_statement_cache_active:
		current_tm_owner_equivalences = active_planning_context.get("owner_tm_equivalence_index", {}).get(
			str(triples_map.triples_map_id),
			{}
		)
		current_tm_alias_equivalence = _planning_equivalent_alias_metadata(triples_map)
		if current_tm_alias_equivalence is not None and len(getattr(triples_map, "predicate_object_maps_list", [])) == 1:
			# Alias late-comer path:
			# the alias is a single-POM TM, so an equivalence hit represents the
			# whole alias result and may safely short-circuit the whole TM.
			_, cached_alias_payload = _equivalent_statement_cache_get(
				current_tm_alias_equivalence["canonical_owner_tm_id"],
				current_tm_alias_equivalence["canonical_owner_po_index"],
				triples_map.data_source,
				row,
				runtime_row_token
			)
			if cached_alias_payload is not None:
				equivalent_statement_cache_hits += 1
				equivalent_statement_cache_alias_hits += 1
				_backfill_annotated_result_cache_if_eligible(
					triples_map,
					runtime_row_token,
					cached_alias_payload,
					"equivalence"
				)
				if semantify_file_start is not None:
					_record_time_metric("semantify_file_total", time.perf_counter() - semantify_file_start)
				return list(cached_alias_payload)
			equivalent_statement_cache_misses += 1
	if triples_map.subject_map.subject_mapping_type == "template":
		subject_value = string_substitution(triples_map.subject_map.value, "{(.+?)}", row, "subject", ignore, triples_map.iterator)
		if triples_map.subject_map.term_type is None:
			if triples_map.subject_map.condition == "":

				try:
					subject = "<" + subject_value + ">"
				except:
					subject = None

			else:
			#	field, condition = condition_separetor(triples_map.subject_map.condition)
			#	if row[field] == condition:
				try:
					subject = "<" + subject_value  + ">"
				except:
					subject = None 
		else:
			if "IRI" in triples_map.subject_map.term_type:
				subject_value = string_substitution(triples_map.subject_map.value, "{(.+?)}", row, "subject", ignore, triples_map.iterator)
				if triples_map.subject_map.condition == "":

					try:
						if "http" not in subject_value:
							subject = "<" + base + subject_value + ">"
						else:
							subject = "<" + encode_char(subject_value) + ">"
					except:
						subject = None

				else:
				#	field, condition = condition_separetor(triples_map.subject_map.condition)
				#	if row[field] == condition:
					try:
						if "http" not in subject_value:
							subject = subject = "<" + base + subject_value + ">"
						else:
							subject = "<" + subject_value + ">"
					except:
						subject = None 

			elif "BlankNode" in triples_map.subject_map.term_type:
				if triples_map.subject_map.condition == "":
					try:
						if "/" in subject_value:
							subject  = "_:" + encode_char(subject_value.replace("/","2F")).replace("%","")
							if "." in subject:
								subject = subject.replace(".","2E")
							if blank_message:
								print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
								blank_message = False
						else:
							subject = "_:" + encode_char(subject_value).replace("%","")
							if "." in subject:
								subject = subject.replace(".","2E")
					except:
						subject = None

				else:
				#	field, condition = condition_separetor(triples_map.subject_map.condition)
				#	if row[field] == condition:
					try:
						subject = "_:" + subject_value  
					except:
						subject = None
			elif "Literal" in triples_map.subject_map.term_type:
				subject = None			
			else:
				if triples_map.subject_map.condition == "":

					try:
						subject = "<" + subject_value + ">"
					except:
						subject = None

				else:
				#	field, condition = condition_separetor(triples_map.subject_map.condition)
				#	if row[field] == condition:
					try:
						subject = "<" + subject_value + ">"
					except:
						subject = None 
	elif "reference" in triples_map.subject_map.subject_mapping_type:
		subject_value = string_substitution(triples_map.subject_map.value, ".+", row, "subject",ignore , triples_map.iterator)
		if "BlankNode" not in triples_map.subject_map.term_type :
			if subject_value != None:
				subject_value = subject_value[1:-1]
				if triples_map.subject_map.condition == "":
					if " " not in subject_value:
						if "http" not in subject_value:
							subject = "<" + base + subject_value + ">"
						else:
							subject = "<" + subject_value + ">"
					else:
						subject = None

			else:
			#	field, condition = condition_separetor(triples_map.subject_map.condition)
			#	if row[field] == condition:
				try:
					if "http" not in subject_value:
						subject = "<" + base + subject_value + ">"
					else:
						subject = "<" + subject_value + ">"
				except:
					subject = None
		else:
			if subject_value != None:
				subject = "_:" + subject_value[1:-1]
			else:
				subject = None

	elif "constant" in triples_map.subject_map.subject_mapping_type:
		subject = "<" + subject_value + ">"

	elif "quoted triples map" in triples_map.subject_map.subject_mapping_type:
		for triples_map_element in triples_map_list:
			if triples_map_element.triples_map_id != triples_map.subject_map.value:
				continue

			if triples_map_element.data_source != triples_map.data_source:
				if triples_map.subject_map.parent != None:
					alias_metadata = _planning_equivalent_alias_metadata(triples_map_element)
					join_fields = triples_map.subject_map.child
					join_value = _resolve_join_value_from_row(row, join_fields)
					if join_value is None:
						subject_list = []
						subject = None
						break
					resolved_subject_payload = None
					if alias_metadata is not None:
						_, _, cached_subject_payload = _resolve_equivalent_join_payload(
							alias_metadata["canonical_owner_tm_id"],
							alias_metadata["canonical_owner_po_index"],
							join_fields,
							join_value
						)
						if cached_subject_payload is not None:
							equivalent_join_cache_hits += 1
							resolved_subject_payload = list(cached_subject_payload)
						else:
							equivalent_join_cache_misses += 1
					if resolved_subject_payload is None:
						join_key = _ensure_quoted_join_index(
							triples_map_element,
							join_fields,
							"quoted_subject_join_build",
							triples_map_list
						)
						if not _should_use_join_cache(triples_map_element) and enable_join_quoted_cache:
							planner_join_cache_skips += 1
						resolved_subject_payload = _resolve_quoted_join_payload_from_tokens(
							triples_map_element,
							join_key,
							join_value
						)
						if alias_metadata is not None and resolved_subject_payload:
							row_tokens = join_table.get(join_key, {}).get(join_value)
							if row_tokens is None:
								row_tokens = []
							_equivalent_join_cache_set(
								alias_metadata["group_id"],
								alias_metadata["canonical_owner_tm_id"],
								alias_metadata["canonical_owner_po_index"],
								join_fields,
								join_value,
								list(row_tokens)
							)
							equivalent_join_cache_populates += 1
					subject_list = resolved_subject_payload if resolved_subject_payload is not None else []
			else:
				if _should_use_same_row_cache(triples_map_element):
					subject_list, cache_hit = _get_or_build_annotated_result(
						triples_map_element,
						triples_map_list,
						delimiter,
						row,
						runtime_row_token,
						"quoted_subject_same_row_miss_build",
						"same_row"
					)
					if cache_hit:
						same_row_cache_hits += 1
					else:
						same_row_cache_misses += 1
				else:
					if enable_same_row_quoted_cache:
						planner_same_row_cache_skips += 1
					with _time_metric("quoted_subject_recursive_no_cache"):
						subject_list = [
							triples
							for triples in semantify_file(
								triples_map_element,
								triples_map_list,
								delimiter,
								row,
								False,
								runtime_row_token
							)
							if triples is not None
						]
			subject = None
			break

	else:
		if triples_map.subject_map.condition == "":

			try:
				subject = "\"" + triples_map.subject_map.value + "\""
			except:
				subject = None

		else:
		#	field, condition = condition_separetor(triples_map.subject_map.condition)
		#	if row[field] == condition:
			try:
				subject = "\"" + triples_map.subject_map.value + "\""
			except:
				subject = None


	if triples_map.subject_map.rdf_class != None and subject != None:
		predicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
		for rdf_class in triples_map.subject_map.rdf_class:
			if rdf_class != None:
				obj = "<{}>".format(rdf_class)
				dictionary_table_update(subject)
				dictionary_table_update(obj)
				dictionary_table_update(predicate + "_" + obj)
				rdf_type = subject + " " + predicate + " " + obj + ".\n"
				for graph in triples_map.subject_map.graph:
					if graph != None and "defaultGraph" not in graph:
						if "{" in graph:	
							rdf_type = rdf_type[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + "> .\n"
							dictionary_table_update("<" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
						else:
							rdf_type = rdf_type[:-2] + " <" + graph + "> .\n"
							dictionary_table_update("<" + graph + ">")
				if no_inner_cycle:			
					if duplicate == "yes":
						if dic_table[predicate + "_" + obj] not in g_triples:
							triples_list.append(rdf_type)
							g_triples.update({dic_table[predicate  + "_" + obj ] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
						elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + obj]]:
							triples_list.append(rdf_type)
							g_triples[dic_table[predicate + "_" + obj]].update({dic_table[subject] + "_" + dic_table[obj] : ""})
					else:
						triples_list.append(rdf_type)
				else:
					triples_list.append(rdf_type)

	
	for pom_index, predicate_object_map in enumerate(triples_map.predicate_object_maps_list):
		if current_tm_owner_equivalences:
			owner_equivalence = current_tm_owner_equivalences.get(pom_index)
			if owner_equivalence is not None:
				# Owner late-comer path:
				# this hit covers exactly one owner POM, so only that POM is skipped;
				# the rest of the owner TM still materializes normally.
				_, cached_owner_payload = _equivalent_statement_cache_get(
					owner_equivalence["canonical_owner_tm_id"],
					owner_equivalence["canonical_owner_po_index"],
					triples_map.data_source,
					row,
					runtime_row_token
				)
				if cached_owner_payload is not None:
					equivalent_statement_cache_hits += 1
					equivalent_statement_cache_owner_hits += 1
					triples_list.extend(list(cached_owner_payload))
					continue
				equivalent_statement_cache_misses += 1
		# Initialize per-POM object state defensively so unmatched parent/quoted
		# branches leave a clean `None`/empty value instead of reusing or missing
		# prior state.
		object = None
		object_list = []
		pom_output_start = len(triples_list)
		if predicate_object_map.predicate_map.mapping_type == "constant" or predicate_object_map.predicate_map.mapping_type == "constant shortcut":
			predicate = "<" + predicate_object_map.predicate_map.value + ">"
		elif predicate_object_map.predicate_map.mapping_type == "template":
			if predicate_object_map.predicate_map.condition != "":
					#field, condition = condition_separetor(predicate_object_map.predicate_map.condition)
					#if row[field] == condition:
					try:
						predicate = "<" + string_substitution(predicate_object_map.predicate_map.value, "{(.+?)}", row, "predicate",ignore, triples_map.iterator) + ">"
					except:
						predicate = None
					#else:
					#	predicate = None
			else:
				try:
					predicate = "<" + string_substitution(predicate_object_map.predicate_map.value, "{(.+?)}", row, "predicate",ignore, triples_map.iterator) + ">"
				except:
					predicate = None
		elif predicate_object_map.predicate_map.mapping_type == "reference":
			if predicate_object_map.predicate_map.condition != "":
				#field, condition = condition_separetor(predicate_object_map.predicate_map.condition)
				#if row[field] == condition:
				predicate = string_substitution(predicate_object_map.predicate_map.value, ".+", row, "predicate",ignore, triples_map.iterator)
				#else:
				#	predicate = None
			else:
				predicate = string_substitution(predicate_object_map.predicate_map.value, ".+", row, "predicate",ignore, triples_map.iterator)
			predicate = "<" + predicate[1:-1] + ">"
		else:
			predicate = None

		if predicate_object_map.object_map.mapping_type == "constant" or predicate_object_map.object_map.mapping_type == "constant shortcut":
			if "/" in predicate_object_map.object_map.value:
				object = "<" + predicate_object_map.object_map.value + ">"
			else:
				object = "\"" + predicate_object_map.object_map.value + "\""
			if predicate_object_map.object_map.datatype != None:
				object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
		elif predicate_object_map.object_map.mapping_type == "template":
			try:
				if predicate_object_map.object_map.term is None:
					object = "<" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row, "object",ignore, triples_map.iterator) + ">"
				elif "IRI" in predicate_object_map.object_map.term:
					object = "<" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row, "object",ignore, triples_map.iterator) + ">"
				elif "BlankNode" in predicate_object_map.object_map.term:
					object = "_:" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row, "object",ignore, triples_map.iterator)
					if "/" in object:
						object  = object.replace("/","2F")
						if blank_message:
							print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
							blank_message = False
					if "." in object:
						object = object.replace(".","2E")
					object = encode_char(object)
				else:
					object = "\"" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row, "object",ignore, triples_map.iterator) + "\""
					if predicate_object_map.object_map.datatype != None:
						object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
					elif predicate_object_map.object_map.language != None:
						if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
							object += "@es"
						elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
							object += "@en"
						elif len(predicate_object_map.object_map.language) == 2:
							object += "@"+predicate_object_map.object_map.language
					elif predicate_object_map.object_map.language_map != None:
						lang = string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object",ignore, triples_map.iterator)
						if lang != None:
							object += "@" + string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object",ignore, triples_map.iterator)[1:-1]  
			except TypeError:
				object = None
		elif predicate_object_map.object_map.mapping_type == "reference":
			object = string_substitution(predicate_object_map.object_map.value, ".+", row, "object",ignore, triples_map.iterator)
			if object != None:
				if "\\" in object[1:-1]:
					object = "\"" + object[1:-1].replace("\\","\\\\") + "\""
				if "'" in object[1:-1]:
					object = "\"" + object[1:-1].replace("'","\\\\'") + "\""
				if "\n" in object:
					object = object.replace("\n","\\n")
				if predicate_object_map.object_map.datatype != None:
					object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
				elif predicate_object_map.object_map.language != None:
					if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
						object += "@es"
					elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
						object += "@en"
					elif len(predicate_object_map.object_map.language) == 2:
						object += "@"+predicate_object_map.object_map.language
				elif predicate_object_map.object_map.language_map != None:
					lang = string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object",ignore, triples_map.iterator)
					if lang != None:
						object += "@"+ string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object",ignore, triples_map.iterator)[1:-1]
				elif predicate_object_map.object_map.term != None:
					if "IRI" in predicate_object_map.object_map.term:
						if " " not in object:
							object = "\"" + object[1:-1].replace("\\\\'","'") + "\""
							object = "<" + encode_char(object[1:-1]) + ">"
						else:
							object = None
					elif "BlankNode" in predicate_object_map.object_map.term:
						object = "_:" + object[1:-1]
		elif "quoted triples map" in predicate_object_map.object_map.mapping_type:
			for triples_map_element in triples_map_list:
				if triples_map_element.triples_map_id != predicate_object_map.object_map.value:
					continue

				if triples_map_element.data_source != triples_map.data_source:
					if predicate_object_map.object_map.parent != None:
						alias_metadata = _planning_equivalent_alias_metadata(triples_map_element)
						join_fields = predicate_object_map.object_map.child
						join_value = _resolve_join_value_from_row(row, join_fields)
						if join_value is None:
							object_list = []
							object = None
							break
						resolved_object_payload = None
						if alias_metadata is not None:
							_, _, cached_object_payload = _resolve_equivalent_join_payload(
								alias_metadata["canonical_owner_tm_id"],
								alias_metadata["canonical_owner_po_index"],
								join_fields,
								join_value
							)
							if cached_object_payload is not None:
								equivalent_join_cache_hits += 1
								resolved_object_payload = list(cached_object_payload)
							else:
								equivalent_join_cache_misses += 1
						if resolved_object_payload is None:
							join_key = _ensure_quoted_join_index(
								triples_map_element,
								join_fields,
								"quoted_object_join_build",
								triples_map_list
							)
							if not _should_use_join_cache(triples_map_element) and enable_join_quoted_cache:
								planner_join_cache_skips += 1
							resolved_object_payload = _resolve_quoted_join_payload_from_tokens(
								triples_map_element,
								join_key,
								join_value
							)
							if alias_metadata is not None and resolved_object_payload:
								row_tokens = join_table.get(join_key, {}).get(join_value)
								if row_tokens is None:
									row_tokens = []
								_equivalent_join_cache_set(
									alias_metadata["group_id"],
									alias_metadata["canonical_owner_tm_id"],
									alias_metadata["canonical_owner_po_index"],
									join_fields,
									join_value,
									list(row_tokens)
								)
								equivalent_join_cache_populates += 1
						object_list = resolved_object_payload if resolved_object_payload is not None else []
						debug_object_list_size = max(debug_object_list_size, len(object_list))
				else:
					if _should_use_same_row_cache(triples_map_element):
						object_list, cache_hit = _get_or_build_annotated_result(
							triples_map_element,
							triples_map_list,
							delimiter,
							row,
							runtime_row_token,
							"quoted_object_same_row_miss_build",
							"same_row"
						)
						if cache_hit:
							same_row_cache_hits += 1
							debug_object_list_size = max(debug_object_list_size, len(object_list))
						else:
							same_row_cache_misses += 1
							debug_object_list_size = max(debug_object_list_size, len(object_list))
					else:
						if enable_same_row_quoted_cache:
							planner_same_row_cache_skips += 1
						with _time_metric("quoted_object_recursive_no_cache"):
							object_list = [
								triples
								for triples in semantify_file(
									triples_map_element,
									triples_map_list,
									delimiter,
									row,
									False,
									runtime_row_token
								)
								if triples is not None
							]
						debug_object_list_size = max(debug_object_list_size, len(object_list))
				object = None
				break
		elif predicate_object_map.object_map.mapping_type == "parent triples map":
			if subject != None:
				debug_branch_label = "parent_triples_map"
				for triples_map_inner in triples_map_list:
					if triples_map_inner.triples_map_id == predicate_object_map.object_map.value:
						if triples_map.data_source != triples_map_inner.data_source:
							if len(predicate_object_map.object_map.child) == 1:
								if (triples_map_inner.triples_map_id + "_" + predicate_object_map.object_map.child[0]) not in join_table:
									if str(triples_map_inner.file_format).lower() == "csv" or triples_map_inner.file_format == "JSONPath":
										with open(str(triples_map_inner.data_source), "r") as input_file_descriptor:
											if str(triples_map_inner.file_format).lower() == "csv":
												data = _load_csv_rows(str(triples_map_inner.data_source), drop_duplicates=True)
												hash_maker(data, triples_map_inner, predicate_object_map.object_map,"", triples_map_list)
											else:
												data = json.load(input_file_descriptor)
												if triples_map_inner.iterator:
													if triples_map_inner.iterator != "None" and triples_map_inner.iterator != "$.[*]":
														join_iterator(data, triples_map_inner.iterator, triples_map_inner, predicate_object_map.object_map)
													else:
														if isinstance(data, list):
															hash_maker(data, triples_map_inner, predicate_object_map.object_map,"", triples_map_list)
														elif len(data) < 2:
															hash_maker(data[list(data.keys())[0]], triples_map_inner, predicate_object_map.object_map,"", triples_map_list)
												else:
													if isinstance(data, list):
														hash_maker(data, triples_map_inner, predicate_object_map.object_map,"", triples_map_list)
													elif len(data) < 2:
														hash_maker(data[list(data.keys())[0]], triples_map_inner, predicate_object_map.object_map,"", triples_map_list)

									elif triples_map_inner.file_format == "XPath":
										with open(str(triples_map_inner.data_source), "r") as input_file_descriptor:
											child_tree = ET.parse(input_file_descriptor)
											child_root = child_tree.getroot()
											hash_maker_xml(child_root, triples_map_inner, predicate_object_map.object_map)								
									else:
										database, query_list = translate_sql(triples_map)
										db = connector.connect(host=host, port=int(port), user=user, password=password)
										cursor = db.cursor(buffered=True)
										cursor.execute("use " + database)
										for query in query_list:
											cursor.execute(query)
										hash_maker_array(cursor, triples_map_inner, predicate_object_map.object_map)

								if sublist(predicate_object_map.object_map.child,row.keys()):
									if child_list_value(predicate_object_map.object_map.child,row) in join_table[triples_map_inner.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)]:
										object_list = join_table[triples_map_inner.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)][child_list_value(predicate_object_map.object_map.child,row)]
										debug_object_list_size = max(debug_object_list_size, len(object_list))
										debug_branch_label = "parent_triples_map_hit"
									else:
										debug_branch_label = "parent_triples_map_lookup_miss"
										if no_update:
											if str(triples_map_inner.file_format).lower() == "csv" or triples_map_inner.file_format == "JSONPath":
												with open(str(triples_map_inner.data_source), "r") as input_file_descriptor:
													if str(triples_map_inner.file_format).lower() == "csv":
														data = _load_csv_rows(str(triples_map_inner.data_source), drop_duplicates=True)
														hash_update(data, triples_map_inner, predicate_object_map.object_map, triples_map_inner.triples_map_id + "_" + predicate_object_map.object_map.child[0])
													else:
														data = json.load(input_file_descriptor)
														if triples_map_inner.iterator:
															if triples_map_inner.iterator != "None" and triples_map_inner.iterator != "$.[*]":
																join_iterator(data, triples_map_inner.iterator, triples_map_inner, predicate_object_map.object_map)
															else:
																if isinstance(data, list):
																	hash_maker(data, triples_map_inner, predicate_object_map.object_map,"", triples_map_list)
																elif len(data) < 2:
																	hash_maker(data[list(data.keys())[0]], triples_map_inner, predicate_object_map.object_map,"", triples_map_list)
														else:
															if isinstance(data, list):
																hash_maker(data, triples_map_inner, predicate_object_map.object_map,"", triples_map_list)
															elif len(data) < 2:
																hash_maker(data[list(data.keys())[0]], triples_map_inner, predicate_object_map.object_map,"", triples_map_list)
											if child_list_value(predicate_object_map.object_map.child,row) in join_table[triples_map_inner.triples_map_id + "_" + predicate_object_map.object_map.child[0]]:
												object_list = join_table[triples_map_inner.triples_map_id + "_" + predicate_object_map.object_map.child[0]][row[predicate_object_map.object_map.child[0]]]
												debug_object_list_size = max(debug_object_list_size, len(object_list))
												debug_branch_label = "parent_triples_map_hit"
											else:
												object_list = []
												debug_branch_label = "parent_triples_map_lookup_miss"
											no_update = False
								object = None
							else:
								if (triples_map_inner.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)) not in join_table:
									if str(triples_map_inner.file_format).lower() == "csv" or triples_map_inner.file_format == "JSONPath":
										if str(triples_map_inner.file_format).lower() == "csv":
											with _stream_csv_rows(str(triples_map_inner.data_source), delimiter=delimiter) as data:
												hash_maker_list(data, triples_map_inner, predicate_object_map.object_map)
										else:
											with open(str(triples_map_inner.data_source), "r") as input_file_descriptor:
												data = json.load(input_file_descriptor)
												if isinstance(data, list):
													hash_maker_list(data, triples_map_inner, predicate_object_map.object_map)
												elif len(data) < 2:
													hash_maker_list(data[list(data.keys())[0]], triples_map_inner, predicate_object_map.object_map)

									elif triples_map_inner.file_format == "XPath":
										with open(str(triples_map_inner.data_source), "r") as input_file_descriptor:
											child_tree = ET.parse(input_file_descriptor)
											child_root = child_tree.getroot()
											hash_maker_xml(child_root, triples_map_inner, predicate_object_map.object_map)						
									else:
										database, query_list = translate_sql(triples_map)
										db = connector.connect(host=host, port=int(port), user=user, password=password)
										cursor = db.cursor(buffered=True)
										cursor.execute("use " + database)
										for query in query_list:
											cursor.execute(query)
										hash_maker_array(cursor, triples_map_inner, predicate_object_map.object_map)
								if sublist(predicate_object_map.object_map.child,row.keys()):
									if child_list_value(predicate_object_map.object_map.child,row) in join_table[triples_map_inner.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)]:
										object_list = join_table[triples_map_inner.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)][child_list_value(predicate_object_map.object_map.child,row)]
										debug_object_list_size = max(debug_object_list_size, len(object_list))
										debug_branch_label = "parent_triples_map_hit"
									else:
										object_list = []
										debug_branch_label = "parent_triples_map_lookup_miss"
								object = None
						else:
							if predicate_object_map.object_map.parent != None:
								if predicate_object_map.object_map.parent[0] != predicate_object_map.object_map.child[0]:
									if (triples_map_inner.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)) not in join_table:
										if str(triples_map_inner.file_format).lower() == "csv":
											with _stream_csv_rows(str(triples_map_inner.data_source), delimiter=delimiter) as parent_data:
												hash_maker_list(parent_data, triples_map_inner, predicate_object_map.object_map)
										else:
											with open(str(triples_map_inner.data_source), "r") as input_file_descriptor:
												parent_data = json.load(input_file_descriptor)
												if isinstance(parent_data, list):
													hash_maker_list(parent_data, triples_map_inner, predicate_object_map.object_map)
												else:
													hash_maker_list(parent_data[list(parent_data.keys())[0]], triples_map_inner, predicate_object_map.object_map)
									if sublist(predicate_object_map.object_map.child,row.keys()):
										if child_list_value(predicate_object_map.object_map.child,row) in join_table[triples_map_inner.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)]:
											object_list = join_table[triples_map_inner.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)][child_list_value(predicate_object_map.object_map.child,row)]
											debug_object_list_size = max(debug_object_list_size, len(object_list))
											debug_branch_label = "parent_triples_map_hit"
										else:
											object_list = []
											debug_branch_label = "parent_triples_map_lookup_miss"
									object = None
								else:
									try:
										object = "<" + string_substitution(triples_map_element.subject_map.value, "{(.+?)}", row, "object",ignore, triples_map.iterator) + ">"
									except TypeError:
										object = None
							else:
								try:
									object = "<" + string_substitution(triples_map_element.subject_map.value, "{(.+?)}", row, "object",ignore, triples_map.iterator) + ">"
								except TypeError:
									object = None
						break
					else:
						continue
			else:
				object = None
		else:
			object = None

		if predicate in general_predicates:
			dictionary_table_update(predicate + "_" + predicate_object_map.object_map.value)
		else:
			dictionary_table_update(predicate)
		if predicate != None and object != None and subject != None:
			dictionary_table_update(subject)
			dictionary_table_update(object)
			for graph in triples_map.subject_map.graph:
				triple = subject + " " + predicate + " " + object + ".\n"
				if graph != None and "defaultGraph" not in graph:
					if "{" in graph:
						triple = triple[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">.\n"
						dictionary_table_update("<" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
					else:
						triple = triple[:-2] + " <" + graph + ">.\n"
						dictionary_table_update("<" + graph + ">")
				if no_inner_cycle:
					if duplicate == "yes":
						if predicate in general_predicates:
							if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
								triples_list.append(triple)
								g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[object]: ""}})
							elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
								triples_list.append(triple)
								g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[object]: ""})
						else:
							if dic_table[predicate] not in g_triples:					
								triples_list.append(triple)
								g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[object]: ""}})
							elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
								triples_list.append(triple)
								g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[object]: ""})
					else:
						triples_list.append(triple)
				else:
					triples_list.append(triple)
			if predicate[1:-1] in predicate_object_map.graph:
				triple = subject + " " + predicate + " " + object + ".\n"
				triple_hdt = dic_table[subject] + " " + dic_table[predicate] + " " + dic_table[object] + ".\n"
				if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
					if "{" in predicate_object_map.graph[predicate[1:-1]]:
						triple = triple[:-2] + " <" + string_substitution(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">.\n"
						dictionary_table_update("<" + string_substitution(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
					else:
						triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
						dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
					if no_inner_cycle:
						if duplicate == "yes":
							if predicate in general_predicates:
								if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
									triples_list.append(triple)
									g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[object]: ""}})
								elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
									triples_list.append(triple)
									g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[object]: ""})
							else:
								if dic_table[predicate] not in g_triples:					
									triples_list.append(triple)
									g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[object]: ""}})
								elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
									triples_list.append(triple)
									g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[object]: ""})
						else:
							triples_list.append(triple)
					else:
						triples_list.append(triple)
		elif predicate != None and subject != None and object_list:
			dictionary_table_update(subject)
			for obj in object_list:
				dictionary_table_update(obj)
				if obj != None:
					for graph in triples_map.subject_map.graph:
						if "quoted triples map" in predicate_object_map.object_map.mapping_type:
							triple = subject + " " + predicate + " << " + obj[:-2] + " >>.\n"
						else:
							if predicate_object_map.object_map.term != None:
								if "IRI" in predicate_object_map.object_map.term:
									triple = subject + " " + predicate + " <" + obj[1:-1] + ">.\n"
								else:
									triple = subject + " " + predicate + " " + obj + ".\n"
							else:
								triple = subject + " " + predicate + " " + obj + ".\n"
						if graph != None and "defaultGraph" not in graph:
							if "{" in graph:
								triple = triple[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">.\n"
								dictionary_table_update("<" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
							else:
								triple = triple[:-2] + " <" + graph + ">.\n"
								dictionary_table_update("<" + graph + ">")
						if no_inner_cycle:
							if duplicate == "yes":
								if predicate in general_predicates:
									if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
										output_file_descriptor.write(triple)
										g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
									elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
										triples_list.append(triple)
										g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
								else:
									if dic_table[predicate] not in g_triples:
										triples_list.append(triple)
										g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
									elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
										triples_list.append(triple)
										g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
							else:
								triples_list.append(triple)
						else:
							triples_list.append(triple)

					if predicate[1:-1] in predicate_object_map.graph:
						if "quoted triples map" in predicate_object_map.object_map.mapping_type:
							triple = subject + " " + predicate + " << " + obj[:-2] + " >>.\n"
						else:
							if predicate_object_map.object_map.term != None:
								if "IRI" in predicate_object_map.object_map.term:
									triple = subject + " " + predicate + " <" + obj[1:-1] + ">.\n"
								else:
									triple = subject + " " + predicate + " " + obj + ".\n"
							else:
								triple = subject + " " + predicate + " " + obj + ".\n"
						if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
							if "{" in predicate_object_map.graph[predicate[1:-1]]:
								triple = triple[:-2] + " <" + string_substitution(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">.\n"
								dictionary_table_update("<" + string_substitution(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
							else:
								triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
								dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
							if no_inner_cycle:
								if duplicate == "yes":
									if predicate in general_predicates:
										if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
											output_file_descriptor.write(triple)
											g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
										elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
											triples_list.append(triple)
											g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
									else:
										if dic_table[predicate] not in g_triples:
											triples_list.append(triple)
											g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
										elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
											triples_list.append(triple)
											g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
								else:
									triples_list.append(triple)
							else:
								triples_list.append(triple)
			object_list = []
		elif predicate != None and object != None and subject_list:
			debug_subject_list_size = max(debug_subject_list_size, len(subject_list))
			dictionary_table_update(object)
			for subj in subject_list:
				dictionary_table_update(subj)
				if subj != None:
					for graph in triples_map.subject_map.graph:
						triple = "<< " + subj[:-2] + " >> " + predicate + " " + object + ".\n"
						if graph != None and "defaultGraph" not in graph:
							if "{" in graph:
								triple = triple[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">.\n"
								dictionary_table_update("<" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
							else:
								triple = triple[:-2] + " <" + graph + ">.\n"
								dictionary_table_update("<" + graph + ">")
						if no_inner_cycle:
							if duplicate == "yes":
								if predicate in general_predicates:
									if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
										output_file_descriptor.write(triple)
										g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subj] + "_" + dic_table[object]: ""}})
									elif dic_table[subj] + "_" + dic_table[object] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
										triples_list.append(triple)
										g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subj] + "_" + dic_table[object]: ""})
								else:
									if dic_table[predicate] not in g_triples:
										triples_list.append(triple)
										g_triples.update({dic_table[predicate] : {dic_table[subj] + "_" + dic_table[object]: ""}})
									elif dic_table[subj] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
										triples_list.append(triple)
										g_triples[dic_table[predicate]].update({dic_table[subj] + "_" + dic_table[object]: ""})
							else:
								triples_list.append(triple)
						else:
							triples_list.append(triple)

					if predicate[1:-1] in predicate_object_map.graph:
						triple = "<< " + subj[:-2] + " >> " + predicate + " " + object + ".\n"
						if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
							if "{" in predicate_object_map.graph[predicate[1:-1]]:
								triple = triple[:-2] + " <" + string_substitution(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">.\n"
								dictionary_table_update("<" + string_substitution(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
							else:
								triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
								dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
							if no_inner_cycle:
								if duplicate == "yes":
									if predicate in general_predicates:
										if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
											output_file_descriptor.write(triple)
											g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subj] + "_" + dic_table[object]: ""}})
										elif dic_table[subj] + "_" + dic_table[object] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
											triples_list.append(triple)
											g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subj] + "_" + dic_table[object]: ""})
									else:
										if dic_table[predicate] not in g_triples:
											triples_list.append(triple)
											g_triples.update({dic_table[predicate] : {dic_table[subj] + "_" + dic_table[object]: ""}})
										elif dic_table[subj] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
											triples_list.append(triple)
											g_triples[dic_table[predicate]].update({dic_table[subj] + "_" + dic_table[object]: ""})
								else:
									triples_list.append(triple)
							else:
								triples_list.append(triple)
		#	subject_list = []
		elif predicate != None and object_list and subject_list:
			debug_subject_list_size = max(debug_subject_list_size, len(subject_list))
			debug_object_list_size = max(debug_object_list_size, len(object_list))
			for subj in subject_list:
				dictionary_table_update(subj)
				for obj in object_list:
					dictionary_table_update(obj)
					if subj != None:
						for graph in triples_map.subject_map.graph:
							if "quoted triples map" in predicate_object_map.object_map.mapping_type:
								triple = "<< " + subj[:-2] + " >> " + predicate + " << " + obj[:-2] + " >>.\n"
							else:
								triple = "<< " + subj[:-2] + " >> " + predicate + " " + obj + ".\n"
							if graph != None and "defaultGraph" not in graph:
								if "{" in graph:
									triple = triple[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">.\n"
									dictionary_table_update("<" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
								else:
									triple = triple[:-2] + " <" + graph + ">.\n"
									dictionary_table_update("<" + graph + ">")
							if no_inner_cycle:
								if duplicate == "yes":
									if predicate in general_predicates:
										if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
											output_file_descriptor.write(triple)
											g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subj] + "_" + dic_table[obj]: ""}})
										elif dic_table[subj] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
											triples_list.append(triple)
											g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subj] + "_" + dic_table[obj]: ""})
									else:
										if dic_table[predicate] not in g_triples:
											triples_list.append(triple)
											g_triples.update({dic_table[predicate] : {dic_table[subj] + "_" + dic_table[obj]: ""}})
										elif dic_table[subj] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
											triples_list.append(triple)
											g_triples[dic_table[predicate]].update({dic_table[subj] + "_" + dic_table[obj]: ""})
								else:
									triples_list.append(triple)
							else:
								triples_list.append(triple)

						if predicate[1:-1] in predicate_object_map.graph:
							if "quoted triples map" in predicate_object_map.object_map.mapping_type:
								triple = "<< " + subj[:-2] + " >> " + predicate + " << " + obj[:-2] + " >>.\n"
							else:
								triple = "<< " + subj[:-2] + " >> " + predicate + " " + obj + ".\n"
							if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
								if "{" in predicate_object_map.graph[predicate[1:-1]]:
									triple = triple[:-2] + " <" + string_substitution(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">.\n"
									dictionary_table_update("<" + string_substitution(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
								else:
									triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
									dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
								if no_inner_cycle:
									if duplicate == "yes":
										if predicate in general_predicates:
											if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
												output_file_descriptor.write(triple)
												g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subj] + "_" + dic_table[obj]: ""}})
											elif dic_table[subj] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
												triples_list.append(triple)
												g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subj] + "_" + dic_table[obj]: ""})
										else:
											if dic_table[predicate] not in g_triples:
												triples_list.append(triple)
												g_triples.update({dic_table[predicate] : {dic_table[subj] + "_" + dic_table[obj]: ""}})
											elif dic_table[subj] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
												triples_list.append(triple)
												g_triples[dic_table[predicate]].update({dic_table[subj] + "_" + dic_table[obj]: ""})
									else:
										triples_list.append(triple)
								else:
									triples_list.append(triple)
			# Keep the resolved quoted-subject context alive across later POMs in
			# the same TM. Only the object payload is POM-local here.
			object_list = []
		else:
			continue
		pom_output_payload = list(triples_list[pom_output_start:])
		if pom_output_payload:
			owner_equivalence = current_tm_owner_equivalences.get(pom_index)
			if owner_equivalence is not None:
				_equivalent_statement_cache_set(
					owner_equivalence["group_id"],
					owner_equivalence["canonical_owner_tm_id"],
					owner_equivalence["canonical_owner_po_index"],
					triples_map.data_source,
					row,
					pom_output_payload,
					runtime_row_token
				)
				equivalent_statement_cache_populates += 1
				equivalent_statement_cache_owner_populates += 1
				if no_inner_cycle and runtime_row_token is not None:
					_stage_equivalent_join_indexes_for_row(
						triples_map.triples_map_id,
						owner_equivalence,
						row,
						runtime_row_token
					)
			if current_tm_alias_equivalence is not None and pom_index == 0:
				_equivalent_statement_cache_set(
					current_tm_alias_equivalence["group_id"],
					current_tm_alias_equivalence["canonical_owner_tm_id"],
					current_tm_alias_equivalence["canonical_owner_po_index"],
					triples_map.data_source,
					row,
					pom_output_payload,
					runtime_row_token
				)
				equivalent_statement_cache_populates += 1
				equivalent_statement_cache_alias_populates += 1
				if no_inner_cycle and runtime_row_token is not None:
					_stage_equivalent_join_indexes_for_row(
						triples_map.triples_map_id,
						current_tm_alias_equivalence,
						row,
						runtime_row_token
					)
	if no_inner_cycle and runtime_row_token is not None and triples_list:
		_backfill_annotated_result_cache_if_eligible(
			triples_map,
			runtime_row_token,
			triples_list,
			"same_row_top_level_scan"
		)
		_stage_mixed_access_join_indexes_for_row(
			triples_map,
			row,
			runtime_row_token
		)
	_record_tm_debug(
		triples_map.triples_map_name,
		debug_branch_label,
		subject is not None,
		object is not None,
		debug_object_list_size,
		debug_subject_list_size,
		len(triples_list)
	)
	if semantify_file_start is not None:
		_record_time_metric("semantify_file_total", time.perf_counter() - semantify_file_start)
	return triples_list

def semantify(config_path):
	start_time = time.time()
	if os.path.isfile(config_path) == False:
		print("The configuration file " + config_path + " does not exist.")
		print("Aborting...")
		sys.exit(1)

	config = ConfigParser(interpolation=ExtendedInterpolation())
	config.read(config_path)

	global duplicate
	duplicate = config["datasets"]["remove_duplicate"]
	# TM-PLANNING-RUNTIME:
	# Optional runtime toggle for the planning-context summary added during the
	# TM-level planning work. Keep this config-driven so normal runs stay quiet.
	show_planning_context = config["datasets"].get("show_planning_context", "no").lower() == "yes"
	# TM-CACHE-SANITY:
	# Optional runtime toggle for read-only sanity checks around the cache
	# foundations added during the execution-preparation phase.
	show_cache_sanity = config["datasets"].get("show_cache_sanity", "no").lower() == "yes"
	# PROFILING-TIME:
	# Optional timing profiler for planner/cache/runtime hot spots. This is
	# independent from memory profiling so each can be enabled on its own.
	global show_time_breakdown
	show_time_breakdown = config["datasets"].get("show_time_breakdown", "no").lower() == "yes"
	# PROFILING-MEMORY:
	# Optional process RSS checkpoint sampling. Kept separate from the timing
	# profiler so users can inspect memory growth without turning on time metrics.
	global show_memory_profile
	show_memory_profile = config["datasets"].get("show_memory_profile", "no").lower() == "yes"
	# TM-CACHE-SAME-ROW:
	# First live cache toggle. This only affects same-source quoted reuse and
	# falls back to the old recursive behavior on cache miss.
	global enable_same_row_quoted_cache
	enable_same_row_quoted_cache = config["datasets"].get("enable_same_row_quoted_cache", "no").lower() == "yes"
	global same_row_cache_min_reference_count
	same_row_cache_min_reference_count = int(config["datasets"].get("same_row_cache_min_reference_count", "2"))
	# TM-CACHE-JOIN:
	# Experimental toggle for the first quoted join-cache branch. Keep it
	# separate from the same-row cache so join refactors can be isolated.
	global enable_join_quoted_cache
	enable_join_quoted_cache = config["datasets"].get("enable_join_quoted_cache", "no").lower() == "yes"
	global enable_eager_join_prebuild
	enable_eager_join_prebuild = config["datasets"].get("enable_eager_join_prebuild", "yes").lower() == "yes"
	global enable_equivalent_statement_cache
	enable_equivalent_statement_cache = config["datasets"].get("enable_equivalent_statement_cache", "yes").lower() == "yes"
	# TM-CACHE-FLUSH:
	# Optional runtime toggle for conservative level-based cache flushing driven
	# by planning metadata and the current execution order.
	global enable_level_cache_flush
	enable_level_cache_flush = config["datasets"].get("enable_level_cache_flush", "no").lower() == "yes"
	# TM-CACHE-LEVEL-EXECUTION:
	# Optional level-aware execution wrapper. This keeps files_sort() as the
	# secondary ordering rule inside each level instead of replacing it.
	global enable_level_execution_order
	enable_level_execution_order = config["datasets"].get("enable_level_execution_order", "no").lower() == "yes"
	# TM-CACHE-FLUSH:
	# Optional per-event debug output for the flush logic. Keep separate from the
	# aggregate sanity counters so the event trace is opt-in.
	global show_cache_flush_debug
	show_cache_flush_debug = config["datasets"].get("show_cache_flush_debug", "no").lower() == "yes"
	global same_row_cache_hits
	global same_row_cache_misses
	global annotated_result_cache_hits
	global annotated_result_cache_misses
	global annotated_result_cache_writes
	global annotated_result_cache_same_row_writes
	global annotated_result_cache_join_materialization_writes
	global annotated_result_cache_same_row_hits
	global annotated_result_cache_join_materialization_hits
	global annotated_result_cache_join_resolution_hits
	global annotated_result_cache_cross_path_hits
	global annotated_result_cache_join_resolution_misses
	global quoted_join_index_prebuilds
	global quoted_join_index_prebuild_skips
	global equivalent_statement_cache_hits
	global equivalent_statement_cache_misses
	global equivalent_statement_cache_populates
	global equivalent_statement_cache_owner_populates
	global equivalent_statement_cache_alias_populates
	global equivalent_statement_cache_owner_hits
	global equivalent_statement_cache_alias_hits
	global equivalent_join_cache_hits
	global equivalent_join_cache_misses
	global equivalent_join_cache_populates
	global planner_same_row_cache_skips
	global planner_join_cache_skips
	global level_cache_flush_events
	global flushed_same_row_cache_entries
	global flushed_join_table_groups
	global flushed_annotated_result_cache_entries
	global flushed_equivalent_statement_cache_entries
	global flushed_equivalent_join_cache_entries
	global flushed_equivalent_statement_group_ids
	global cache_flush_debug_events
	global debug_tm_filters
	global debug_tm_stats
	global same_row_key_runtime_token_uses
	global same_row_key_fallback_uses
	global perf_time_stats
	global perf_memory_samples
	global perf_memory_peak_bytes
	global perf_memory_supported
	same_row_cache_hits = 0
	same_row_cache_misses = 0
	same_row_key_runtime_token_uses = 0
	same_row_key_fallback_uses = 0
	annotated_result_cache_hits = 0
	annotated_result_cache_misses = 0
	annotated_result_cache_writes = 0
	annotated_result_cache_same_row_writes = 0
	annotated_result_cache_join_materialization_writes = 0
	annotated_result_cache_same_row_hits = 0
	annotated_result_cache_join_materialization_hits = 0
	annotated_result_cache_join_resolution_hits = 0
	annotated_result_cache_cross_path_hits = 0
	annotated_result_cache_join_resolution_misses = 0
	quoted_join_index_prebuilds = 0
	quoted_join_index_prebuild_skips = 0
	equivalent_statement_cache_hits = 0
	equivalent_statement_cache_misses = 0
	equivalent_statement_cache_populates = 0
	equivalent_statement_cache_owner_populates = 0
	equivalent_statement_cache_alias_populates = 0
	equivalent_statement_cache_owner_hits = 0
	equivalent_statement_cache_alias_hits = 0
	equivalent_join_cache_hits = 0
	equivalent_join_cache_misses = 0
	equivalent_join_cache_populates = 0
	planner_same_row_cache_skips = 0
	planner_join_cache_skips = 0
	level_cache_flush_events = 0
	flushed_same_row_cache_entries = 0
	flushed_join_table_groups = 0
	flushed_annotated_result_cache_entries = 0
	flushed_equivalent_statement_cache_entries = 0
	flushed_equivalent_join_cache_entries = 0
	flushed_equivalent_statement_group_ids = set()
	cache_flush_debug_events = []
	debug_tm_filters = [
		part.strip()
		for part in config["datasets"].get("debug_tm_filters", "").split(",")
		if part.strip()
	]
	debug_tm_stats = {}
	perf_time_stats = {}
	perf_memory_samples = []
	perf_memory_peak_bytes = 0
	perf_memory_supported = False

	global annotated_result_cache
	global annotated_result_cache_origins
	global equivalent_statement_cache
	global equivalent_statement_cache_groups
	global equivalent_join_cache
	global equivalent_join_cache_groups
	global staged_join_table
	global staged_equivalent_join_cache
	global active_planning_context
	annotated_result_cache = {}
	annotated_result_cache_origins = {}
	equivalent_statement_cache = {}
	equivalent_statement_cache_groups = {}
	equivalent_join_cache = {}
	equivalent_join_cache_groups = {}
	staged_join_table = {}
	staged_equivalent_join_cache = {}
	active_planning_context = None
	_capture_memory_sample("run_start")

	enrichment = config["datasets"]["enrichment"]

	if not os.path.exists(config["datasets"]["output_folder"]):
		os.mkdir(config["datasets"]["output_folder"])

	global number_triple
	global blank_message
	start = time.time()

	if config["datasets"]["all_in_one_file"] == "no":

		with ThreadPoolExecutor(max_workers=10) as executor:
			for dataset_number in range(int(config["datasets"]["number_of_datasets"])):
				dataset_i = "dataset" + str(int(dataset_number) + 1)
				with _time_metric("mapping_parser"):
					triples_map_list = mapping_parser(config[dataset_i]["mapping"])
				_capture_memory_sample(f"{dataset_i}:mapping_parsed")
				with _time_metric("analyze_tm_levels"):
					tm_levels = analyze_tm_levels(triples_map_list)
				print(format_tm_levels_summary(tm_levels))
				planning_context_required = (
					show_planning_context
					or enable_same_row_quoted_cache
					or enable_join_quoted_cache
					or enable_equivalent_statement_cache
					or enable_level_cache_flush
					or enable_level_execution_order
				)
				if planning_context_required:
					with _time_metric("build_planning_context"):
						planning_context = build_planning_context(triples_map_list, tm_levels)
					active_planning_context = planning_context
					_capture_memory_sample(f"{dataset_i}:planning_ready")
				else:
					planning_context = None
					active_planning_context = None
				# TM-PLANNING-RUNTIME:
				# Print the planning summary only when explicitly requested, but
				# allow runtime planner-aware features to reuse the same metadata.
				if show_planning_context:
					print(format_planning_context_summary(planning_context))
				if show_cache_sanity:
					print(format_cache_foundation_sanity(planning_context))
					print(f"  same_row_cache_enabled={enable_same_row_quoted_cache}")
					print(f"  join_quoted_cache_enabled={enable_join_quoted_cache}")
					print(f"  equivalent_statement_cache_enabled={enable_equivalent_statement_cache}")
					print(f"  level_cache_flush_enabled={enable_level_cache_flush}")
					print(f"  level_execution_order_enabled={enable_level_execution_order}")
					print(f"  cache_flush_debug_enabled={show_cache_flush_debug}")
				print(f"Sanity: parsed {len(triples_map_list)} triples maps from {config[dataset_i]['mapping']}")
				global base
				base = extract_base(config[dataset_i]["mapping"])
				output_file = config["datasets"]["output_folder"] + "/" + config[dataset_i]["name"] + ".nt"

				print("Semantifying {}...".format(config[dataset_i]["name"]))
				with open(output_file, "w", encoding = "utf-8") as output_file_descriptor:
					with _time_metric("files_sort"):
						sorted_sources, predicate_list, order_list = files_sort(triples_map_list, config["datasets"]["ordered"])
					execution_batches = _build_execution_batches(sorted_sources, order_list, planning_context)
					_prebuild_high_join_indexes(planning_context, triples_map_list)
					remaining_execution_tm_ids = set(_build_asserted_tm_execution_order_from_batches(sorted_sources, execution_batches))
					flushed_tm_ids = set()
					_capture_memory_sample(f"{dataset_i}:execution_batches_ready")
					if sorted_sources:
						for source_type, source, tm_keys in execution_batches:
							if source_type == "csv":
								if config["datasets"]["enrichment"].lower() == "false":
									if ".csv" in source:
										reader = pd.read_csv(source, dtype = str)
									else:
										reader = pd.read_csv(source, dtype = str, sep='\t')
									reader = reader.where(pd.notnull(reader), None)
									if duplicate == "yes":
										reader = reader.drop_duplicates(keep ='first')
									data = reader.to_dict(orient='records')
									for triples_map in tm_keys:
										tm = sorted_sources[source_type][source][triples_map]
										print("TM:", tm.triples_map_name)
										blank_message = True
										if enrichment == "yes":
											for row_position, row in enumerate(data):
												triples_list = executor.submit(semantify_file, tm, triples_map_list, ",", row, True, _make_same_row_runtime_token(source, row_position)).result()
												for triples in triples_list:
													output_file_descriptor.write(triples)
											predicate_list = release_PTT(tm, predicate_list)
											_mark_tm_complete_and_maybe_flush(tm, planning_context, remaining_execution_tm_ids, flushed_tm_ids)
										else:
											number_triple += executor.submit(semantify_file_array, tm, triples_map_list, ",", output_file_descriptor, wr, config[dataset_i]["name"], data).result()
											_mark_tm_complete_and_maybe_flush(tm, planning_context, remaining_execution_tm_ids, flushed_tm_ids)
								else:
									for triples_map in tm_keys:
										tm = sorted_sources[source_type][source][triples_map]
										print("TM:", tm.triples_map_name)
										delimiter = ',' if ".csv" in source else '\t'
										with _stream_csv_rows(source, delimiter=delimiter) as data:
											blank_message = True
											if enrichment == "yes":
												for row_position, row in enumerate(data):
													triples_list = executor.submit(semantify_file, tm, triples_map_list, ",", row, True, _make_same_row_runtime_token(source, row_position)).result()
													for triples in triples_list:
														output_file_descriptor.write(triples)
												predicate_list = release_PTT(tm, predicate_list)
												_mark_tm_complete_and_maybe_flush(tm, planning_context, remaining_execution_tm_ids, flushed_tm_ids)
											else:
												number_triple += executor.submit(semantify_file_array, tm, triples_map_list, ",", output_file_descriptor, wr, config[dataset_i]["name"], data).result()
												_mark_tm_complete_and_maybe_flush(tm, planning_context, remaining_execution_tm_ids, flushed_tm_ids)
							elif source_type == "JSONPath":
								pass
							elif source_type == "XPath":
								pass
					if predicate_list:
						for triples_map in triples_map_list:
							blank_message = True
							if str(triples_map.file_format).lower() != "csv" and triples_map.file_format != "JSONPath" and triples_map.file_format != "XPath":
								if config["datasets"]["dbType"] == "mysql":
									print("TM:", triples_map.triples_map_name)
									pass
								elif config["datasets"]["dbType"] == "postgres":
									print("TM:", triples_map.triples_map_name)	
									pass					 
								else:
									print("Invalid reference formulation or format")
									print("Aborting...")
									sys.exit(1)
					_flush_completed_tm_caches(planning_context, set(), flushed_tm_ids)
					_capture_memory_sample(f"{dataset_i}:post_flush")
				if show_cache_sanity:
					print(format_cache_foundation_sanity(planning_context))
					print(f"  same_row_cache_enabled={enable_same_row_quoted_cache}")
					print(f"  join_quoted_cache_enabled={enable_join_quoted_cache}")
					print(f"  equivalent_statement_cache_enabled={enable_equivalent_statement_cache}")
					print(f"  level_cache_flush_enabled={enable_level_cache_flush}")
					print(f"  level_execution_order_enabled={enable_level_execution_order}")
					print(f"  cache_flush_debug_enabled={show_cache_flush_debug}")
				if show_cache_flush_debug:
					print(format_cache_flush_debug_events())
				if debug_tm_filters:
					print(format_tm_debug_stats())
				print("Successfully semantified {}.\n\n".format(config[dataset_i]["name"]))
	else:
		output_file = config["datasets"]["output_folder"] + "/" + config["datasets"]["name"] + ".nt"

		with ThreadPoolExecutor(max_workers=10) as executor:
			with open(output_file, "w", encoding="utf-8") as output_file_descriptor:
				for dataset_number in range(int(config["datasets"]["number_of_datasets"])):
					dataset_i = "dataset" + str(int(dataset_number) + 1)
					with _time_metric("mapping_parser"):
						triples_map_list = mapping_parser(config[dataset_i]["mapping"])
					_capture_memory_sample(f"{dataset_i}:mapping_parsed")
					with _time_metric("analyze_tm_levels"):
						tm_levels = analyze_tm_levels(triples_map_list)
					print(format_tm_levels_summary(tm_levels))
					planning_context_required = (
						show_planning_context
						or enable_same_row_quoted_cache
						or enable_join_quoted_cache
						or enable_equivalent_statement_cache
						or enable_level_cache_flush
						or enable_level_execution_order
					)
					if planning_context_required:
						with _time_metric("build_planning_context"):
							planning_context = build_planning_context(triples_map_list, tm_levels)
						active_planning_context = planning_context
						_capture_memory_sample(f"{dataset_i}:planning_ready")
					else:
						planning_context = None
						active_planning_context = None
					# TM-PLANNING-RUNTIME:
					# Same summary hook for the all-in-one-file branch so both
					# execution paths expose the same planning metadata.
					if show_planning_context:
						print(format_planning_context_summary(planning_context))
					if show_cache_sanity:
						print(format_cache_foundation_sanity(planning_context))
						print(f"  same_row_cache_enabled={enable_same_row_quoted_cache}")
						print(f"  join_quoted_cache_enabled={enable_join_quoted_cache}")
						print(f"  equivalent_statement_cache_enabled={enable_equivalent_statement_cache}")
						print(f"  level_cache_flush_enabled={enable_level_cache_flush}")
						print(f"  level_execution_order_enabled={enable_level_execution_order}")
						print(f"  cache_flush_debug_enabled={show_cache_flush_debug}")
					base = extract_base(config[dataset_i]["mapping"])
					output_file = config["datasets"]["output_folder"] + "/" + config[dataset_i]["name"] + ".nt"

					print("Semantifying {}...".format(config[dataset_i]["name"]))
				
					with _time_metric("files_sort"):
						sorted_sources, predicate_list, order_list = files_sort(triples_map_list, config["datasets"]["ordered"])
					execution_batches = _build_execution_batches(sorted_sources, order_list, planning_context)
					_prebuild_high_join_indexes(planning_context, triples_map_list)
					remaining_execution_tm_ids = set(_build_asserted_tm_execution_order_from_batches(sorted_sources, execution_batches))
					flushed_tm_ids = set()
					_capture_memory_sample(f"{dataset_i}:execution_batches_ready")
					if sorted_sources:
						for source_type, source, tm_keys in execution_batches:
							if source_type == "csv":
								if config["datasets"]["enrichment"].lower() == "yes":
									for triples_map in tm_keys:
										tm = sorted_sources[source_type][source][triples_map]
										print("TM:", tm.triples_map_name)
										with _stream_csv_rows(source) as data:
											blank_message = True
											if enrichment == "yes":
												for row_position, row in enumerate(data):
													triples_list = executor.submit(semantify_file, tm, triples_map_list, ",", row, True, _make_same_row_runtime_token(source, row_position)).result()
													for triples in triples_list:
														output_file_descriptor.write(triples)
												predicate_list = release_PTT(tm, predicate_list)
												_mark_tm_complete_and_maybe_flush(tm, planning_context, remaining_execution_tm_ids, flushed_tm_ids)
											else:
												number_triple += executor.submit(semantify_file_array, tm, triples_map_list, ",", output_file_descriptor, wr, config[dataset_i]["name"], data).result()
												_mark_tm_complete_and_maybe_flush(tm, planning_context, remaining_execution_tm_ids, flushed_tm_ids)
								else:
									with _stream_csv_rows(source) as data:
										for triples_map in tm_keys:
											tm = sorted_sources[source_type][source][triples_map]
											print("TM:", tm.triples_map_name)
											blank_message = True
											for row_position, row in enumerate(data):
												triples_list = executor.submit(semantify_file, tm, triples_map_list, ",", row, True, _make_same_row_runtime_token(source, row_position)).result()
												for triples in triples_list:
													output_file_descriptor.write(triples)
											predicate_list = release_PTT(tm, predicate_list)
											_mark_tm_complete_and_maybe_flush(tm, planning_context, remaining_execution_tm_ids, flushed_tm_ids)
							elif source_type == "JSONPath":
								pass
							elif source_type == "XPath":
								pass
					
					if predicate_list:
						for triples_map in triples_map_list:
							blank_message = True
							if str(triples_map.file_format).lower() != "csv" and triples_map.file_format != "JSONPath" and triples_map.file_format != "XPath":
								if config["datasets"]["dbType"] == "mysql":
									print("TM:", triples_map.triples_map_name)
									pass
								elif config["datasets"]["dbType"] == "postgres":	
									pass					
								else:
									print("Invalid reference formulation or format")
									print("Aborting...")
									sys.exit(1)
					_flush_completed_tm_caches(planning_context, set(), flushed_tm_ids)
					_capture_memory_sample(f"{dataset_i}:post_flush")
					if show_cache_sanity:
						print(format_cache_foundation_sanity(planning_context))
						print(f"  same_row_cache_enabled={enable_same_row_quoted_cache}")
						print(f"  join_quoted_cache_enabled={enable_join_quoted_cache}")
						print(f"  equivalent_statement_cache_enabled={enable_equivalent_statement_cache}")
						print(f"  level_cache_flush_enabled={enable_level_cache_flush}")
						print(f"  level_execution_order_enabled={enable_level_execution_order}")
						print(f"  cache_flush_debug_enabled={show_cache_flush_debug}")
					if show_cache_flush_debug:
						print(format_cache_flush_debug_events())
					if debug_tm_filters:
						print(format_tm_debug_stats())
					print("Successfully semantified {}.\n\n".format(config[dataset_i]["name"]))

	duration = time.time() - start_time
	_capture_memory_sample("run_end")

	print("Successfully semantified all datasets in {:.3f} seconds.".format(duration))
	if show_time_breakdown:
		print(format_time_breakdown(duration))
	if show_memory_profile:
		print(format_memory_profile())
