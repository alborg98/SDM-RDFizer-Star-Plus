import argparse
import os
import sys
from typing import Tuple

import rdflib


def term_label(g: rdflib.Graph, term: rdflib.term.Identifier, max_len: int = 60) -> Tuple[str, str]:
    """Return a (label, group) for a term for display purposes.

    group is one of: uri, literal, bnode
    """
    from rdflib.term import URIRef, BNode, Literal

    if isinstance(term, URIRef):
        try:
            label = g.namespace_manager.normalizeUri(term)
        except Exception:
            label = str(term)
        group = "uri"
    elif isinstance(term, BNode):
        label = f"_:{str(term)}"
        group = "bnode"
    elif isinstance(term, Literal):
        # Prefer lexical form, show datatype/lang if present
        txt = str(term)
        if term.language:
            txt = f'"{txt}"@{term.language}'
        elif term.datatype:
            dt = g.namespace_manager.normalizeUri(term.datatype) if term.datatype else str(term.datatype)
            txt = f'"{txt}"^^{dt}'
        label = txt
        group = "literal"
    else:
        label = str(term)
        group = "uri"

    if len(label) > max_len:
        label = label[: max_len - 1] + "…"
    return label, group


def build_pyvis(nt_path: str, limit: int, out_html: str, physics: bool = True) -> str:
    try:
        from pyvis.network import Network
    except ImportError:
        print("pyvis is required. Install with: pip install pyvis", file=sys.stderr)
        sys.exit(1)

    g = rdflib.Graph()
    fmt = "nt" if nt_path.endswith(".nt") else None
    g.parse(nt_path, format=fmt)

    net = Network(height="800px", width="100%", directed=True, bgcolor="#ffffff")
    # Configure physics for readability
    net.toggle_physics(physics)
    net.set_options(
        '{"edges": {"smooth": {"type": "dynamic"}}, "nodes": {"shape": "dot", "scaling": {"min": 5, "max": 20}}}'
    )

    # Color map per group
    colors = {"uri": "#1976d2", "literal": "#ef6c00", "bnode": "#616161"}

    added = set()
    count = 0
    for s, p, o in g:
        if limit and count >= limit:
            break
        s_label, s_group = term_label(g, s)
        o_label, o_group = term_label(g, o)
        p_label, _ = term_label(g, p)

        if s_label not in added:
            net.add_node(s_label, label=s_label, title=str(s), color=colors.get(s_group, None), group=s_group)
            added.add(s_label)
        if o_label not in added:
            net.add_node(o_label, label=o_label, title=str(o), color=colors.get(o_group, None), group=o_group)
            added.add(o_label)

        net.add_edge(s_label, o_label, label=p_label)
        count += 1

    if not out_html:
        base = os.path.splitext(os.path.basename(nt_path))[0]
        out_html = os.path.join(os.path.dirname(nt_path), f"{base}_graph.html")

    net.write_html(out_html)
    return out_html


def main():
    parser = argparse.ArgumentParser(description="Visualize an RDF graph from an N-Triples file as interactive HTML.")
    parser.add_argument("nt_file", help="Path to N-Triples (.nt) file, e.g., output/Test.nt")
    parser.add_argument("--limit", type=int, default=1000, help="Max number of triples to visualize (default: 1000)")
    parser.add_argument("--out", default="", help="Output HTML file path (default: alongside the input)")
    parser.add_argument("--no-physics", action="store_true", help="Disable physics layout (useful for small graphs)")
    args = parser.parse_args()

    nt_path = os.path.abspath(args.nt_file)
    if not os.path.isfile(nt_path):
        print(f"File not found: {nt_path}", file=sys.stderr)
        sys.exit(1)

    html_path = build_pyvis(nt_path, args.limit, args.out, physics=not args.no_physics)
    print(f"Wrote graph visualization to: {html_path}")
    print("Open it in a browser to explore the graph interactively.")


if __name__ == "__main__":
    main()

