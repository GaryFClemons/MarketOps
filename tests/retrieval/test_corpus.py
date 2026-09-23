"""load_corpus is implemented plumbing; these pass today."""

from __future__ import annotations

from market_ops.retrieval.corpus import load_corpus


def _write(root, rel, text):
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_sources_types_exclusions_and_front_matter(tmp_path):
    _write(tmp_path, "docs/runbooks/vendor.md", "---\nstatus: active\n---\n# Vendor runbook\n\n## Symptoms\n\n429s.\n")
    _write(tmp_path, "docs/incidents/2026-08-30-x.md", "# Incident: stale session\n")
    _write(tmp_path, "ROADMAP.md", "---\nnext_action: x\n---\n# Roadmap\n")
    _write(tmp_path, "codebase-map.md", "# Map\n")
    _write(tmp_path, "docs/architecture.md", "no heading here\n")
    _write(tmp_path, "docs/build-order.md", "# Backlog\n")
    _write(tmp_path, "docs/eval_results.md", "# Results\n")
    _write(tmp_path, "README.md", "# Readme\n")

    docs = load_corpus(tmp_path)
    by_id = {d.doc_id: d for d in docs}

    assert [d.doc_id for d in docs] == sorted(by_id)
    assert set(by_id) == {
        "docs/runbooks/vendor.md",
        "docs/incidents/2026-08-30-x.md",
        "ROADMAP.md",
        "codebase-map.md",
        "docs/architecture.md",
    }
    # First matching glob wins: a runbook is not re-typed as a generic doc.
    assert by_id["docs/runbooks/vendor.md"].source_type == "runbook"
    assert by_id["docs/architecture.md"].source_type == "doc"
    assert by_id["ROADMAP.md"].source_type == "decision_log"
    # Front-matter stripped; H1 becomes the title; no H1 -> file stem.
    assert "status: active" not in by_id["docs/runbooks/vendor.md"].text
    assert by_id["docs/runbooks/vendor.md"].title == "Vendor runbook"
    assert by_id["docs/architecture.md"].title == "architecture"


def test_real_repo_corpus_has_the_operational_docs(repo_root):
    ids = {d.doc_id for d in load_corpus(repo_root)}
    assert "ROADMAP.md" in ids
    assert "codebase-map.md" in ids
    assert "docs/incidents/2026-08-30-stale-session-partition.md" in ids
    assert "docs/build-order.md" not in ids
