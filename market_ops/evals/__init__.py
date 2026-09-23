"""Measure whether the AI layer is any good — with numbers, not impressions.

Two things are measured separately, on purpose:

- **Retrieval** (``run_retrieval_eval``): did the right runbook section come
  back? precision@k, recall@k, MRR, latency.
- **Briefs** (``score_brief``): given a signal, is the incident brief's
  severity, escalation, and citation right?

A bad brief built on good retrieval is a generation problem; a bad brief built
on bad retrieval is a retrieval problem. Only measuring both tells you which
one to fix.

Deterministic checks come first. LLM-as-judge (``judge.py``) is for the
qualities code can't check — and a judge is itself a model that has to be
validated before its scores mean anything.
"""
