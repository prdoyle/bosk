------------------------------- MODULE BoskMongo -------------------------------
\* Top-level TLA+ module for bosk-mongo model checking.
\*
\* This module composes format-specific sub-models:
\*   - BoskMongoSequoia: single-document (Sequoia) format
\*   - BoskMongoPando:   multi-document (Pando) format    (TODO)
\*   - BoskMongoRefurbish: cross-format refurbish          (TODO)
\*
\* Currently extended: BoskMongoSequoia

EXTENDS BoskMongoSequoia

\* The specification is provided by the EXTENDed module.
\* TLC should use:
\*   Spec  (no fairness — checks safety invariants only)
\*   or
\*   SpecFair (with fairness — also checks liveness properties)

=============================================================================
