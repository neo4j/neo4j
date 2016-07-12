# Legacy consistency check

Contains the legacy version of the consistency checker that could be found in neo4j version 2.2 and below.

The current consistency checker depends on this checker for the sole purpose of being able to run a consistency check exactly how it was done before while the new checker stabilizes, as a fall back. The code is a complete copy, with changed top-level package from org.neo4j.consistency --> org.neo4j.legacy.consistency.

Ones the new checker is considered stable, this legacy component should be deleted.
