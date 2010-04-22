package org.neo4j.graphdb.traversal;

public interface PruneEvaluator
{
    /**
     * Default {@link PruneEvaluator}, does not prune any parts of the
     * traversal.
     */
    static final PruneEvaluator NONE = new PruneEvaluator()
    {
        public boolean pruneAfter( Position position )
        {
            return false;
        }
    };

    boolean pruneAfter( Position position );
}
