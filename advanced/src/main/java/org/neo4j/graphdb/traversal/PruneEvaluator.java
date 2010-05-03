package org.neo4j.graphdb.traversal;

/**
 * An evaluator which can "cut off" relationships so that they will not be
 * traversed in the ongoing traversal. For any given position a prune evaluator
 * can decide whether or not to prune whatever is beyond (i.e. after) that
 * position or not.
 */
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
    
    /**
     * Decides whether or not to prune after {@code position}. If {@code true}
     * is returned the position won't be expanded and traversals won't be
     * made beyond that position.
     * 
     * @param position the {@link Position} to decide whether or not to prune
     * after.
     * @return whether or not to prune after {@code position}. 
     */
    boolean pruneAfter( Position position );
}
