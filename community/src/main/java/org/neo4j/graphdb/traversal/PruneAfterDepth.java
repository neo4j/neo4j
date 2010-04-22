package org.neo4j.graphdb.traversal;

public final class PruneAfterDepth implements PruneEvaluator
{
    private final int depth;

    public PruneAfterDepth( int depth )
    {
        this.depth = depth;
    }

    public boolean pruneAfter( Position position )
    {
        return position.depth() >= depth;
    }
}
