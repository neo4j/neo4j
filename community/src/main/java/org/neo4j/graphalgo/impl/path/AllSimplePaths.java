package org.neo4j.graphalgo.impl.path;

import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.Uniqueness;

public class AllSimplePaths extends AllPaths
{
    public AllSimplePaths( int maxDepth, RelationshipExpander expander )
    {
        super( maxDepth, expander );
    }
    
    @Override
    protected Uniqueness uniqueness()
    {
        return Uniqueness.NODE_PATH;
    }
}
