package org.neo4j.kernel;

import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.UniquenessFilter;

abstract class AbstractUniquenessFilter implements UniquenessFilter
{
    final PrimitiveTypeFetcher type;

    AbstractUniquenessFilter( PrimitiveTypeFetcher type )
    {
        this.type = type;
    }

    public boolean checkFirst( TraversalBranch branch )
    {
        return type == PrimitiveTypeFetcher.RELATIONSHIP ? true : check( branch, true );
    }
}
