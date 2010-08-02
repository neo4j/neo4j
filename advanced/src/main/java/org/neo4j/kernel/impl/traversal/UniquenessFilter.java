package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.traversal.TraversalBranch;

abstract class UniquenessFilter
{
    final PrimitiveTypeFetcher type;

    UniquenessFilter( PrimitiveTypeFetcher type )
    {
        this.type = type;
    }

    boolean check( TraversalBranch source, boolean remember )
    {
        return check( type.getId( source ), remember );
    }

    abstract boolean check( long id, boolean remember );
}
