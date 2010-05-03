package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.traversal.ExpansionSource;

abstract class UniquenessFilter
{
    final PrimitiveTypeFetcher type;

    UniquenessFilter( PrimitiveTypeFetcher type )
    {
        this.type = type;
    }

    boolean check( ExpansionSource source, boolean remember )
    {
        return check( type.getId( source ), remember );
    }

    abstract boolean check( long id, boolean remember );
}
