package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.traversal.ExpansionSource;

class NotUnique extends UniquenessFilter
{
    NotUnique()
    {
        super( null );
    }

    @Override
    boolean check( ExpansionSource source, boolean remember )
    {
        return true;
    }

    @Override
    boolean check( long id, boolean remember )
    {
        return true;
    }
}
