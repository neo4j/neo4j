package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.traversal.TraversalBranch;

class NotUnique extends UniquenessFilter
{
    NotUnique()
    {
        super( null );
    }

    @Override
    boolean check( TraversalBranch source, boolean remember )
    {
        return true;
    }

    @Override
    boolean check( long id, boolean remember )
    {
        return true;
    }
}
