package org.neo4j.kernel;

import org.neo4j.graphdb.traversal.TraversalBranch;

class NotUnique extends AbstractUniquenessFilter
{
    NotUnique()
    {
        super( null );
    }

    public boolean check( TraversalBranch source, boolean remember )
    {
        return true;
    }
}
