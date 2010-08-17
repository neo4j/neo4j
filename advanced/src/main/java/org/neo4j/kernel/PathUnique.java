package org.neo4j.kernel;

import org.neo4j.graphdb.traversal.TraversalBranch;

class PathUnique extends AbstractUniquenessFilter
{
    PathUnique( PrimitiveTypeFetcher type )
    {
        super( type );
    }
    
    public boolean check( TraversalBranch source, boolean remember )
    {
        long idToCompare = type.getId( source );
        while ( (source = source.parent()) != null )
        {
            if (type.idEquals(source, idToCompare))
            {
                return false;
            }
        }
        return true;
    }
}
