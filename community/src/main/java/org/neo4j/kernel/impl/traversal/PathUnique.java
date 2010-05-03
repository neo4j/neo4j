package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.traversal.ExpansionSource;

class PathUnique extends UniquenessFilter
{
    PathUnique( PrimitiveTypeFetcher type )
    {
        super( type );
    }
    
    @Override
    boolean check( ExpansionSource source, boolean remember )
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

    @Override
    boolean check( long id, boolean remember )
    {
        throw new UnsupportedOperationException();
    }
}
