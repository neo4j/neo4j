package org.neo4j.impl.batchinsert;

import org.neo4j.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.impl.util.ArrayMap;

public class RelationshipTypeHolder
{
    private final ArrayMap<String,Integer> relTypes = 
        new ArrayMap<String,Integer>( 5, false, false);
    private final ArrayMap<Integer,String> idToName = 
        new ArrayMap<Integer,String>( 5, false, false);
    
    RelationshipTypeHolder( RelationshipTypeData[] types )
    {
        for ( RelationshipTypeData type : types )
        {
           relTypes.put( type.getName(), type.getId() );
           idToName.put( type.getId(), type.getName() );
        }
    }
    
    void addRelationshipType( String name, int id )
    {
        relTypes.put( name, id );
        idToName.put( id, name );
    }
    
    int getTypeId( String name )
    {
        Integer id = relTypes.get( name );
        if ( id != null )
        {
            return id;
        }
        return -1;
    }
    
    String getName( int id )
    {
        String name = idToName.get( id );
        if ( name == null )
        {
            throw new RuntimeException( "No such relationship type[" + id + 
                "]" );
        }
        return name;
    }
}
