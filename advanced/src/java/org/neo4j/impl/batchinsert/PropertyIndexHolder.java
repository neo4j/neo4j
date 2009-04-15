package org.neo4j.impl.batchinsert;

import org.neo4j.impl.nioneo.store.PropertyIndexData;
import org.neo4j.impl.util.ArrayMap;

public class PropertyIndexHolder
{
    private final ArrayMap<String,Integer> propertyIndexes = 
        new ArrayMap<String,Integer>( 5, false, false);
    private final ArrayMap<Integer,String> idToIndex = 
        new ArrayMap<Integer,String>( 5, false, false);
    
    PropertyIndexHolder( PropertyIndexData[] indexes )
    {
        for ( PropertyIndexData index : indexes )
        {
            propertyIndexes.put( index.getValue(), index.getKeyId() );
            idToIndex.put( index.getKeyId(), index.getValue() );
        }
    }
    
    void addPropertyIndex( String stringKey, int keyId )
    {
        propertyIndexes.put( stringKey, keyId );
        idToIndex.put( keyId, stringKey );
    }
    
    int getKeyId( String stringKey )
    {
        Integer keyId = propertyIndexes.get( stringKey );
        if ( keyId != null )
        {
            return keyId;
        }
        return -1;
    }
    
    String getStringKey( int keyId )
    {
        String stringKey = idToIndex.get( keyId );
        if ( stringKey == null )
        {
            throw new RuntimeException( "No such property index[" + keyId + 
                "]" );
        }
        return stringKey;
    }
}
