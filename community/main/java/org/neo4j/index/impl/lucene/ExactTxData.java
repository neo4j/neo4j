package org.neo4j.index.impl.lucene;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.search.Query;
import org.neo4j.helpers.Pair;

public class ExactTxData extends TxData
{
    private Map<String, Map<Object, Set<Long>>> data;
    
    ExactTxData( LuceneIndex index )
    {
        super( index );
    }

    @Override
    TxData add( Long entityId, String key, Object value )
    {
        idCollection( key, value, true ).add( entityId );
        return this;
    }
    
    @Override
    TxData add( Query query )
    {
        return toFullTxData().add( query );
    }
    
    private Set<Long> idCollection( String key, Object value, boolean create )
    {
        Map<Object, Set<Long>> keyMap = keyMap( key, create );
        if ( keyMap == null )
        {
            return null;
        }
        
        Set<Long> ids = keyMap.get( value );
        if ( ids == null && create )
        {
            ids = new HashSet<Long>();
            keyMap.put( value, ids );
        }
        return ids;
    }

    private Map<Object, Set<Long>> keyMap( String key, boolean create )
    {
        if ( data == null )
        {
            if ( create )
            {
                data = new HashMap<String, Map<Object,Set<Long>>>();
            }
            else
            {
                return null;
            }
        }
        
        Map<Object, Set<Long>> inner = data.get( key );
        if ( inner == null && create )
        {
            inner = new HashMap<Object, Set<Long>>();
            data.put( key, inner );
        }
        return inner;
    }

    private TxData toFullTxData()
    {
        FullTxData data = new FullTxData( index );
        if ( this.data != null )
        {
            for ( Map.Entry<String, Map<Object, Set<Long>>> entry : this.data.entrySet() )
            {
                String key = entry.getKey();
                for ( Map.Entry<Object, Set<Long>> valueEntry : entry.getValue().entrySet() )
                {
                    Object value = valueEntry.getKey();
                    for ( long id : valueEntry.getValue() )
                    {
                        data.add( id, key, value );
                    }
                }
            }
        }
        return data;
    }

    @Override
    void close()
    {
    }

    @Override
    Pair<Collection<Long>, TxData> query( Query query, QueryContext contextOrNull )
    {
        return toFullTxData().query( query, contextOrNull );
    }

    @Override
    TxData remove( Long entityId, String key, Object value )
    {
        if ( data == null )
        {
            return this;
        }
        Collection<Long> ids = idCollection( key, value, false );
        if ( ids != null )
        {
            ids.remove( entityId );
        }
        return this;
    }

    @Override
    TxData remove( Query query )
    {
        return toFullTxData().remove( query );
    }

    @Override
    Pair<Collection<Long>, TxData> get( String key, Object value )
    {
        Set<Long> ids = idCollection( key, value, false );
        if ( ids == null )
        {
            return new Pair<Collection<Long>, TxData>( Collections.<Long>emptySet(), this );
        }
        return new Pair<Collection<Long>, TxData>( ids, this );
    }
    
    @Override
    Query getExtraQuery()
    {
        return null;
    }
    
    @Override
    TxData clear()
    {
        if ( this.data != null )
        {
            this.data.clear();
        }
        return this;
    }
}
