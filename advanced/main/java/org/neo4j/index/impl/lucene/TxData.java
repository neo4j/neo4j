package org.neo4j.index.impl.lucene;

import java.util.Collection;

import org.apache.lucene.search.Query;
import org.neo4j.helpers.Pair;

abstract class TxData
{
    final LuceneIndex index;
    private boolean removeAll;
    
    TxData( LuceneIndex index )
    {
        this.index = index;
    }

    abstract TxData add( Long entityId, String key, Object value );
    
    /**
     * Only for the {@link TxData} representing removal.
     */
    abstract TxData add( Query query );

    abstract TxData remove( Long entityId, String key, Object value );

    abstract TxData remove( Query query );

    abstract Pair<Collection<Long>, TxData> query( Query query, QueryContext contextOrNull );

    abstract Pair<Collection<Long>, TxData> get( String key, Object value );
    
    abstract void close();

    abstract Query getExtraQuery();
    
    abstract TxData clear();

    void setRemoveAll()
    {
        removeAll = true;
    }
    
    boolean isRemoveAll()
    {
        return removeAll;
    }
}
