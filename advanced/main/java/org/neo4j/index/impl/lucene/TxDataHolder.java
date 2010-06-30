package org.neo4j.index.impl.lucene;

import java.util.Set;

import org.apache.lucene.search.Query;
import org.neo4j.commons.Pair;

class TxDataHolder
{
    final LuceneIndex index;
    private TxData data;
    
    TxDataHolder( LuceneIndex index, TxData initialData )
    {
        this.index = index;
        this.data = initialData;
    }

    void add( Long entityId, String key, Object value )
    {
        this.data = this.data.add( entityId, key, value );
    }
    
    /**
     * Only for the tx data representing removal.
     */
    void add( Query query )
    {
        this.data = this.data.add( query );
    }

    void remove( Long entityId, String key, Object value )
    {
        this.data = this.data.remove( entityId, key, value );
    }

    void remove( Query query )
    {
        this.data = this.data.remove( query );
    }

    Set<Long> query( Query query )
    {
        Pair<Set<Long>, TxData> entry = this.data.query( query );
        this.data = entry.other();
        return entry.first();
    }

    Set<Long> get( String key, Object value )
    {
        Pair<Set<Long>, TxData> entry = this.data.get( key, value );
        this.data = entry.other();
        return entry.first();
    }
    
    void close()
    {
        this.data.close();
    }

    Query getExtraQuery()
    {
        return this.data.getExtraQuery();
    }
    
    void clear()
    {
        this.data = this.data.clear();
    }

    void setRemoveAll()
    {
        this.data.setRemoveAll();
    }
    
    boolean isRemoveAll()
    {
        return this.data.isRemoveAll();
    }
}
