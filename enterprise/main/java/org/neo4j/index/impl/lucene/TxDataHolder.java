package org.neo4j.index.impl.lucene;

import java.util.Collection;

import org.apache.lucene.search.Query;
import org.neo4j.helpers.Pair;

class TxDataHolder
{
    final LuceneIndex index;
    private TxData data;
    
    TxDataHolder( LuceneIndex index, TxData initialData )
    {
        this.index = index;
        this.data = initialData;
    }

    void add( Object entityId, String key, Object value )
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

    void remove( Object entityId, String key, Object value )
    {
        this.data = this.data.remove( entityId, key, value );
    }

    void remove( Query query )
    {
        this.data = this.data.remove( query );
    }

    Collection<Long> query( Query query, QueryContext contextOrNull )
    {
        Pair<Collection<Long>, TxData> entry = this.data.query( query, contextOrNull );
        this.data = entry.other();
        return entry.first();
    }

    Collection<Long> get( String key, Object value )
    {
        Pair<Collection<Long>, TxData> entry = this.data.get( key, value );
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
