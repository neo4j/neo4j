package org.neo4j.index.impl.lucene;

import java.util.Iterator;

import org.neo4j.graphdb.index.IndexHits;

class LazyIndexHits<T> implements IndexHits<T>
{
    private final IndexHits<T> hits;
    private final IndexSearcherRef searcher;
    
    LazyIndexHits( IndexHits<T> hits, IndexSearcherRef searcher )
    {
        this.hits = hits;
        this.searcher = searcher;
    }

    public void close()
    {
        this.hits.close();
        if ( this.searcher != null )
        {
            this.searcher.closeStrict();
        }
    }

    public int size()
    {
        return this.hits.size();
    }

    public Iterator<T> iterator()
    {
        return this.hits.iterator();
    }

    public boolean hasNext()
    {
        return this.hits.hasNext();
    }

    public T next()
    {
        return this.hits.next();
    }

    public void remove()
    {
        this.hits.remove();
    }
    
    public T getSingle()
    {
        return hits.getSingle();
    }
}
