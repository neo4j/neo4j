package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.search.IndexSearcher;

class IndexSearcherRef
{
    private final IndexIdentifier identifier;
    private final IndexSearcher searcher;
    private final AtomicInteger refCount = new AtomicInteger( 0 );
    private boolean isClosed;
    
    /**
     * We need this because we only want to close the reader/searcher if
     * it has been detached... i.e. the {@link LuceneDataSource} no longer
     * has any reference to it, only an iterator out in the client has a ref.
     * And when that client calls close() it should be closed.
     */
    private boolean detached;
    
    public IndexSearcherRef( IndexIdentifier identifier, IndexSearcher searcher )
    {
        this.identifier = identifier;
        this.searcher = searcher;
    }
    
    public IndexSearcher getSearcher()
    {
        return this.searcher;
    }
    
    public IndexIdentifier getIdentifier()
    {
        return identifier;
    }

    void incRef()
    {
        this.refCount.incrementAndGet();
    }
    
    public void dispose() throws IOException
    {
        if ( !this.isClosed )
        {
            this.searcher.close();
            this.searcher.getIndexReader().close();
            this.isClosed = true;
        }
    }
    
    public void detachOrClose() throws IOException
    {
        if ( this.refCount.get() == 0 )
        {
            dispose();
        }
        else
        {
            this.detached = true;
        }
    }
    
    public boolean close() throws IOException
    {
        if ( this.isClosed || this.refCount.get() == 0 )
        {
            return true;
        }
        
        boolean reallyClosed = false;
        if ( this.refCount.decrementAndGet() <= 0 && this.detached )
        {
            dispose();
            reallyClosed = true;
        }
        return reallyClosed;
    }
    
    boolean closeStrict()
    {
        try
        {
            return close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
