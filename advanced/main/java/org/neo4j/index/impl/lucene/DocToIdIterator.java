package org.neo4j.index.impl.lucene;

import java.util.Collection;
import java.util.Iterator;

import org.apache.lucene.document.Document;
import org.neo4j.helpers.collection.PrefetchingIterator;

class DocToIdIterator extends PrefetchingIterator<Long>
{
    private final SearchResult searchResult;
    private final Collection<Long> exclude;
    private final IndexSearcherRef searcherOrNull;
    
    DocToIdIterator( SearchResult searchResult, Collection<Long> exclude,
        IndexSearcherRef searcherOrNull )
    {
        this.searchResult = searchResult;
        this.exclude = exclude;
        this.searcherOrNull = searcherOrNull;
    }

    @Override
    protected Long fetchNextOrNull()
    {
        Long result = null;
        while ( result == null )
        {
            if ( !searchResult.documents.hasNext() )
            {
                endReached();
                break;
            }
            Document doc = searchResult.documents.next();
            long id = Long.parseLong(
                doc.getField( LuceneIndex.KEY_DOC_ID ).stringValue() );
            if ( exclude == null || !exclude.contains( id ) )
            {
                result = id;
            }
        }
        return result;
    }
    
    private void endReached()
    {
        if ( this.searcherOrNull != null )
        {
            this.searcherOrNull.closeStrict();
        }
    }

    public int size()
    {
        return searchResult.size;
    }

    public void close()
    {
    }

    public Iterator<Long> iterator()
    {
        return this;
    }
}
