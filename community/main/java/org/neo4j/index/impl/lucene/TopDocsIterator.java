package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.neo4j.commons.iterator.ArrayIterator;
import org.neo4j.commons.iterator.PrefetchingIterator;

class TopDocsIterator extends PrefetchingIterator<Document>
{
    private final Iterator<ScoreDoc> iterator;
    private final IndexSearcherRef searcher;
    
    TopDocsIterator( TopDocs docs, IndexSearcherRef searcher )
    {
        this.iterator = new ArrayIterator<ScoreDoc>( docs.scoreDocs );
        this.searcher = searcher;
    }

    @Override
    protected Document fetchNextOrNull()
    {
        if ( !iterator.hasNext() )
        {
            return null;
        }
        ScoreDoc doc = iterator.next();
        try
        {
            return searcher.getSearcher().doc( doc.doc );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
