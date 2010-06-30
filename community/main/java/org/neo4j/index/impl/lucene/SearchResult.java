package org.neo4j.index.impl.lucene;

import java.util.Iterator;

import org.apache.lucene.document.Document;

class SearchResult
{
    final Iterator<Document> documents;
    final int size;
    
    SearchResult( Iterator<Document> documents, int size )
    {
        super();
        this.documents = documents;
        this.size = size;
    }
}
