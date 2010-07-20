package org.neo4j.index.impl.lucene;

import org.apache.lucene.search.Sort;

public class QueryContext
{
    final Object queryOrQueryObject;
    Sort sorting;
    
    public QueryContext( Object queryOrQueryObject )
    {
        this.queryOrQueryObject = queryOrQueryObject;
    }
    
    public QueryContext sort( Sort sorting )
    {
        this.sorting = sorting;
        return this;
    }
}
