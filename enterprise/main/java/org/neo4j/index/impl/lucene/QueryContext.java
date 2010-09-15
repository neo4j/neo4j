package org.neo4j.index.impl.lucene;

import org.apache.lucene.queryParser.QueryParser.Operator;
import org.apache.lucene.search.Sort;

public class QueryContext
{
    final Object queryOrQueryObject;
    Sort sorting;
    Operator defaultOperator;
    
    public QueryContext( Object queryOrQueryObject )
    {
        this.queryOrQueryObject = queryOrQueryObject;
    }
    
    public QueryContext sort( Sort sorting )
    {
        this.sorting = sorting;
        return this;
    }
    
    public QueryContext defaultOperator( Operator defaultOperator )
    {
        this.defaultOperator = defaultOperator;
        return this;
    }
}
