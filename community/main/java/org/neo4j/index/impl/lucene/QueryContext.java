package org.neo4j.index.impl.lucene;

import org.apache.lucene.queryParser.QueryParser.Operator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

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
    
    public QueryContext sort( String key, String... additionalKeys )
    {
        SortField firstSortField = new SortField( key, SortField.STRING );
        if ( additionalKeys.length == 0 )
        {
            return sort( new Sort( firstSortField ) );
        }
        
        SortField[] sortFields = new SortField[1+additionalKeys.length];
        sortFields[0] = firstSortField;
        for ( int i = 0; i < additionalKeys.length; i++ )
        {
            sortFields[1+i] = new SortField( additionalKeys[i], SortField.STRING );
        }
        return sort( new Sort( sortFields ) );
    }
    
    public QueryContext defaultOperator( Operator defaultOperator )
    {
        this.defaultOperator = defaultOperator;
        return this;
    }
}
