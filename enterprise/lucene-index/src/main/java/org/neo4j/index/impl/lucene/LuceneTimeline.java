package org.neo4j.index.impl.lucene;

import static java.lang.Long.MAX_VALUE;
import static org.apache.lucene.search.NumericRangeQuery.newLongRange;
import static org.neo4j.index.impl.lucene.ValueContext.numeric;

import java.util.Map;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

public class LuceneTimeline<T extends PropertyContainer> implements TimelineIndex<T>
{
    private static final String FIELD = "timestamp";
    private final Index<T> index;

    public LuceneTimeline( GraphDatabaseService db, Index<T> index )
    {
        assertIsLuceneIndex( db, index );
        this.index = index;
    }
    
    private void assertIsLuceneIndex( GraphDatabaseService db, Index<T> index )
    {
        Map<String, String> config = db.index().getConfiguration( index );
        if ( !config.get( "provider" ).equals( "lucene" ) ) // Not so hard coded please
        {
            throw new IllegalArgumentException( index + " isn't a Lucene index" );
        }
    }

    private T getSingle( boolean reversed )
    {
        IndexHits<T> hits = index.query( sort( everythingQuery().topDocs( 1 ), reversed ) );
        return hits.getSingle();
    }
    
    private QueryContext everythingQuery()
    {
        return new QueryContext( newLongRange( FIELD, 0L, MAX_VALUE, true, true ) );
    }

    private QueryContext rangeQuery( Long startTimestampOrNull, Long endTimestampOrNull )
    {
        long start = startTimestampOrNull != null ? startTimestampOrNull : 0L;
        long end = endTimestampOrNull != null ? endTimestampOrNull : MAX_VALUE;
        return new QueryContext( newLongRange( FIELD, start, end, false, false ) );
    }
    
    private QueryContext sort( QueryContext query, boolean reversed )
    {
        return query.sort( new Sort( new SortField( FIELD, SortField.LONG, reversed ) ) );
    }
    
    @Override
    public T getLast()
    {
        return getSingle( true );
    }

    @Override
    public T getFirst()
    {
        return getSingle( false );
    }

    @Override
    public void remove( T entity, long timestamp )
    {
        index.remove( entity, FIELD, timestamp );
    }

    @Override
    public void add( T entity, long timestamp )
    {
        index.add( entity, FIELD, numeric( timestamp ) );
    }
    
    @Override
    public IndexHits<T> getBetween( Long startTimestampOrNull, Long endTimestampOrNull )
    {
        return index.query( sort( rangeQuery( startTimestampOrNull, endTimestampOrNull ), false ) );
    }

    @Override
    public IndexHits<T> getBetweenReversed( Long startTimestampOrNull, Long endTimestampOrNull )
    {
        return index.query( sort( rangeQuery( startTimestampOrNull, endTimestampOrNull ), true ) );
    }
}
