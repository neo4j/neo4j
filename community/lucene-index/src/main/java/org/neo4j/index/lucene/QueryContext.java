/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.lucene;

import org.apache.lucene.queryParser.QueryParser.Operator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.impl.lucene.LuceneUtil;

/**
 * This class has the extra query configuration to use
 * with {@link Index#query(Object)} and {@link Index#query(String, Object)}.
 * It allows a query to have sorting, default operators, and allows the engine
 * to turn off searching of modifications made inside a transaction,
 * to gain performance.
 */
public class QueryContext
{
    private final Object queryOrQueryObject;
    private Sort sorting;
    private Operator defaultOperator;
    private boolean tradeCorrectnessForSpeed;
    private int topHits;
    
    public QueryContext( Object queryOrQueryObject )
    {
        this.queryOrQueryObject = queryOrQueryObject;
    }
    
    /**
     * @return the query (or query object) specified in the constructor.
     */
    public Object getQueryOrQueryObject()
    {
        return queryOrQueryObject;
    }

    /**
     * Returns a QueryContext with sorting added to it.
     *
     * @param sorting The sorting to be used
     * @return A QueryContext with the sorting applied.
     */
    public QueryContext sort( Sort sorting )
    {
        this.sorting = sorting;
        return this;
    }

    /**
     * Returns a QueryContext with sorting added to it.
     *
     * @param key The key to sort on.
     * @param additionalKeys Any additional keys to sort on.
     * @return A QueryContext with sorting added to it.
     */
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
    
    /**
     * @return a QueryContext with sorting by relevance, i.e. sorted after which
     * score each hit has. 
     */
    public QueryContext sortByScore()
    {
        return sort( Sort.RELEVANCE );
    }

    /**
     * Sort the results of a numeric range query if the query in this context
     * is a {@link NumericRangeQuery}, see {@link #numericRange(String, Number, Number)},
     * Otherwise an {@link IllegalStateException} will be thrown.
     * 
     * @param key the key to sort on.
     * @param reversed if the sort order should be reversed or not. {@code true}
     * for lowest first (ascending), {@code false} for highest first (descending)
     * @return a QueryContext with sorting by numeric value.
     */
    public QueryContext sortNumeric( String key, boolean reversed )
    {
        if ( !( queryOrQueryObject instanceof NumericRangeQuery ) )
        {
            throw new IllegalStateException( "Not a numeric range query" );
        }
        
        Number number = ((NumericRangeQuery)queryOrQueryObject).getMin();
        number = number != null ? number : ((NumericRangeQuery)queryOrQueryObject).getMax();
        int fieldType = SortField.INT;
        if ( number instanceof Long )
        {
            fieldType = SortField.LONG;
        }
        else if ( number instanceof Float )
        {
            fieldType = SortField.FLOAT;
        }
        else if ( number instanceof Double )
        {
            fieldType = SortField.DOUBLE;
        }
        sort( new Sort( new SortField( key, fieldType, reversed ) ) );
        return this;
    }
    
    /**
     * Returns the sorting setting for this context.
     * 
     * @return the sorting set with one of the sort methods, f.ex
     * {@link #sort(Sort)} or {@link #sortByScore()}
     */
    public Sort getSorting()
    {
        return this.sorting;
    }
    
    /**
     * Changes the the default operator used between terms in compound queries,
     * default is OR.
     *
     * @param defaultOperator The new operator to use.
     * @return A QueryContext with the new default operator applied.
     */
    public QueryContext defaultOperator( Operator defaultOperator )
    {
        this.defaultOperator = defaultOperator;
        return this;
    }
    
    /**
     * Returns the default operator used between terms in compound queries.
     * 
     * @return the default {@link Operator} specified with
     *         {@link #defaultOperator} or "OR" if none specified.
     */
    public Operator getDefaultOperator()
    {
        return this.defaultOperator;
    }

    /**
     * Adding to or removing from an index affects results from query methods
     * inside the same transaction, even before those changes are committed.
     * To let those modifications be visible in query results, some rather heavy
     * operations may have to be done, which can be slow to complete.
     *
     * The default behavior is that these modifications are visible, but using
     * this method will tell the query to not strive to include the absolutely
     * latest modifications, so that such a performance penalty can be avoided.
     *
     * @return A QueryContext which doesn't necessarily include the latest
     * transaction modifications in the results, but may perform faster.
     */
    public QueryContext tradeCorrectnessForSpeed()
    {
        this.tradeCorrectnessForSpeed = true;
        return this;
    }
    
    /**
     * Returns {@code true} if this context is set to prioritize speed over
     * the inclusion of transactional state in the results.
     * @return whether or not {@link #tradeCorrectnessForSpeed()} has been called.
     */
    public boolean getTradeCorrectnessForSpeed()
    {
        return tradeCorrectnessForSpeed;
    }
    
    /**
     * Makes use of {@link IndexSearcher#search(org.apache.lucene.search.Query, int)},
     * alternatively {@link IndexSearcher#search(org.apache.lucene.search.Query, org.apache.lucene.search.Filter, int, Sort)}
     * where only the top {@code numberOfTopHits} hits are returned. Default
     * behavior is to return all hits, although lazily retrieved from lucene all
     * the way up to the {@link IndexHits} iterator.
     * 
     * @param numberOfTopHits the maximum number of top hits to return.
     * @return A {@link QueryContext} with the number of top hits set.
     */
    public QueryContext top( int numberOfTopHits )
    {
        this.topHits = numberOfTopHits;
        return this;
    }
    
    /**
     * Return the max number of results to be returned.
     * 
     * @return the top hits set with {@link #top(int)}.
     */
    public int getTop()
    {
        return this.topHits;
    }
    
    /**
     * Will create a {@link QueryContext} with a query for numeric ranges, that is
     * values that have been indexed using {@link ValueContext#indexNumeric()}.
     * {@code from} (lower) and {@code to} (higher) bounds are inclusive.
     * It will match the type of numbers supplied to the type of values that
     * are indexed in the index, f.ex. long, int, float and double.
     * If both {@code from} and {@code to} is {@code null} then it will default
     * to int.
     * 
     * @param key the property key to query.
     * @param from the low end of the range (inclusive)
     * @param to the high end of the range (inclusive)
     * @return a {@link QueryContext} to do numeric range queries with.
     */
    public static QueryContext numericRange( String key, Number from, Number to )
    {
        return numericRange( key, from, to, true, true );
    }
    
    /**
     * Will create a {@link QueryContext} with a query for numeric ranges, that is
     * values that have been indexed using {@link ValueContext#indexNumeric()}.
     * It will match the type of numbers supplied to the type of values that
     * are indexed in the index, f.ex. long, int, float and double.
     * If both {@code from} and {@code to} is {@code null} then it will default
     * to int.
     * 
     * @param key the property key to query.
     * @param from the low end of the range (inclusive)
     * @param to the high end of the range (inclusive)
     * @param includeFrom whether or not {@code from} (the lower bound) is inclusive
     * or not.
     * @param includeTo whether or not {@code to} (the higher bound) is inclusive
     * or not.
     * @return a {@link QueryContext} to do numeric range queries with.
     */
    public static QueryContext numericRange( String key, Number from, Number to,
            boolean includeFrom, boolean includeTo )
    {
        return new QueryContext( LuceneUtil.rangeQuery( key, from, to, includeFrom, includeTo ) );
    }
}
