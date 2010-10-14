/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.index.impl.lucene;

import org.apache.lucene.queryParser.QueryParser.Operator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.neo4j.graphdb.index.Index;

/**
 * This class has the extra query configuration to use
 * with {@link Index}. It allows a query to have sorting,
 * default operators, and allows the engine to return
 * even inside a transaction that has introduced modifications.
 */
public class QueryContext
{
    final Object queryOrQueryObject;
    Sort sorting;
    Operator defaultOperator;
    boolean allowQueryingModifications;
    
    public QueryContext( Object queryOrQueryObject )
    {
        this.queryOrQueryObject = queryOrQueryObject;
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
     * Lucenes default operator is OR. Using this method, the default operator
     * can be changed for the query.
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
     * Adding or removing data from an index should affect the index results
     * inside the transaction, even if it's not committed. To do this, some
     * rather heavy operations have to be done, which can be slow to complete.
     *
     * The default is to not allow these slow, but correct, queries. If you
     * do want them, this is the method you are looking for.
     *
     * @return  A QueryContext with the slow but correct setting turned on.
     */
    public QueryContext allowQueryingModifications()
    {
        this.allowQueryingModifications = true;
        return this;
    }
}
