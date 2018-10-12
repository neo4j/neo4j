/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.QueryBuilder;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

public abstract class FulltextIndexReader implements IndexReader
{
    /**
     * Queires the fulltext index with the given lucene-syntax query
     *
     * @param query the lucene query
     * @return A {@link ScoreEntityIterator} over the results
     */
    public ScoreEntityIterator query( String query ) throws ParseException
    {
        FulltextIndexDescriptor descriptor = getDescriptor();
        MultiFieldQueryParser multiFieldQueryParser = new MultiFieldQueryParser( descriptor.propertyNames(), descriptor.analyzer() );
        Query queryObject = multiFieldQueryParser.parse( query );
        return indexQuery( queryObject );
    }

    protected abstract ScoreEntityIterator indexQuery( Query query );

    @Override
    public IndexSampler createSampler()
    {
        return IndexSampler.EMPTY;
    }

    @Override
    public PrimitiveLongResourceIterator query( IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        throw new IndexNotApplicableKernelException( "Fulltext indexes does not support IndexQuery queries" );
    }

    @Override
    public void query( IndexProgressor.EntityValueClient client, IndexOrder indexOrder, boolean needsValues, IndexQuery... queries )
            throws IndexNotApplicableKernelException
    {
        QueryBuilder qb = new QueryBuilder( getDescriptor().analyzer() );
        BooleanQuery query = new BooleanQuery();
        for ( IndexQuery indexQuery : queries )
        {
            String propertyKeyName;
            String searchTerm;
            try
            {
                propertyKeyName = getPropertyKeyName( indexQuery.propertyKeyId() );
            }
            catch ( TokenNotFoundException e )
            {
                throw new IndexNotApplicableKernelException( "No property key name found for property key token: " + indexQuery.propertyKeyId() );
            }

            switch ( indexQuery.type() )
            {
            case exact:
                IndexQuery.ExactPredicate predicate = (IndexQuery.ExactPredicate) indexQuery;
                if ( predicate.valueGroup() != ValueGroup.TEXT )
                {
                    throw new IndexNotApplicableKernelException( "A fulltext schema index cannot be used to search for exact matches of non-string typed " +
                            "values, but the given value was in type-group " + predicate.valueGroup() + "." );
                }
                String stringValue = predicate.value().asObject().toString();
                searchTerm = QueryParser.escape( stringValue );
                query.add( qb.createBooleanQuery( propertyKeyName, searchTerm ), BooleanClause.Occur.SHOULD );
                break;
            case stringContains:
                searchTerm = QueryParser.escape( ((IndexQuery.StringContainsPredicate) indexQuery).contains() );
                query.add( qb.createBooleanQuery( propertyKeyName, "*" + searchTerm + "*" ), BooleanClause.Occur.SHOULD );
                break;
            case stringPrefix:
                searchTerm = QueryParser.escape( ((IndexQuery.StringPrefixPredicate) indexQuery).prefix() );
                query.add( qb.createBooleanQuery( propertyKeyName, searchTerm + "*" ), BooleanClause.Occur.SHOULD );
                break;
            case stringSuffix:
                searchTerm = QueryParser.escape( ((IndexQuery.StringSuffixPredicate) indexQuery).suffix() );
                query.add( qb.createBooleanQuery( propertyKeyName, "*" + searchTerm ), BooleanClause.Occur.SHOULD );
                break;
            default:
                throw new IndexNotApplicableKernelException( "A fulltext schema index cannot answer " + indexQuery.type() + " queries." );
            }
        }
        ScoreEntityIterator itr = indexQuery( query );
        IndexProgressor progressor = new IndexProgressor()
        {
            @Override
            public boolean next()
            {
                if ( !itr.hasNext() )
                {
                    return false;
                }
                ScoreEntityIterator.ScoreEntry entry;
                boolean accepted;
                do
                {
                    entry = itr.next();
                    accepted = client.acceptEntity( entry.entityId(), entry.score(), (Value[]) null );
                }
                while ( !accepted && itr.hasNext() );
                return accepted;
            }

            @Override
            public void close()
            {
            }
        };
        client.initialize( getDescriptor(), progressor, queries, indexOrder, needsValues );
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        return false;
    }

    protected abstract String getPropertyKeyName( int propertyKey ) throws TokenNotFoundException;

    protected abstract FulltextIndexDescriptor getDescriptor();
}
