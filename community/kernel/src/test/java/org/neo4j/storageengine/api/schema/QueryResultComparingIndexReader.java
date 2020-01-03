/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.storageengine.api.schema;

import org.neo4j.collection.PrimitiveLongResourceCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

/**
 * {@link IndexReader} that executes and compares results from both query methods, as long as they should exist, when using the either of the two query methods.
 */
public class QueryResultComparingIndexReader implements IndexReader
{
    private final IndexReader actual;

    public QueryResultComparingIndexReader( IndexReader actual )
    {
        this.actual = actual;
    }

    @Override
    public long countIndexedNodes( long nodeId, int[] propertyKeyIds, Value... propertyValues )
    {
        return actual.countIndexedNodes( nodeId, propertyKeyIds, propertyValues );
    }

    @Override
    public IndexSampler createSampler()
    {
        return actual.createSampler();
    }

    @Override
    public PrimitiveLongResourceIterator query( IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        PrimitiveLongResourceIterator mainResult = actual.query( predicates );

        // Also call the other query method and bake comparison from it into a wrapped version of this iterator
        NodeValueIterator otherResult = new NodeValueIterator();
        actual.query( otherResult, IndexOrder.NONE, false, predicates );
        return new PrimitiveLongResourceCollections.PrimitiveLongBaseResourceIterator( mainResult )
        {
            @Override
            protected boolean fetchNext()
            {
                if ( mainResult.hasNext() )
                {
                    long mainValue = mainResult.next();
                    if ( !otherResult.hasNext() )
                    {
                        throw new IllegalStateException(
                                format( "Legacy query method returned %d, but new query method didn't have more values in it", mainValue ) );
                    }
                    long otherValue = otherResult.next();
                    if ( mainValue != otherValue )
                    {
                        throw new IllegalStateException( format( "Query methods disagreeing on next value legacy:%d new:%d", mainValue, otherValue ) );
                    }
                    return next( mainValue );
                }
                else if ( otherResult.hasNext() )
                {
                    throw new IllegalStateException( format( "Legacy query method exhausted, but new query method had more %d", otherResult.next() ) );
                }
                return false;
            }

            @Override
            public void close()
            {
                super.close();
                otherResult.close();
            }
        };
    }

    @Override
    public void query( IndexProgressor.NodeValueClient client, IndexOrder indexOrder, boolean needsValues, IndexQuery... query )
            throws IndexNotApplicableKernelException
    {
        // Also call the other query method and bake comparison from it into a wrapped version of this iterator
        PrimitiveLongResourceIterator otherResult = actual.query( query );

        // This is a client which gets driven by the client, such that it can know when there are no more values in it.
        // Therefore we can hook in correct comparison on this type of client.
        // Also call the other query method and bake comparison from it into a wrapped version of this iterator
        IndexProgressor.NodeValueClient wrappedClient = new IndexProgressor.NodeValueClient()
        {
            private long mainValue;

            @Override
            public void initialize( IndexDescriptor descriptor, IndexProgressor progressor, IndexQuery[] query, IndexOrder indexOrder, boolean needsValues )
            {
                IndexProgressor wrappedProgressor = new IndexProgressor()
                {
                    @Override
                    public boolean next()
                    {
                        mainValue = NO_ID;
                        if ( progressor.next() )
                        {
                            if ( !otherResult.hasNext() )
                            {
                                throw new IllegalStateException(
                                        format( "new query method returned %d, but legacy query method didn't have more values in it", mainValue ) );
                            }
                            long otherValue = otherResult.next();
                            if ( mainValue != otherValue )
                            {
                                throw new IllegalStateException( format( "Query methods disagreeing on next value new:%d legacy:%d", mainValue, otherValue ) );
                            }
                            return true;
                        }
                        else if ( otherResult.hasNext() )
                        {
                            throw new IllegalStateException( format( "New query method exhausted, but legacy query method had more %d", otherResult.next() ) );
                        }
                        return false;
                    }

                    @Override
                    public void close()
                    {
                        progressor.close();
                        otherResult.close();
                    }
                };

                client.initialize( descriptor, wrappedProgressor, query, indexOrder, needsValues );
            }

            @Override
            public boolean acceptNode( long reference, Value... values )
            {
                mainValue = reference;
                return client.acceptNode( reference, values );
            }

            @Override
            public boolean needsValues()
            {
                return client.needsValues();
            }
        };

        actual.query( wrappedClient, indexOrder, needsValues, query );
    }

    @Override
    public void distinctValues( IndexProgressor.NodeValueClient client, NodePropertyAccessor propertyAccessor, boolean needsValues )
    {
        actual.distinctValues( client, propertyAccessor, needsValues );
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        return actual.hasFullValuePrecision( predicates );
    }

    @Override
    public void close()
    {
        actual.close();
    }
}
