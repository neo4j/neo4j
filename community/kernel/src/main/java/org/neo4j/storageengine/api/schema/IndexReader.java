/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.storageengine.api.schema;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Resource;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.impl.newapi.IndexCursorProgressor;
import org.neo4j.values.storable.Value;

/**
 * Reader for an index. Must honor repeatable reads, which means that if a lookup is executed multiple times the
 * same result set must be returned.
 */
public interface IndexReader extends Resource
{
    /**
     * @param nodeId node id to match.
     * @param propertyValues property values to match.
     * @return number of index entries for the given {@code nodeId} and {@code propertyValues}.
     */
    long countIndexedNodes( long nodeId, Value... propertyValues );

    IndexSampler createSampler();

    /**
     * Queries the index for the given {@link IndexQuery} predicates.
     *
     * @param predicates the predicates to query for.
     * @return the matching entity IDs.
     */
    PrimitiveLongIterator query( IndexQuery... predicates ) throws IndexNotApplicableKernelException;

    /**
     * @param predicates query to determine whether or not index has full number precision for.
     * @return whether or not this reader will only return 100% matching results from {@link #query(IndexQuery...)}
     * when calling with predicates involving numbers, such as {@link IndexQuery#exact(int, Object)}
     * w/ a {@link Number} or {@link IndexQuery#range(int, Number, boolean, Number, boolean)}.
     * If {@code false} is returned this means that the caller of {@link #query(IndexQuery...)} will have to
     * do additional filtering, double-checking of actual property values, externally.
     */
    boolean hasFullNumberPrecision( IndexQuery... predicates );

    IndexReader EMPTY = new IndexReader()
    {
        // Used for checking index correctness
        @Override
        public long countIndexedNodes( long nodeId, Value... propertyValues )
        {
            return 0;
        }

        @Override
        public IndexSampler createSampler()
        {
            return IndexSampler.EMPTY;
        }

        @Override
        public PrimitiveLongIterator query( IndexQuery[] predicates )
        {
            return PrimitiveLongCollections.emptyIterator();
        }

        @Override
        public void close()
        {
        }

        @Override
        public boolean hasFullNumberPrecision( IndexQuery... predicates )
        {
            return true;
        }

        @Override
        public void query( IndexCursorProgressor.NodeValueCursor cursor, IndexQuery... query )
        {
            cursor.done();
        }

        @Override
        public void scan( IndexCursorProgressor.NodeValueCursor cursor )
        {
            cursor.done();
        }
    };

    default void query( IndexCursorProgressor.NodeValueCursor cursor, IndexQuery... query )
    {
        try
        {
            cursor.initialize( new NodeValueIndexProgressor( query( query ), cursor ), null );
        }
        catch ( IndexNotApplicableKernelException e )
        {
            throw new RuntimeException( "SOMEONE FORGOT TO DO EXCEPTION HANDLING", e ); // TODO: exception handling
        }
    }

    default void scan( IndexCursorProgressor.NodeValueCursor cursor )
    {
        try
        {
            // the actual property key is ignored in the implementation, so we can pass in whatever...
            // it is actually ok for the index implementation to ignore the property key under these two assumptions:
            // 1. That all queries is "well formed" - i.e. we never ask for a key not in the index.
            // 2. For compound indexes all nodes have all properties assigned.
            // While we violate 1. here, we are at least "well intended", we don't actually care about what the key is.
            // 2. holds because compound indexes are only created through node keys.
            IndexQuery.ExistsPredicate scan = IndexQuery.exists( -1 );
            cursor.initialize( new NodeValueIndexProgressor( query( scan ), cursor ), null );
        }
        catch ( IndexNotApplicableKernelException e )
        {
            throw new RuntimeException( "SOMEONE FORGOT TO DO EXCEPTION HANDLING", e ); // TODO: exception handling
        }
    }
}
