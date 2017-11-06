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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.util.ArrayDeque;
import java.util.Queue;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.ExistsPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.NumberRangePredicate;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSchemaIndexProvider.Selector;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.Value;

class FusionIndexReader implements IndexReader
{
    private final IndexReader nativeReader;
    private final IndexReader luceneReader;
    private final Selector selector;
    private final int[] propertyKeys;

    FusionIndexReader( IndexReader nativeReader, IndexReader luceneReader, Selector selector, int[] propertyKeys )
    {
        this.nativeReader = nativeReader;
        this.luceneReader = luceneReader;
        this.selector = selector;
        this.propertyKeys = propertyKeys;
    }

    @Override
    public void close()
    {
        try
        {
            nativeReader.close();
        }
        finally
        {
            luceneReader.close();
        }
    }

    @Override
    public long countIndexedNodes( long nodeId, Value... propertyValues )
    {
        return selector.select( nativeReader, luceneReader, propertyValues ).countIndexedNodes( nodeId, propertyValues );
    }

    @Override
    public IndexSampler createSampler()
    {
        return new FusionIndexSampler( nativeReader.createSampler(), luceneReader.createSampler() );
    }

    @Override
    public PrimitiveLongIterator query( IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        if ( predicates.length > 1 )
        {
            return luceneReader.query( predicates );
        }

        if ( predicates[0] instanceof ExactPredicate )
        {
            ExactPredicate exactPredicate = (ExactPredicate) predicates[0];
            return selector.select( nativeReader, luceneReader, exactPredicate.value() ).query( predicates );
        }

        if ( predicates[0] instanceof NumberRangePredicate )
        {
            return nativeReader.query( predicates[0] );
        }

        // todo: There will be no ordering of the node ids here. Is this a problem?
        if ( predicates[0] instanceof ExistsPredicate )
        {
            PrimitiveLongIterator nativeResult = nativeReader.query( predicates[0] );
            PrimitiveLongIterator luceneResult = luceneReader.query( predicates[0] );
            return PrimitiveLongCollections.concat( nativeResult, luceneResult );
        }

        return luceneReader.query( predicates );
    }

    @Override
    public void query( IndexProgressor.NodeValueClient cursor, IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        if ( predicates.length > 1 )
        {
            luceneReader.query( cursor, predicates );
            return;
        }
        else if ( predicates[0] instanceof ExactPredicate )
        {
            ExactPredicate exactPredicate = (ExactPredicate) predicates[0];
            selector.select( nativeReader, luceneReader, exactPredicate.value() ).query( cursor, predicates );
            return;
        }
        else if ( predicates[0] instanceof NumberRangePredicate )
        {
            nativeReader.query( cursor, predicates[0] );
            return;
        }
        // todo: There will be no ordering of the node ids here. Is this a problem?
        else if ( predicates[0] instanceof ExistsPredicate )
        {
            MultiProgressorNodeValueCursor multiProgressor = new MultiProgressorNodeValueCursor( cursor, propertyKeys );
            cursor.initialize( multiProgressor, propertyKeys );
            nativeReader.query( multiProgressor, predicates[0] );
            luceneReader.query( multiProgressor, predicates[0] );
            return;
        }
        luceneReader.query( cursor, predicates );
    }

    @Override
    public boolean hasFullNumberPrecision( IndexQuery... predicates )
    {
        if ( predicates.length > 1 )
        {
            return false;
        }

        IndexQuery predicate = predicates[0];
        if ( predicate instanceof ExactPredicate )
        {
            Value value = ((ExactPredicate) predicate).value();
            return selector.select(
                    nativeReader.hasFullNumberPrecision( predicates ),
                    luceneReader.hasFullNumberPrecision( predicates ), value );
        }
        if ( predicates[0] instanceof NumberRangePredicate )
        {
            return nativeReader.hasFullNumberPrecision( predicates );
        }
        return false;
    }

    private class MultiProgressorNodeValueCursor implements IndexProgressor.NodeValueClient, IndexProgressor
    {
        private final NodeValueClient cursor;
        private final int[] keys;
        private final Queue<IndexProgressor> progressors;
        private boolean initialized;
        private IndexProgressor current;

        MultiProgressorNodeValueCursor( NodeValueClient cursor, int[] keys )
        {
            this.cursor = cursor;
            this.keys = keys;
            progressors = new ArrayDeque<>();
        }

        @Override
        public boolean next()
        {
            if ( current == null )
            {
                current = progressors.poll();
            }
            while ( current != null )
            {
                if ( current.next() )
                {
                    return true;
                }
                else
                {
                    current = progressors.poll();
                }
            }
            return false;
        }

        @Override
        public void close()
        {
            progressors.forEach( IndexProgressor::close );
        }

        @Override
        public void initialize( IndexProgressor progressor, int[] keys )
        {
            assertKeysAlign( keys );
            progressors.add( progressor );
        }

        private void assertKeysAlign( int[] keys )
        {
            for ( int i = 0; i < this.keys.length; i++ )
            {
                if ( this.keys[i] != keys[i] )
                {
                    throw new UnsupportedOperationException( "Can not chain multiple progressors with different key set." );
                }
            }
        }

        @Override
        public boolean acceptNode( long reference, Value[] values )
        {
            return cursor.acceptNode( reference, values );
        }

        @Override
        public void done()
        {
            cursor.done();
        }
    }
}
