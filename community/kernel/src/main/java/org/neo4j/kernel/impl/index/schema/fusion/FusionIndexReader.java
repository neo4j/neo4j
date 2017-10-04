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

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.IndexQuery.ExactPredicate;
import org.neo4j.kernel.api.schema.IndexQuery.ExistsPredicate;
import org.neo4j.kernel.api.schema.IndexQuery.NumberRangePredicate;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSchemaIndexProvider.Selector;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.Value;

class FusionIndexReader implements IndexReader
{
    private final IndexReader nativeReader;
    private final IndexReader luceneReader;
    private final Selector selector;

    FusionIndexReader( IndexReader nativeReader, IndexReader luceneReader, Selector selector )
    {
        this.nativeReader = nativeReader;
        this.luceneReader = luceneReader;
        this.selector = selector;
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
}
