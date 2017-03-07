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
package org.neo4j.kernel.impl.api;

import java.util.Arrays;
import java.util.function.LongPredicate;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.schema_new.IndexQuery;
import org.neo4j.kernel.impl.api.operations.EntityOperations;
import org.neo4j.storageengine.api.NodeItem;

/**
 * When looking up nodes by a property value, we have to do a two-stage check.
 * The first stage is to look up the value in lucene, that will find us nodes that may have
 * the correct property value.
 * Then the second stage is to ensure the values actually match the value we are looking for,
 * which requires us to load the actual property value and filter the result we got in the first stage.
 * <p>This class defines the methods for the second stage check.<p>
 */
public class LookupFilter
{
    /**
     * used by the consistency checker
     */
    public static PrimitiveLongIterator exactIndexMatches( PropertyAccessor accessor,
            PrimitiveLongIterator indexedNodeIds, int propertyKeyId, Object value )
    {
        if ( isNumberOrArray( value ) )
        {
            return PrimitiveLongCollections.filter( indexedNodeIds,
                    new LookupBasedExactMatchPredicate( accessor, propertyKeyId,
                            value ) );
        }
        return indexedNodeIds;
    }

    /**
     * used in "normal" operation
     */
    public static PrimitiveLongIterator exactIndexMatches( EntityOperations operations, KernelStatement state,
                                                           PrimitiveLongIterator indexedNodeIds, IndexQuery... predicates )
    {
        if ( !indexedNodeIds.hasNext() )
        {
            return indexedNodeIds;
        }

        IndexQuery[] numericPredicates =
                Arrays.stream( predicates )
                        .filter( LookupFilter::isNumericPredicate )
                        .toArray( IndexQuery[]::new );

        if ( numericPredicates.length > 0 )
        {
            LongPredicate combinedPredicate = nodeId ->
            {
                try ( Cursor<NodeItem> node = operations.nodeCursorById( state, nodeId ) )
                {
                    NodeItem nodeItem = node.get();
                    for ( IndexQuery predicate : numericPredicates )
                    {
                        int propertyKeyId = predicate.propertyKeyId();
                        Object value = operations.nodeGetProperty( state, nodeItem, propertyKeyId );
                        if ( !predicate.test( value ) )
                        {
                            return false;
                        }
                    }
                    return true;
                }
                catch ( EntityNotFoundException ignored )
                {
                    return false;
                }
            };
            return PrimitiveLongCollections.filter( indexedNodeIds, combinedPredicate );
        }
        return indexedNodeIds;
    }

    private static boolean isNumericPredicate( IndexQuery predicate )
    {

        if ( predicate.type() == IndexQuery.IndexQueryType.exact )
        {
            IndexQuery.ExactPredicate exactPredicate = (IndexQuery.ExactPredicate) predicate;
            if ( isNumberOrArray( exactPredicate.value() ) )
            {
                return true;
            }
        }
        else if ( predicate.type() == IndexQuery.IndexQueryType.rangeNumeric )
        {
            return true;
        }
        return false;
    }

    private static boolean isNumberOrArray( Object value )
    {
        return value instanceof Number || value.getClass().isArray();
    }

    /**
     * used by CC
     */
    private static class LookupBasedExactMatchPredicate implements LongPredicate
    {
        final PropertyAccessor accessor;
        private final int propertyKeyId;
        private final Object value;

        LookupBasedExactMatchPredicate( PropertyAccessor accessor, int propertyKeyId, Object value )
        {
            this.accessor = accessor;
            this.propertyKeyId = propertyKeyId;
            this.value = value;
        }

        @Override
        public boolean test( long nodeId )
        {
            try
            {
                return accessor.getProperty( nodeId, propertyKeyId ).valueEquals( value );
            }
            catch ( EntityNotFoundException ignored )
            {
                return false;
            }
        }
    }
}
