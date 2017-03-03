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
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
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

    private abstract static class BaseExactMatchPredicate implements LongPredicate
    {
        private final int propertyKeyId;
        private final Object value;

        BaseExactMatchPredicate( int propertyKeyId, Object value )
        {
            this.propertyKeyId = propertyKeyId;
            this.value = value;
        }

        @Override
        public boolean test( long nodeId )
        {
            try
            {
                return nodeProperty( nodeId, propertyKeyId ).valueEquals( value );
            }
            catch ( EntityNotFoundException ignored )
            {
                return false;
            }
        }

        abstract Property nodeProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException;
    }

    /**
<<<<<<< HEAD
     * used by "normal" operation
     */
    private static class OperationsBasedExactMatchPredicate extends BaseExactMatchPredicate
    {
        final EntityReadOperations readOperations;
        final KernelStatement state;

        OperationsBasedExactMatchPredicate( EntityReadOperations readOperations, KernelStatement state,
                int propertyKeyId, Object value )
        {
            super( propertyKeyId, value );
            this.readOperations = readOperations;
            this.state = state;
        }

        @Override
        Property nodeProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException
        {
            try ( Cursor<NodeItem> node = readOperations.nodeCursorById( state, nodeId ) )
            {
                Object value = readOperations.nodeGetProperty( state, node.get(), propertyKeyId );
                return value == null ? Property.noNodeProperty( nodeId, propertyKeyId )
                                     : Property.property( propertyKeyId, value );
            }
        }
    }

    /**
=======
>>>>>>> Initial implementation of composite indexes in TxState
     * used by CC
     */
    private static class LookupBasedExactMatchPredicate extends BaseExactMatchPredicate
    {
        final PropertyAccessor accessor;

        LookupBasedExactMatchPredicate( PropertyAccessor accessor, int propertyKeyId, Object value )
        {
            super( propertyKeyId, value );
            this.accessor = accessor;
        }

        @Override
        Property nodeProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException
        {
            return accessor.getProperty( nodeId, propertyKeyId );
        }
    }

    static class NumericRangeMatchPredicate implements LongPredicate
    {
        final EntityOperations readOperations;
        final KernelStatement state;
        final int propertyKeyId;
        final Number lower;
        final boolean includeLower;
        final Number upper;
        final boolean includeUpper;

        NumericRangeMatchPredicate( EntityOperations readOperations, KernelStatement state, int propertyKeyId,
                Number lower, boolean includeLower, Number upper, boolean includeUpper )
        {
            this.readOperations = readOperations;
            this.state = state;
            this.propertyKeyId = propertyKeyId;
            this.lower = lower;
            this.includeLower = includeLower;
            this.upper = upper;
            this.includeUpper = includeUpper;
        }

        @Override
        public boolean test( long nodeId )
        {
            try ( Cursor<NodeItem> node = readOperations.nodeCursorById( state, nodeId ) )
            {
                return inRange( readOperations.nodeGetProperty( state, node.get(), propertyKeyId ) );
            }
            catch ( EntityNotFoundException e )
            {
                return false;
            }
        }

        boolean inRange( Object value )
        {
            if ( value == null )
            {
                return false;
            }
            if ( !(value instanceof Number) )
            {
                throw new IllegalStateException( "Unable to verify range for non-numeric property " +
                                                 "value: " + value + " for property key: " + propertyKeyId );
            }
            Number number = (Number) value;
            if ( lower != null )
            {
                int compare = PropertyValueComparison.COMPARE_NUMBERS.compare( number, lower );
                if ( compare < 0 || !includeLower && compare == 0 )
                {
                    return false;
                }
            }
            if ( upper != null )
            {
                int compare = PropertyValueComparison.COMPARE_NUMBERS.compare( number, upper );
                if ( compare > 0 || !includeUpper && compare == 0 )
                {
                    return false;
                }
            }
            return true;
        }
    }
}
