/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api.impl.schema.verification;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Base class for strategy used for duplicate check during verification of value uniqueness during
 * constraint creation.
 *
 * Each particular strategy determines how uniqueness check is done and how to accumulate and store those values for
 * to make check time and resource consumption optimal.
 */
abstract class DuplicateCheckStrategy
{
    /**
     * Check uniqueness of multiple properties that belong to a node with provided node id
     * @param values property values
     * @param nodeId checked node id
     * @throws IndexEntryConflictException
     */
    abstract void checkForDuplicate( Value[] values, long nodeId )
            throws IndexEntryConflictException;

    /**
     * Check uniqueness of single property that belong to a node with provided node id.
     * @param value property value
     * @param nodeId checked node id
     * @throws IndexEntryConflictException
     */
    abstract void checkForDuplicate( Value value, long nodeId ) throws IndexEntryConflictException;

    private static boolean propertyValuesEqual( Value[] properties, Value[] values )
    {
        if ( properties.length != values.length )
        {
            return false;
        }
        for ( int i = 0; i < properties.length; i++ )
        {
            if ( !properties[i].equals( values[i] ) )
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Duplicate check strategy that uses plain hash map. Should be optimal for small amount of entries.
     */
    static class MapDuplicateCheckStrategy extends DuplicateCheckStrategy
    {
        private Map<Object,Long> valueNodeIdMap;

        MapDuplicateCheckStrategy( int expectedNumberOfEntries )
        {
            this.valueNodeIdMap = new HashMap<>( expectedNumberOfEntries );
        }

        @Override
        public void checkForDuplicate( Value[] values, long nodeId )
                throws IndexEntryConflictException
        {
            Long previousNodeId = valueNodeIdMap.put( ValueTuple.of( values ), nodeId );
            if ( previousNodeId != null )
            {
                throw new IndexEntryConflictException( previousNodeId, nodeId, ValueTuple.of( values ) );
            }
        }

        @Override
        void checkForDuplicate( Value value, long nodeId ) throws IndexEntryConflictException
        {
            Long previousNodeId = valueNodeIdMap.put( value, nodeId );
            if ( previousNodeId != null )
            {
                throw new IndexEntryConflictException( previousNodeId, nodeId, value );
            }
        }

    }

    /**
     * Strategy that uses arrays to store entries and uses hash codes to split those entries over different buckets.
     * Number of buckets and size of entries block are dynamic and evaluated based on expected number of duplicates.
     */
    static class BucketsDuplicateCheckStrategy extends DuplicateCheckStrategy
    {
        private static final int BASE_ENTRY_SIZE = 1000;
        private static final int DEFAULT_BUCKETS = 10;
        static final int BUCKET_STRATEGY_ENTRIES_THRESHOLD = BASE_ENTRY_SIZE * DEFAULT_BUCKETS;

        private static final int MAX_NUMBER_OF_BUCKETS = 100;
        private final int numberOfBuckets;
        private BucketEntry[] buckets;
        private final int bucketSetSize;

        BucketsDuplicateCheckStrategy()
        {
            this( BUCKET_STRATEGY_ENTRIES_THRESHOLD );
        }

        BucketsDuplicateCheckStrategy( int expectedNumberOfEntries )
        {
            numberOfBuckets = min( MAX_NUMBER_OF_BUCKETS, (expectedNumberOfEntries / BASE_ENTRY_SIZE) + 1 );
            buckets = new BucketEntry[numberOfBuckets];
            bucketSetSize = max( 100, BUCKET_STRATEGY_ENTRIES_THRESHOLD / numberOfBuckets );
        }

        @Override
        public void checkForDuplicate( Value[] values, long nodeId )
                throws IndexEntryConflictException
        {
            BucketEntry current = bucketEntrySet( Arrays.hashCode( values ), bucketSetSize );

            // We either have to find the first conflicting entry set element,
            // or append one for the property we just fetched:
            scan:
            do
            {
                for ( int i = 0; i < bucketSetSize; i++ )
                {
                    Value[] currentValues = (Value[]) current.value[i];

                    if ( current.nodeId[i] == StatementConstants.NO_SUCH_NODE )
                    {
                        current.value[i] = values;
                        current.nodeId[i] = nodeId;
                        if ( i == bucketSetSize - 1 )
                        {
                            current.next = new BucketEntry( bucketSetSize );
                        }
                        break scan;
                    }
                    else if ( propertyValuesEqual( values, currentValues ) )
                    {
                        throw new IndexEntryConflictException( current.nodeId[i], nodeId, currentValues );
                    }
                }
                current = current.next;
            }
            while ( current != null );
        }

        @Override
        void checkForDuplicate( Value propertyValue, long nodeId ) throws IndexEntryConflictException
        {
            BucketEntry current = bucketEntrySet( propertyValue.hashCode(), bucketSetSize );

            // We either have to find the first conflicting entry set element,
            // or append one for the property we just fetched:
            scan:
            do
            {
                for ( int i = 0; i < bucketSetSize; i++ )
                {
                    Value value = (Value) current.value[i];

                    if ( current.nodeId[i] == StatementConstants.NO_SUCH_NODE )
                    {
                        current.value[i] = propertyValue;
                        current.nodeId[i] = nodeId;
                        if ( i == bucketSetSize - 1 )
                        {
                            current.next = new BucketEntry( bucketSetSize );
                        }
                        break scan;
                    }
                    else if ( propertyValue.equals( value ) )
                    {
                        throw new IndexEntryConflictException( current.nodeId[i], nodeId, value );
                    }
                }
                current = current.next;
            }
            while ( current != null );
        }

        private BucketEntry bucketEntrySet( int hashCode, int entrySetSize )
        {
            int bucket = Math.abs( hashCode ) % numberOfBuckets;
            BucketEntry current = buckets[bucket];
            if ( current == null )
            {
                current = new BucketEntry( entrySetSize );
                buckets[bucket] = current;
            }
            return current;
        }

        /**
         * Each bucket entry contains arrays of nodes and corresponding values and link to next BucketEntry in the
         * chain for cases when we have more data then the size of one bucket. So bucket entries will form a
         * chain of entries to represent values in particular bucket
         */
        private static class BucketEntry
        {
            final Object[] value;
            final long[] nodeId;
            BucketEntry next;

            BucketEntry( int entrySize )
            {
                value = new Object[entrySize];
                nodeId = new long[entrySize];
                Arrays.fill( nodeId, StatementConstants.NO_SUCH_NODE );
            }
        }
    }
}
