/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.newapi;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Collectors;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.impl.newapi.PartitionedScanTestSuite.Query;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

abstract class PropertyIndexPartitionedScanTestSuite<QUERY extends Query<?>, CURSOR extends Cursor>
        implements PartitionedScanTestSuite.TestSuite<QUERY,CURSOR>
{
    protected final Iterable<IndexPrototype> createIndexPrototypes( Pair<Integer,int[]> tokenAndPropKeyCombination )
    {
        // IndexPrototype isn't hashable, and do not wish to alter production code for test;
        // therefore using Pair as a hashable wrapper
        final var indexesFrom = new HashSet<Pair<SchemaDescriptor,String>>();
        final var factory = (PartitionedScanFactories.PropertyIndex<QUERY,CURSOR>) getFactory();
        final var tokenId = tokenAndPropKeyCombination.first();
        final var propKeyIds = tokenAndPropKeyCombination.other();
        for ( final var propKeyId : propKeyIds )
        {
            indexesFrom.add( Pair.of( factory.getSchemaDescriptor( tokenId, propKeyId ), factory.getIndexName( tokenId, propKeyId ) ) );
        }
        indexesFrom.add( Pair.of( factory.getSchemaDescriptor( tokenId, propKeyIds ), factory.getIndexName( tokenId, propKeyIds ) ) );

        return indexesFrom.stream()
                .map( indexFrom -> IndexPrototype.forSchema( indexFrom.first() ).withName( indexFrom.other() ) )
                .collect( Collectors.toUnmodifiableList() );
    }

    abstract static class WithoutData<QUERY extends Query<?>, CURSOR extends Cursor>
            extends PartitionedScanTestSuite.WithoutData<QUERY,CURSOR>
    {
        protected final PartitionedScanFactories.PropertyIndex<QUERY,CURSOR> factory;

        WithoutData( PropertyIndexPartitionedScanTestSuite<QUERY,CURSOR> testSuite )
        {
            super( testSuite );
            this.factory = (PartitionedScanFactories.PropertyIndex<QUERY,CURSOR>) testSuite.getFactory();
        }
    }

    abstract static class WithData<QUERY extends Query<?>, CURSOR extends Cursor>
            extends PartitionedScanTestSuite.WithData<QUERY,CURSOR>
    {
        protected final PartitionedScanFactories.PropertyIndex<QUERY,CURSOR> factory;

        protected abstract EntityIdsMatchingQuery<QUERY> createData( int numberOfProperties,
                                                                     Pair<Integer,int[]> tokenAndPropKeyCombination );

        WithData( PropertyIndexPartitionedScanTestSuite<QUERY,CURSOR> testSuite )
        {
            super( testSuite );
            this.factory = (PartitionedScanFactories.PropertyIndex<QUERY,CURSOR>) testSuite.getFactory();
        }

        protected Value createValue( int value )
        {
            final var type = random.among( ValueTypes.values() );
            return type.toValue( value );
        }
    }

    protected enum ValueTypes
    {
        NUMBER
        {
            @Override
            protected Integer toValueObject( int value )
            {
                return value;
            }
        },

        BOOLEAN
        {
            @Override
            protected Boolean toValueObject( int value )
            {
                return value % 2 == 0;
            }
        },

        TEXT
        {
            @Override
            protected String toValueObject( int value )
            {
                return String.valueOf( value );
            }
        },

        TEMPORAL
        {
            @Override
            protected ZonedDateTime toValueObject( int value )
            {
                return ZonedDateTime.ofInstant( Instant.ofEpochSecond( value ), ZoneOffset.UTC );
            }
        },

        POINT
        {
            @Override
            protected PointValue toValueObject( int value )
            {
                final int[] coords = splitNumber( value );
                return Values.pointValue( CoordinateReferenceSystem.Cartesian, Arrays.stream( coords ).asDoubleStream().toArray() );
            }
        },

        ARRAY
        {
            @Override
            protected int[] toValueObject( int value )
            {
                return splitNumber( value );
            }
        };

        protected abstract Object toValueObject( int value );

        protected int[] splitNumber( int value )
        {
            final int mask = Short.MAX_VALUE;
            final int x = value & mask;
            final int y = (value & ~mask) >> Short.SIZE;
            return new int[]{x, y};
        }

        public Value toValue( int value )
        {
            return Values.of( toValueObject( value ) );
        }
    }
}
