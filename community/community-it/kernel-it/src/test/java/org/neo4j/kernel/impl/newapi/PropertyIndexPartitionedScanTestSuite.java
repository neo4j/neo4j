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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider;
import org.neo4j.kernel.impl.index.schema.fusion.NativeLuceneFusionIndexProviderFactory30;
import org.neo4j.kernel.impl.newapi.PartitionedScanTestSuite.Query;
import org.neo4j.test.RandomSupport;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

abstract class PropertyIndexPartitionedScanTestSuite<QUERY extends Query<?>, CURSOR extends Cursor>
        implements PartitionedScanTestSuite.TestSuite<QUERY,IndexReadSession,CURSOR>
{
    // range for range based queries, other value type ranges are calculated from this for consistency
    // as using an int as source of values, ~half of ints will be covered by this range
    private static final Pair<Integer,Integer> RANGE = Pair.of( Integer.MIN_VALUE / 2, Integer.MAX_VALUE / 2 );

    private final IndexType index;

    PropertyIndexPartitionedScanTestSuite( IndexType index )
    {
        this.index = index;
    }

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
                          .map( indexFrom -> IndexPrototype.forSchema( indexFrom.first() )
                                                           .withIndexType( index.type() )
                                                           .withIndexProvider( index.descriptor() )
                                                           .withName( indexFrom.other() ) )
                          .collect( Collectors.toUnmodifiableList() );
    }

    abstract static class WithoutData<QUERY extends Query<?>, CURSOR extends Cursor>
            extends PartitionedScanTestSuite.WithoutData<QUERY,IndexReadSession,CURSOR>
    {
        protected final PartitionedScanFactories.PropertyIndex<QUERY,CURSOR> factory;

        WithoutData( PropertyIndexPartitionedScanTestSuite<QUERY,CURSOR> testSuite )
        {
            super( testSuite );
            factory = (PartitionedScanFactories.PropertyIndex<QUERY,CURSOR>) testSuite.getFactory();
        }
    }

    abstract static class WithData<QUERY extends Query<?>, CURSOR extends Cursor>
            extends PartitionedScanTestSuite.WithData<QUERY,IndexReadSession,CURSOR>
    {
        protected final PartitionedScanFactories.PropertyIndex<QUERY,CURSOR> factory;

        protected abstract Queries<QUERY> createData( int numberOfProperties, Pair<Integer,int[]> tokenAndPropKeyCombination );

        WithData( PropertyIndexPartitionedScanTestSuite<QUERY,CURSOR> testSuite )
        {
            super( testSuite );
            this.factory = (PartitionedScanFactories.PropertyIndex<QUERY,CURSOR>) testSuite.getFactory();
        }

    }

    protected static final class PropertyRecord
    {
        final int id;
        final Value value;
        final ValueType type;

        PropertyRecord( int id, Value value, ValueType type )
        {
            this.id = id;
            this.value = value;
            this.type = type;
        }
    }

    protected static PropertyRecord createRandomPropertyRecord( RandomSupport random, int propKeyId, int value )
    {
        final var type = random.among( ValueType.values() );
        return new PropertyRecord( propKeyId, type.toValue( value ), type );
    }

    protected static Stream<PropertyIndexQuery> queries( PropertyRecord prop )
    {
        if ( prop == null )
        {
            return Stream.of();
        }

        final var general = Stream.of(
                PropertyIndexQuery.exists( prop.id ),
                PropertyIndexQuery.exact( prop.id, prop.value ),
                PropertyIndexQuery.range( prop.id, prop.type.toValue( RANGE.first() ), true, prop.type.toValue( RANGE.other() ), false ) );

        final var text = Stream.of(
                PropertyIndexQuery.stringPrefix( prop.id, Values.utf8Value( "1" ) ),
                PropertyIndexQuery.stringSuffix( prop.id, Values.utf8Value( "1" ) ),
                PropertyIndexQuery.stringContains( prop.id, Values.utf8Value( "1" ) ) );

        final var queries = prop.type == ValueType.TEXT
                            ? Stream.concat( general, text )
                            : general;

        return queries.filter( query -> query.acceptsValue( prop.value ) );
    }

    protected static Stream<PropertyIndexQuery[]> queries( PropertyRecord... props )
    {
        final var allSingleQueries = Arrays.stream( props )
                                           .map( PropertyIndexPartitionedScanTestSuite::queries )
                                           .map( queries -> queries.collect( Collectors.toUnmodifiableList() ) )
                                           .collect( Collectors.toUnmodifiableList() );

        // cartesian product of all single queries that match
        Stream<List<PropertyIndexQuery>> compositeQueries = Stream.of( List.of() );
        for ( final var singleQueries : allSingleQueries )
        {
            compositeQueries = compositeQueries.flatMap( prev ->
                singleQueries.stream().map( extra ->
                {
                    final var prevWithExtra = new ArrayList<>( prev );
                    prevWithExtra.add( extra );
                    return prevWithExtra;
                } ) );
        }

        return compositeQueries.map( compositeQuery -> compositeQuery.toArray( PropertyIndexQuery[]::new ) );
    }

    protected enum ValueType
    {
        NUMBER
        {
            @Override
            protected Integer createUnderlyingValue( int value )
            {
                return value;
            }
        },

        NUMBER_ARRAY
        {
            @Override
            protected Integer[] createUnderlyingValue( int value )
            {
                return splitNumber( value ).mapToObj( NUMBER::createUnderlyingValue )
                                           .map( Integer.class::cast )
                                           .toArray( Integer[]::new );
            }
        },

        TEXT
        {
            @Override
            protected String createUnderlyingValue( int value )
            {
                return String.valueOf( value );
            }
        },

        TEXT_ARRAY
        {
            @Override
            protected String[] createUnderlyingValue( int value )
            {
                return splitNumber( value ).mapToObj( TEXT::createUnderlyingValue )
                                           .map( String.class::cast )
                                           .toArray( String[]::new );
            }
        },

        GEOMETRY
        {
            @Override
            protected PointValue createUnderlyingValue( int value )
            {
                return Values.pointValue( CoordinateReferenceSystem.Cartesian,
                                          splitNumber( value ).asDoubleStream().toArray() );
            }
        },

        GEOMETRY_ARRAY
        {
            @Override
            protected PointValue[] createUnderlyingValue( int value )
            {
                return splitNumber( value ).mapToObj( GEOMETRY::createUnderlyingValue )
                                           .map( PointValue.class::cast )
                                           .toArray( PointValue[]::new );
            }
        },

        TEMPORAL
        {
            @Override
            protected ZonedDateTime createUnderlyingValue( int value )
            {
                return ZonedDateTime.ofInstant( Instant.ofEpochSecond( value ), ZoneOffset.UTC );
            }
        },

        TEMPORAL_ARRAY
        {
            @Override
            protected ZonedDateTime[] createUnderlyingValue( int value )
            {
                return splitNumber( value ).mapToObj( TEMPORAL::createUnderlyingValue )
                                           .map( ZonedDateTime.class::cast )
                                           .toArray( ZonedDateTime[]::new );
            }
        },

        BOOLEAN
        {
            @Override
            protected Boolean createUnderlyingValue( int value )
            {
                return value % 2 == 0;
            }
        },

        BOOLEAN_ARRAY
        {
            @Override
            protected Boolean[] createUnderlyingValue( int value )
            {
                return splitNumber( value ).mapToObj( BOOLEAN::createUnderlyingValue )
                                           .map( Boolean.class::cast )
                                           .toArray( Boolean[]::new );
            }
        };

        protected abstract Object createUnderlyingValue( int value );

        protected IntStream splitNumber( int value )
        {
            final int mask = Short.MAX_VALUE;
            final int x = value & mask;
            final int y = (value & ~mask) >> Short.SIZE;
            return IntStream.of( x, y );
        }

        public Value toValue( int value )
        {
            return Values.of( createUnderlyingValue( value ) );
        }
    }

    protected enum IndexType
    {
        BTREE( org.neo4j.internal.schema.IndexType.BTREE, GenericNativeIndexProvider.DESCRIPTOR ),
        FUSION( org.neo4j.internal.schema.IndexType.BTREE, NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR );

        private final org.neo4j.internal.schema.IndexType type;
        private final IndexProviderDescriptor descriptor;

        IndexType( org.neo4j.internal.schema.IndexType type, IndexProviderDescriptor descriptor )
        {
            this.type = type;
            this.descriptor = descriptor;
        }

        final org.neo4j.internal.schema.IndexType type()
        {
            return type;
        }

        final IndexProviderDescriptor descriptor()
        {
            return descriptor;
        }
    }
}
