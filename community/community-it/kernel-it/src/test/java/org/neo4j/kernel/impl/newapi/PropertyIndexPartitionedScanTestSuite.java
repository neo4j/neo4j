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
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.impl.newapi.PartitionedScanTestSuite.EntityIdsMatchingScanQuery;
import org.neo4j.kernel.impl.newapi.PartitionedScanTestSuite.ScanQuery;
import org.neo4j.kernel.impl.newapi.PropertyIndexPartitionedScanTestSuite.PropertyKeyScanQuery;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

abstract class PropertyIndexPartitionedScanTestSuite<CURSOR extends Cursor>
        implements PartitionedScanTestSuite.TestSuite<PropertyKeyScanQuery,CURSOR>
{
    abstract String generateIndexName( int tokenId, int[] propKeyIds );

    final String generateIndexName( int tokenId, int propKeyId )
    {
        return generateIndexName( tokenId, new int[]{propKeyId} );
    }

    protected Iterable<IndexPrototype> createIndexPrototypes( Pair<Integer,int[]> tokenAndPropKeyCombination )
    {
        // IndexPrototype isn't hashable, and do not wish to alter production code for test;
        // therefore using Pair as a hashable wrapper
        final var indexesFrom = new HashSet<Pair<SchemaDescriptor,String>>();
        final var tokenId = tokenAndPropKeyCombination.first();
        final var propKeyIds = tokenAndPropKeyCombination.other();
        for ( var propKeyId : propKeyIds )
        {
            indexesFrom.add( Pair.of( SchemaDescriptors.forLabel( tokenId, propKeyId ), generateIndexName( tokenId, propKeyId ) ) );
        }
        indexesFrom.add( Pair.of( SchemaDescriptors.forLabel( tokenId, propKeyIds ), generateIndexName( tokenId, propKeyIds ) ) );

        return indexesFrom.stream()
                .map( indexFrom -> IndexPrototype.forSchema( indexFrom.first() ).withName( indexFrom.other() ) )
                .collect( Collectors.toUnmodifiableList() );
    }

    abstract static class WithoutData<CURSOR extends Cursor>
            extends PartitionedScanTestSuite.WithoutData<PropertyKeyScanQuery,CURSOR>
    {
        final PropertyIndexPartitionedScanTestSuite<CURSOR> testSuite;

        WithoutData( PropertyIndexPartitionedScanTestSuite<CURSOR> testSuite )
        {
            super( testSuite );
            this.testSuite = testSuite;
        }

        protected EntityIdsMatchingScanQuery<PropertyKeyScanQuery> emptyScanQueries( Pair<Integer,int[]> tokenAndPropKeyCombination )
        {
            final var empty = new EntityIdsMatchingScanQuery<PropertyKeyScanQuery>();
            final var tokenId = tokenAndPropKeyCombination.first();
            final var propKeyIds = tokenAndPropKeyCombination.other();
            for ( var propKeyId : propKeyIds )
            {
                empty.getOrCreate( new PropertyKeyScanQuery( testSuite.generateIndexName( tokenId, propKeyId ),
                                                             PropertyIndexQuery.exists( propKeyId ) ) );
            }
            return empty;
        }
    }

    abstract static class WithData<CURSOR extends Cursor>
            extends PartitionedScanTestSuite.WithData<PropertyKeyScanQuery,CURSOR>
    {
        protected abstract EntityIdsMatchingScanQuery<PropertyKeyScanQuery> createData( int numberOfProperties,
                                                                                        Pair<Integer,int[]> tokenAndPropKeyCombination );

        WithData( PropertyIndexPartitionedScanTestSuite<CURSOR> testSuite )
        {
            super( testSuite );
        }

        protected Value createValue( int value )
        {
            final var type = random.among( ValueTypes.values() );
            return type.toValue( value );
        }
    }

    /**
     * Used to keep track of what entity ids we expect to find from different queries.
     * In "tracking" we keep track of all queries and all nodes.
     * In "included" we keep track of the queries we want to test. There will be a lot of
     * different exact queries so we randomly select a few of them to test.
     */
    protected static class TrackEntityIdsMatchingScanQuery
    {
        // range for range based queries, other value type ranges are calculated from this for consistency
        // as using an int as source of values, ~half of ints will be covered by this range
        private static final Pair<Integer,Integer> RANGE = Pair.of( Integer.MIN_VALUE / 2, Integer.MAX_VALUE / 2 );
        private final EntityIdsMatchingScanQuery<PropertyKeyScanQuery> tracking = new PartitionedScanTestSuite.EntityIdsMatchingScanQuery<>();
        private final EntityIdsMatchingScanQuery<PropertyKeyScanQuery> included = new PartitionedScanTestSuite.EntityIdsMatchingScanQuery<>();

        final PartitionedScanTestSuite.EntityIdsMatchingScanQuery<PropertyKeyScanQuery> get()
        {
            return included;
        }

        protected void generateAndTrack( long nodeId, String indexName, int propKeyId, Value value, boolean includeExactQueries )
        {
            // always have a property exist query
            include( add( nodeId, indexName, PropertyIndexQuery.exists( propKeyId ) ) );

            // sometimes have an exact query, as it's likely to only match one thing
            // track regardless, as there is a chance a previous Label/PropertyKey/Value would match (likely with boolean)
            final var exactQuery = add( nodeId, indexName, PropertyIndexQuery.exact( propKeyId, value ) );
            if ( includeExactQueries )
            {
                include( exactQuery );
            }

            // less trivial queries
            Stream.concat(
                    // ranges
                    Arrays.stream( ValueTypes.values() )
                            // note: geometric range predicates don't currently work
                            .filter( type -> type != ValueTypes.POINT )
                            .map( type -> Pair.of( type.toValue( RANGE.first() ), type.toValue( RANGE.other() ) ) )
                            .flatMap( range -> Stream.of(
                                    PropertyIndexQuery.range( propKeyId, range.first(), false, range.other(), false ),
                                    PropertyIndexQuery.range( propKeyId, range.first(), false, range.other(), true ),
                                    PropertyIndexQuery.range( propKeyId, range.first(), true, range.other(), false ),
                                    PropertyIndexQuery.range( propKeyId, range.first(), true, range.other(), false ) ) ),
                    // text queries
                    Stream.of( PropertyIndexQuery.stringPrefix( propKeyId, Values.utf8Value( "1" ) ),
                            PropertyIndexQuery.stringPrefix( propKeyId, Values.utf8Value( "999" ) ),
                            PropertyIndexQuery.stringSuffix( propKeyId, Values.utf8Value( "1" ) ),
                            PropertyIndexQuery.stringSuffix( propKeyId, Values.utf8Value( "999" ) ),
                            PropertyIndexQuery.stringContains( propKeyId, Values.utf8Value( "1" ) ),
                            PropertyIndexQuery.stringContains( propKeyId, Values.utf8Value( "999" ) ) ) )
                    // if value would match query, ensure the query is tracked
                    .filter( query -> query.acceptsValue( value ) )
                    .forEach( query -> include( add( nodeId, indexName, query ) ) );
        }

        protected void generateAndTrack( long nodeId, String indexName, int[] propKeyIds, Value[] values, boolean includeExactQueries )
        {
            if ( Arrays.stream( values ).allMatch( Objects::nonNull ) )
            {
                include( add( nodeId, indexName, Arrays.stream( propKeyIds ).mapToObj( PropertyIndexQuery::exists ).toArray( PropertyIndexQuery[]::new ) ) );

                final var exactQuery = add( nodeId, indexName,
                                            IntStream.range( 0, propKeyIds.length )
                                                     .mapToObj( i -> PropertyIndexQuery.exact( propKeyIds[i], values[i] ) )
                                                     .toArray( PropertyIndexQuery[]::new ) );
                if ( includeExactQueries )
                {
                    include( exactQuery );
                }
            }
        }

        private PropertyKeyScanQuery add( long nodeId, String indexName, PropertyIndexQuery... queries )
        {
            final var scanQuery = new PropertyKeyScanQuery( indexName, queries );
            tracking.getOrCreate( scanQuery ).add( nodeId );
            return scanQuery;
        }

        private void include( PropertyKeyScanQuery scanQuery )
        {
            included.addOrReplace( scanQuery, tracking.getOrCreate( scanQuery ) );
        }
    }

    protected static class PropertyKeyScanQuery implements ScanQuery<PropertyIndexQuery[]>
    {
        private final String indexName;
        private final PropertyIndexQuery[] queries;

        PropertyKeyScanQuery( String indexName, PropertyIndexQuery... queries )
        {
            this.indexName = indexName;
            this.queries = queries;
        }

        @Override
        public final String indexName()
        {
            return indexName;
        }

        @Override
        public final PropertyIndexQuery[] get()
        {
            return queries;
        }

        public final PropertyIndexQuery get( int i )
        {
            return queries[i];
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj )
            {
                return true;
            }
            if ( obj == null || getClass() != obj.getClass() )
            {
                return false;
            }
            final var that = (PropertyKeyScanQuery) obj;
            return Objects.equals( indexName, that.indexName ) && Arrays.equals( queries, that.queries );
        }

        @Override
        public int hashCode()
        {
            var result = Objects.hash( indexName );
            result = 31 * result + Arrays.hashCode( queries );
            return result;
        }

        @Override
        public String toString()
        {
            return String.format( "%s[index='%s', query='%s']",
                                  getClass().getSimpleName(), indexName,
                                  Arrays.stream( queries ).map( PropertyIndexQuery::toString ).collect( Collectors.joining( "," ) ) );
        }
    }

    private enum ValueTypes
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
