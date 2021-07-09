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

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.kernel.impl.newapi.PartitionedScanTestSuite.EntityIdsMatchingQuery;
import org.neo4j.kernel.impl.newapi.PartitionedScanTestSuite.Query;
import org.neo4j.kernel.impl.newapi.PropertyIndexSeekPartitionedScanTestSuite.PropertyKeySeekQuery;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

abstract class PropertyIndexSeekPartitionedScanTestSuite<CURSOR extends Cursor>
        extends PropertyIndexPartitionedScanTestSuite<PropertyKeySeekQuery,CURSOR>
{
    abstract static class WithoutData<CURSOR extends Cursor>
            extends PropertyIndexPartitionedScanTestSuite.WithoutData<PropertyKeySeekQuery,CURSOR>
    {
        WithoutData( PropertyIndexSeekPartitionedScanTestSuite<CURSOR> testSuite )
        {
            super( testSuite );
        }

        protected EntityIdsMatchingQuery<PropertyKeySeekQuery> emptyQueries( Pair<Integer,int[]> tokenAndPropKeyCombination )
        {
            final var empty = new EntityIdsMatchingQuery<PropertyKeySeekQuery>();
            final var tokenId = tokenAndPropKeyCombination.first();
            final var propKeyIds = tokenAndPropKeyCombination.other();
            for ( final var propKeyId : propKeyIds )
            {
                empty.getOrCreate( new PropertyKeySeekQuery( testSuite.generateIndexName( tokenId, propKeyId ),
                                                             PropertyIndexQuery.exists( propKeyId ) ) );
            }
            empty.getOrCreate( new PropertyKeySeekQuery( testSuite.generateIndexName( tokenId, propKeyIds ),
                                                         Arrays.stream( propKeyIds )
                                                               .mapToObj( PropertyIndexQuery::exists )
                                                               .toArray( PropertyIndexQuery[]::new ) ) );
            return empty;
        }
    }

    abstract static class WithData<CURSOR extends Cursor>
            extends PropertyIndexPartitionedScanTestSuite.WithData<PropertyKeySeekQuery,CURSOR>
    {
        WithData( PropertyIndexSeekPartitionedScanTestSuite<CURSOR> testSuite )
        {
            super( testSuite );
        }
    }

    /**
     * Used to keep track of what entity ids we expect to find from different queries.
     * In "tracking" we keep track of all queries and all nodes.
     * In "included" we keep track of the queries we want to test. There will be a lot of
     * different exact queries so we randomly select a few of them to test.
     */
    protected static class TrackEntityIdsMatchingQuery
    {
        // range for range based queries, other value type ranges are calculated from this for consistency
        // as using an int as source of values, ~half of ints will be covered by this range
        private static final Pair<Integer,Integer> RANGE = Pair.of( Integer.MIN_VALUE / 2, Integer.MAX_VALUE / 2 );
        private final EntityIdsMatchingQuery<PropertyKeySeekQuery> tracking = new EntityIdsMatchingQuery<>();
        private final EntityIdsMatchingQuery<PropertyKeySeekQuery> included = new EntityIdsMatchingQuery<>();

        final EntityIdsMatchingQuery<PropertyKeySeekQuery> get()
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

        private PropertyKeySeekQuery add( long nodeId, String indexName, PropertyIndexQuery... queries )
        {
            final var propertyKeySeekQuery = new PropertyKeySeekQuery( indexName, queries );
            tracking.getOrCreate( propertyKeySeekQuery ).add( nodeId );
            return propertyKeySeekQuery;
        }

        private void include( PropertyKeySeekQuery propertyKeySeekQuery )
        {
            included.addOrReplace( propertyKeySeekQuery, tracking.getOrCreate( propertyKeySeekQuery ) );
        }
    }

    protected static class PropertyKeySeekQuery implements Query<PropertyIndexQuery[]>
    {
        private final String indexName;
        private final PropertyIndexQuery[] queries;

        PropertyKeySeekQuery( String indexName, PropertyIndexQuery... queries )
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
            final var that = (PropertyKeySeekQuery) obj;
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
}
