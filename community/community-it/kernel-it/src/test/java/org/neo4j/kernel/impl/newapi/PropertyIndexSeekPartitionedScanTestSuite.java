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
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.newapi.PartitionedScanTestSuite.EntityIdsMatchingQuery;
import org.neo4j.kernel.impl.newapi.PartitionedScanTestSuite.Queries;
import org.neo4j.kernel.impl.newapi.PartitionedScanTestSuite.Query;
import org.neo4j.kernel.impl.newapi.PropertyIndexSeekPartitionedScanTestSuite.PropertyKeySeekQuery;

abstract class PropertyIndexSeekPartitionedScanTestSuite<CURSOR extends Cursor>
        extends PropertyIndexPartitionedScanTestSuite<PropertyKeySeekQuery,CURSOR>
{
    PropertyIndexSeekPartitionedScanTestSuite( IndexType index )
    {
        super( index );
    }

    abstract static class WithoutData<CURSOR extends Cursor>
            extends PropertyIndexPartitionedScanTestSuite.WithoutData<PropertyKeySeekQuery,CURSOR>
    {
        WithoutData( PropertyIndexSeekPartitionedScanTestSuite<CURSOR> testSuite )
        {
            super( testSuite );
        }

        protected Queries<PropertyKeySeekQuery> emptyQueries( Pair<Integer,int[]> tokenAndPropKeyCombination )
        {
            try ( var tx = beginTx() )
            {
                final var tokenId = tokenAndPropKeyCombination.first();
                final var propKeyIds = tokenAndPropKeyCombination.other();
                final var validQueries = Stream.concat(
                        // single queries
                        Arrays.stream( propKeyIds ).boxed()
                              .flatMap( propKeyId -> Arrays.stream( ValueType.values() )
                                      .map( type -> new PropertyRecord( propKeyId, type.toValue( 0 ), type ) ) )
                              .flatMap( prop -> queries( prop )
                                      .map( query -> new PropertyKeySeekQuery( factory.getIndexName( tokenId, prop.id ), query ) ) ),

                        // composite queries
                        queries( Arrays.stream( propKeyIds )
                                       .mapToObj( propKeyId -> createRandomPropertyRecord( random, propKeyId, 0 ) )
                                       .toArray( PropertyRecord[]::new ) )
                                .map( query -> new PropertyKeySeekQuery( factory.getIndexName( tokenId, propKeyIds ), query ) ) )

                                               .collect( Collectors.partitioningBy( query -> factory.getIndex( tx, query.indexName() )
                                                                                                    .getCapability()
                                                                                                    .supportPartitionedScan( query.get() ) ) );

                return new Queries<>( validQueries.get( true ).stream().collect( EntityIdsMatchingQuery.collector() ),
                                      validQueries.get( false ).stream().collect( Collectors.toUnmodifiableSet() ) );
            }
            catch ( Exception e )
            {
                throw new AssertionError( "failed to create empty queries", e );
            }
        }
    }

    abstract static class WithData<CURSOR extends Cursor>
            extends PropertyIndexPartitionedScanTestSuite.WithData<PropertyKeySeekQuery,CURSOR>
    {
        protected double ratioForExactQuery;

        WithData( PropertyIndexSeekPartitionedScanTestSuite<CURSOR> testSuite )
        {
            super( testSuite );
        }

        protected boolean shouldIncludeExactQuery()
        {
            return random.nextDouble() < ratioForExactQuery;
        }
    }

    /**
     * Used to keep track of what entity ids we expect to find from different queries.
     * In "tracking" we keep track of all queries and all nodes.
     * In "included" we keep track of the queries we want to test. There will be a lot of
     * different exact queries so we randomly select a few of them to test.
     */
    protected static final class TrackEntityIdsMatchingQuery
    {
        private final EntityIdsMatchingQuery<PropertyKeySeekQuery> tracking = new EntityIdsMatchingQuery<>();
        private final EntityIdsMatchingQuery<PropertyKeySeekQuery> included = new EntityIdsMatchingQuery<>();
        private final Set<PropertyKeySeekQuery> invalid = new HashSet<>();

        Queries<PropertyKeySeekQuery> get()
        {
            return new Queries<>( included, Collections.unmodifiableSet( invalid ) );
        }

        void generateAndTrack( long nodeId, boolean includeExactQueries, IndexDescriptor index, PropertyRecord... props )
        {
            final var validQueries = queries( props )
                    .map( queries -> new PropertyKeySeekQuery( index.getName(), queries ) )
                    .collect( Collectors.partitioningBy( query -> index.getCapability().supportPartitionedScan( query.get() ) ) );

            validQueries.get( true ).stream()
                        .map( query -> add( nodeId, query ) )
                        .filter( query -> Arrays.stream( query.get() ).noneMatch( PropertyIndexQuery.ExactPredicate.class::isInstance )
                                          || includeExactQueries )
                        .forEach( this::include );

            invalid.addAll( validQueries.get( false ) );
        }

        private PropertyKeySeekQuery add( long nodeId, PropertyKeySeekQuery query )
        {
            tracking.getOrCreate( query ).add( nodeId );
            return query;
        }

        private void include( PropertyKeySeekQuery propertyKeySeekQuery )
        {
            included.addOrReplace( propertyKeySeekQuery, tracking.getOrCreate( propertyKeySeekQuery ) );
        }
    }

    protected static final class PropertyKeySeekQuery implements Query<PropertyIndexQuery[]>
    {
        private final String indexName;
        private final PropertyIndexQuery[] queries;

        PropertyKeySeekQuery( String indexName, PropertyIndexQuery... queries )
        {
            this.indexName = indexName;
            this.queries = queries;
        }

        @Override
        public String indexName()
        {
            return indexName;
        }

        @Override
        public PropertyIndexQuery[] get()
        {
            return queries;
        }

        public PropertyIndexQuery get( int i )
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
