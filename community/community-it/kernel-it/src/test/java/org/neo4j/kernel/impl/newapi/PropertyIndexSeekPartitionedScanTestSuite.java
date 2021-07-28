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

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.newapi.PartitionedScanTestSuite.EntityIdsMatchingQuery;
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

        protected EntityIdsMatchingQuery<PropertyKeySeekQuery> emptyQueries( Pair<Integer,int[]> tokenAndPropKeyCombination )
        {
            final var empty = new EntityIdsMatchingQuery<PropertyKeySeekQuery>();
            try ( var tx = beginTx() )
            {
                final var tokenId = tokenAndPropKeyCombination.first();
                final var propKeyIds = tokenAndPropKeyCombination.other();
                for ( final var propKeyId : propKeyIds )
                {
                    final var index = factory.getIndex( tx, tokenId, propKeyId );
                    Arrays.stream( ValueType.values() )
                          .map( type -> new PropertyRecord( propKeyId, type.toValue( 0 ), type ) )
                          .map( PropertyIndexPartitionedScanTestSuite::queries )
                          .forEach( queries -> queries.filter( index.getCapability()::supportPartitionedScan )
                                                      .map( query -> new PropertyKeySeekQuery( index.getName(), query ) )
                                                      .forEach( empty::getOrCreate ) );
                }

                final var index = factory.getIndex( tx, tokenId, propKeyIds );
                queries( Arrays.stream( propKeyIds ).mapToObj( propKeyId ->
                    createRandomPropertyRecord( random, propKeyId, 0 ) ).toArray( PropertyRecord[]::new ) )
                        .filter( index.getCapability()::supportPartitionedScan )
                        .map( query -> new PropertyKeySeekQuery( index.getName(), query ) )
                        .forEach( empty::getOrCreate );
            }
            catch ( Exception e )
            {
                throw new AssertionError( "failed to create empty queries", e );
            }
            return empty;
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
    protected static class TrackEntityIdsMatchingQuery
    {
        private final EntityIdsMatchingQuery<PropertyKeySeekQuery> tracking = new EntityIdsMatchingQuery<>();
        private final EntityIdsMatchingQuery<PropertyKeySeekQuery> included = new EntityIdsMatchingQuery<>();

        final EntityIdsMatchingQuery<PropertyKeySeekQuery> get()
        {
            return included;
        }

        protected void generateAndTrack( long nodeId, boolean includeExactQueries, IndexDescriptor index, PropertyRecord... props )
        {
            queries( props ).filter( index.getCapability()::supportPartitionedScan )
                            .forEach( rawQuery ->
                            {
                                final var query = add( nodeId, index.getName(), rawQuery );
                                if ( Arrays.stream( rawQuery ).noneMatch( PropertyIndexQuery.ExactPredicate.class::isInstance ) || includeExactQueries )
                                {
                                    include( query );
                                }
                            } );
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
