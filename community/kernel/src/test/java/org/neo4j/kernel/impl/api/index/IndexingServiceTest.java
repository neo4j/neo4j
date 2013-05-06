/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import org.junit.Test;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.xa.DefaultSchemaIndexProviderMap;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.TestLogger;
import org.neo4j.kernel.logging.Logging;

import static java.util.Arrays.asList;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.info;

public class IndexingServiceTest
{
    @Test
    public void shouldLogIndexStateOnInit() throws Exception
    {
        // given
        TestLogger logger = new TestLogger();
        SchemaIndexProvider provider = mock( SchemaIndexProvider.class );
        when( provider.getProviderDescriptor() ).thenReturn( PROVIDER_DESCRIPTOR );
        SchemaIndexProviderMap providerMap = new DefaultSchemaIndexProviderMap( provider );
        IndexingService indexingService = new IndexingService(
                mock( JobScheduler.class ),
                providerMap,
                mock( IndexStoreView.class ),
                mock( UpdateableSchemaState.class ),
                mockLogging( logger ) );

        IndexRule onlineIndex = new IndexRule( 1, 1, PROVIDER_DESCRIPTOR, 1 );
        IndexRule populatingIndex = new IndexRule( 2, 2, PROVIDER_DESCRIPTOR, 2 );
        IndexRule failedIndex = new IndexRule( 3, 3, PROVIDER_DESCRIPTOR, 3 );

        when( provider.getInitialState( onlineIndex.getId() ) ).thenReturn( InternalIndexState.ONLINE );
        when( provider.getInitialState( populatingIndex.getId() ) ).thenReturn( InternalIndexState.POPULATING );
        when( provider.getInitialState( failedIndex.getId() ) ).thenReturn( InternalIndexState.FAILED );

        // when
        indexingService.initIndexes( asList( onlineIndex, populatingIndex, failedIndex ).iterator() );

        // then
        logger.assertExactly(
                info( "IndexingService.initIndexes: IndexDescriptor[label:1, property:1] is ONLINE" ),
                info( "IndexingService.initIndexes: IndexDescriptor[label:2, property:2] is POPULATING" ),
                info( "IndexingService.initIndexes: IndexDescriptor[label:3, property:3] is FAILED" )
        );
    }

    @Test
    public void shouldLogIndexStateOnStart() throws Exception
    {
        // given
        TestLogger logger = new TestLogger();
        SchemaIndexProvider provider = mock( SchemaIndexProvider.class );
        when( provider.getProviderDescriptor() ).thenReturn( PROVIDER_DESCRIPTOR );
        SchemaIndexProviderMap providerMap = new DefaultSchemaIndexProviderMap( provider );
        IndexingService indexingService = new IndexingService(
                mock( JobScheduler.class ),
                providerMap,
                mock( IndexStoreView.class ),
                mock( UpdateableSchemaState.class ),
                mockLogging( logger ) );

        IndexRule onlineIndex = new IndexRule( 1, 1, PROVIDER_DESCRIPTOR, 1 );
        IndexRule populatingIndex = new IndexRule( 2, 2, PROVIDER_DESCRIPTOR, 2 );
        IndexRule failedIndex = new IndexRule( 3, 3, PROVIDER_DESCRIPTOR,  3 );

        when( provider.getInitialState( onlineIndex.getId() ) ).thenReturn( InternalIndexState.ONLINE );
        when( provider.getInitialState( populatingIndex.getId() ) ).thenReturn( InternalIndexState.POPULATING );
        when( provider.getInitialState( failedIndex.getId() ) ).thenReturn( InternalIndexState.FAILED );
        indexingService.initIndexes( asList( onlineIndex, populatingIndex, failedIndex ).iterator() );

        logger.clear();

        // when
        indexingService.start();

        // then
        logger.assertAtLeastOnce( info( "IndexingService.start: IndexDescriptor[label:1, property:1] is ONLINE" ) );
        logger.assertAtLeastOnce( info( "IndexingService.start: IndexDescriptor[label:2, property:2] is POPULATING" ) );
        logger.assertAtLeastOnce( info( "IndexingService.start: IndexDescriptor[label:3, property:3] is FAILED" ) );
    }

    private static Logging mockLogging( TestLogger logger )
    {
        Logging logging = mock( Logging.class );
        when( logging.getMessagesLog( any( Class.class ) ) ).thenReturn( logger );
        return logging;
    }
}
