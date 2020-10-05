/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.dbms.database.DbmsRuntimeSystemGraphComponent;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestDirectoryExtension
@DbmsExtension()
class DbmsRuntimeSystemGraphComponentTest
{

    // The trick here is that we pass a normal user database
    // to the tested system graph component, which will treat it as a system database
    @Inject
    private GraphDatabaseService userDatabase;
    private GraphDatabaseService fakeSystemDb;

    private final SystemGraphComponents systemGraphComponents = new SystemGraphComponents();
    private DbmsRuntimeSystemGraphComponent dbmsRuntimeSystemGraphComponent;

    @BeforeEach
    void beforeEach()
    {
        fakeSystemDb = new FakeSystemDb( userDatabase );
        initDbmsComponent( false );
    }

    @Test
    void testInitialisationOnFreshDatabase()
    {
        systemGraphComponents.initializeSystemGraph( fakeSystemDb );
        assertDbmsRuntimeNode( DbmsRuntimeRepository.LATEST_VERSION.getVersionNumber() );

        assertStatus( SystemGraphComponent.Status.CURRENT );
    }

    @Test
    void testInitialisationOnExistingDatabase()
    {
        userDatabase.executeTransactionally( "CREATE (:Version)" );

        systemGraphComponents.initializeSystemGraph( fakeSystemDb );
        assertDbmsRuntimeNode( DbmsRuntimeVersion.V4_1.getVersionNumber() );

        assertStatus( SystemGraphComponent.Status.REQUIRES_UPGRADE );
    }

    @Test
    void testInitialisationOnExistingDatabaseWithAutomaticUpgrade()
    {
        systemGraphComponents.deregister( DbmsRuntimeSystemGraphComponent.COMPONENT_NAME );
        initDbmsComponent( true );

        userDatabase.executeTransactionally( "CREATE (:Version)" );

        systemGraphComponents.initializeSystemGraph( fakeSystemDb );
        assertDbmsRuntimeNode( DbmsRuntimeRepository.LATEST_VERSION.getVersionNumber() );

        assertStatus( SystemGraphComponent.Status.CURRENT );
    }

    @Test
    void testCurrentVersionPresent()
    {
        createDbmsRuntimeNode( DbmsRuntimeRepository.LATEST_VERSION );

        systemGraphComponents.initializeSystemGraph( fakeSystemDb );
        assertDbmsRuntimeNode( DbmsRuntimeRepository.LATEST_VERSION.getVersionNumber() );

        assertStatus( SystemGraphComponent.Status.CURRENT );
    }

    @Test
    void testOldVersionPresent()
    {
        createDbmsRuntimeNode( DbmsRuntimeVersion.V4_1 );

        systemGraphComponents.initializeSystemGraph( fakeSystemDb );
        assertDbmsRuntimeNode( DbmsRuntimeVersion.V4_1.getVersionNumber() );

        assertStatus( SystemGraphComponent.Status.REQUIRES_UPGRADE );
    }

    @Test
    void testUpgrade() throws Exception
    {
        createDbmsRuntimeNode( DbmsRuntimeVersion.V4_1 );

        assertStatus( SystemGraphComponent.Status.REQUIRES_UPGRADE );

        systemGraphComponents.upgradeToCurrent( fakeSystemDb );

        assertDbmsRuntimeNode( DbmsRuntimeRepository.LATEST_VERSION.getVersionNumber() );
        assertStatus( SystemGraphComponent.Status.CURRENT );
    }

    @Test
    void upgradeFromUninitialized() throws Exception
    {
        assertStatus( SystemGraphComponent.Status.UNINITIALIZED );

        systemGraphComponents.upgradeToCurrent( fakeSystemDb );

        assertDbmsRuntimeNode( DbmsRuntimeRepository.LATEST_VERSION.getVersionNumber() );
        assertStatus( SystemGraphComponent.Status.CURRENT );
    }

    private void initDbmsComponent( boolean automaticUpgrade )
    {
        var config = Config.newBuilder()
                           .set( GraphDatabaseSettings.allow_single_automatic_upgrade, automaticUpgrade )
                           .build();
        dbmsRuntimeSystemGraphComponent = new DbmsRuntimeSystemGraphComponent( config );
        systemGraphComponents.register( dbmsRuntimeSystemGraphComponent );
    }

    private void assertDbmsRuntimeNode( int expectedVersion )
    {
        int foundVersion = userDatabase.executeTransactionally( "MATCH (n:DbmsRuntime) RETURN n.version AS version", Map.of(), result ->
        {
            ResourceIterator<Integer> ri = result.columnAs( "version" );
            return ri.stream().findFirst().get();
        } );

        assertEquals( expectedVersion, foundVersion );
    }

    private void assertStatus( SystemGraphComponent.Status expectedStatus )
    {
        try ( var tx = userDatabase.beginTx() )
        {
            assertEquals( expectedStatus, dbmsRuntimeSystemGraphComponent.detect( tx ) );
        }
    }

    private void createDbmsRuntimeNode( DbmsRuntimeVersion version )
    {
        try ( var tx = userDatabase.beginTx() )
        {
            tx.createNode( Label.label( "DbmsRuntime" ) ).setProperty( "version", version.getVersionNumber() );
            tx.commit();
        }
    }

    private static class FakeSystemDb implements GraphDatabaseService
    {

        private final GraphDatabaseService wrappedDb;

        FakeSystemDb( GraphDatabaseService wrappedDb )
        {
            this.wrappedDb = wrappedDb;
        }

        @Override
        public boolean isAvailable( long timeout )
        {
            return wrappedDb.isAvailable( timeout );
        }

        @Override
        public Transaction beginTx()
        {
            return wrappedDb.beginTx();
        }

        @Override
        public Transaction beginTx( long timeout, TimeUnit unit )
        {
            return wrappedDb.beginTx( timeout, unit );
        }

        @Override
        public void executeTransactionally( String query ) throws QueryExecutionException
        {
            throw new IllegalStateException( "Not implemented" );
        }

        @Override
        public void executeTransactionally( String query, Map<String,Object> parameters ) throws QueryExecutionException
        {
            throw new IllegalStateException( "Not implemented" );
        }

        @Override
        public <T> T executeTransactionally( String query, Map<String,Object> parameters, ResultTransformer<T> resultTransformer )
                throws QueryExecutionException
        {
            throw new IllegalStateException( "Not implemented" );
        }

        @Override
        public <T> T executeTransactionally( String query, Map<String,Object> parameters, ResultTransformer<T> resultTransformer, Duration timeout )
                throws QueryExecutionException
        {
            throw new IllegalStateException( "Not implemented" );
        }

        @Override
        public String databaseName()
        {
            return "system";
        }
    }
}
