/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.guard.GuardTimeoutException;
import org.neo4j.kernel.impl.factory.CommunityFacadeFactory;
import org.neo4j.kernel.impl.factory.DataSourceModule;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactoryState;
import org.neo4j.time.FakeClock;

import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TransactionGuardIntegrationTest
{
    @Rule
    public CleanupRule cleanupRule = new CleanupRule();

    @Test
    public void terminateLongRunningTransaction()
    {
        FakeClock clock = new FakeClock();
        Map<Setting<?>,String> configMap = MapUtil.genericMap(
                GraphDatabaseSettings.execution_guard_enabled, Settings.TRUE,
                GraphDatabaseSettings.transaction_timeout, "2s",
                GraphDatabaseSettings.statement_timeout, "100s" );
        GraphDatabaseAPI database = startCustomDatabase( clock, configMap );
        try ( Transaction transaction = database.beginTx() )
        {
            clock.forward( 3, TimeUnit.SECONDS );
            transaction.success();
            database.createNode();
            fail( "Transaction should be already terminated." );
        }
        catch ( GuardTimeoutException e )
        {
            assertThat( e.getMessage(), startsWith( "Transaction timeout." ) );
        }

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningQueryTransaction()
    {
        FakeClock clock = new FakeClock();
        Map<Setting<?>,String> configMap = MapUtil.genericMap(
                GraphDatabaseSettings.execution_guard_enabled, Settings.TRUE,
                GraphDatabaseSettings.transaction_timeout, "2s",
                GraphDatabaseSettings.statement_timeout, "100s" );
        GraphDatabaseAPI database = startCustomDatabase( clock, configMap );
        try ( Transaction transaction = database.beginTx() )
        {
            clock.forward( 3, TimeUnit.SECONDS );
            transaction.success();
            database.execute( "create (n)" );
            fail( "Transaction should be already terminated." );
        }
        catch ( GuardTimeoutException e )
        {
            assertThat( e.getMessage(), startsWith( "Transaction timeout." ) );
        }

        assertDatabaseDoesNotHaveNodes( database );
    }

    private void assertDatabaseDoesNotHaveNodes( GraphDatabaseAPI database )
    {
        try ( Transaction ignored = database.beginTx() )
        {
            assertEquals( 0, database.getAllNodes().stream().count() );
        }
    }

    private GraphDatabaseAPI startCustomDatabase( Clock clock, Map<Setting<?>,String> configMap )
    {
        GuardCommunityFacadeFactory guardCommunityFacadeFactory = new GuardCommunityFacadeFactory( clock );
        GraphDatabaseAPI database =
                (GraphDatabaseAPI) new GuardTestGraphDatabaseFactory( guardCommunityFacadeFactory )
                        .newImpermanentDatabase( configMap );
        cleanupRule.add( database );
        return database;
    }

    private class GuardTestGraphDatabaseFactory extends TestGraphDatabaseFactory
    {

        private CommunityFacadeFactory customFacadeFactory;

        GuardTestGraphDatabaseFactory( CommunityFacadeFactory customFacadeFactory )
        {
            this.customFacadeFactory = customFacadeFactory;
        }

        @Override
        protected GraphDatabaseBuilder.DatabaseCreator createImpermanentDatabaseCreator( File storeDir,
                TestGraphDatabaseFactoryState state )
        {
            return config -> customFacadeFactory.newFacade( storeDir, config,
                    GraphDatabaseDependencies.newDependencies( state.databaseDependencies() ) );
        }
    }

    private class GuardCommunityFacadeFactory extends CommunityFacadeFactory
    {

        private Clock clock;

        GuardCommunityFacadeFactory( Clock clock )
        {
            this.clock = clock;
        }

        @Override
        protected DataSourceModule createDataSource( Dependencies dependencies,
                PlatformModule platformModule, EditionModule editionModule )
        {
            return new GuardDataSourceModule( dependencies, platformModule, editionModule, clock );
        }

        private class GuardDataSourceModule extends DataSourceModule
        {

            GuardDataSourceModule( GraphDatabaseFacadeFactory.Dependencies dependencies,
                    PlatformModule platformModule, EditionModule editionModule, Clock clock )
            {
                super( dependencies, platformModule, editionModule );
            }

            @Override
            protected Clock getClock()
            {
                return clock;
            }
        }
    }
}
