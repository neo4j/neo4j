/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.InOrder;

import java.io.File;
import java.time.Clock;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLog;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.rule.NeoStoreDataSourceRule;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;

import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.api.security.SecurityContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.KernelTransaction.Type.explicit;

public class KernelTest
{
    private final PageCacheAndDependenciesRule pageCacheRule = new PageCacheAndDependenciesRule();
    private final NeoStoreDataSourceRule neoStoreRule = new NeoStoreDataSourceRule();

    @Rule
    public final RuleChain rules = RuleChain.outerRule( pageCacheRule ).around( neoStoreRule );

    @Test
    public void shouldNotAllowCreationOfConstraintsWhenInHA() throws Exception
    {
        //noinspection deprecation
        GraphDatabaseAPI db = new FakeHaDatabase();
        ThreadToStatementContextBridge stmtBridge =
                db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );

        try ( Transaction ignored = db.beginTx() )
        {
            Statement statement = stmtBridge.get();

            try
            {
                statement.schemaWriteOperations().uniquePropertyConstraintCreate( forLabel( 1, 1 ) );
                fail( "expected exception here" );
            }
            catch ( InvalidTransactionTypeKernelException e )
            {
                assertThat( e.getMessage(), containsString( "HA" ) );
            }
        }

        db.shutdown();
    }

    @Test
    public void shouldIncrementTransactionMonitorBeforeCheckingDatabaseAvailability() throws Exception
    {
        // GIVEN
        AvailabilityGuard availabilityGuard = spy( new AvailabilityGuard( Clock.systemUTC(), NullLog.getInstance() ) );
        TransactionMonitor transactionMonitor = mock( TransactionMonitor.class );
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies( availabilityGuard, transactionMonitor );
        NeoStoreDataSource dataSource = neoStoreRule.getDataSource( pageCacheRule.directory().absolutePath(),
                pageCacheRule.fileSystem(), pageCacheRule.pageCache(), dependencies );
        dataSource.start();
        KernelAPI kernel = dataSource.getKernel();

        // WHEN
        try ( KernelTransaction tx = kernel.newTransaction( explicit, AUTH_DISABLED ) )
        {
            // THEN
            InOrder order = inOrder( transactionMonitor, availabilityGuard );
            order.verify( transactionMonitor, times( 1 ) ).transactionStarted();
            order.verify( availabilityGuard, times( 1 ) ).await( anyLong() );
        }
    }

    @SuppressWarnings( "deprecation" )
    class FakeHaDatabase extends ImpermanentGraphDatabase
    {
        @Override
        protected void create( File storeDir, Map<String, String> params, GraphDatabaseFacadeFactory.Dependencies dependencies )
        {
            Function<PlatformModule,EditionModule> factory =
                    ( platformModule ) -> new CommunityEditionModule( platformModule )
                    {
                        @Override
                        protected SchemaWriteGuard createSchemaWriteGuard()
                        {
                            return () ->
                            {
                                throw new InvalidTransactionTypeKernelException(
                                        "Creation or deletion of constraints is not possible while running in a HA cluster. " +
                                                "In order to do that, please restart in non-HA mode and propagate the database copy" +
                                                "to all slaves" );
                            };
                        }
                    };
            new GraphDatabaseFacadeFactory( DatabaseInfo.COMMUNITY,  factory )
            {
                @Override
                protected PlatformModule createPlatform( File storeDir, Config config,
                        GraphDatabaseFacadeFactory.Dependencies dependencies, GraphDatabaseFacade graphDatabaseFacade )
                {
                    return new ImpermanentPlatformModule( storeDir, config, databaseInfo, dependencies,
                            graphDatabaseFacade );
                }
            }.initFacade( storeDir, params, dependencies, this );
        }
    }
}
