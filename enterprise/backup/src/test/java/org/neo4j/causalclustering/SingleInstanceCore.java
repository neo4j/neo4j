/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering;

import java.io.File;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupServer;
import org.neo4j.causalclustering.core.EnterpriseCoreEditionModule;
import org.neo4j.causalclustering.core.NoConsensusEnterpriseCoreEditionModule;
import org.neo4j.causalclustering.core.server.CoreServerModule;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.NoOpDiscoveryService;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Connector;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.rule.TestDirectory;

/**
 * This is a rule used in tests for situations where you need to interact with a cluster core (over the transaction protocol),
 * but don't need cluster functionality (core vs read replica, catchup etc... not relevant to test)
 */
public class SingleInstanceCore extends GraphDatabaseFacade implements ExternalResourceIface
{
    private LazySingletonSupplier<TestDirectory> getTestDirectory;

    public SingleInstanceCore( Supplier<TestDirectory> testDirectory )
    {
        getTestDirectory = new LazySingletonSupplier<>( testDirectory );
    }

    private final LifeSupport lifeSupport = new LifeSupport();

    @Override
    public void before() throws Throwable
    {
    }

    @Override
    public void after( boolean successful ) throws Throwable
    {
        lifeSupport.stop();
    }

    private CatchupServer createCatchupServer( Config config )
    {
        File storeDir = getTestDirectory.get().directory( "store-dir" );
        GraphDatabaseDependencies dependencies = GraphDatabaseDependencies.newDependencies();
        DatabaseInfo databaseInfo = DatabaseInfo.CORE;

        DiscoveryServiceFactory discoveryServiceFactory = new NoOpDiscoveryService();

        Function<PlatformModule,EditionModule> factory =
                platformModule -> NoConsensusEnterpriseCoreEditionModule.refactorThisHack( platformModule, discoveryServiceFactory );
        GraphDatabaseFacadeFactory graphDatabaseFacadeFactory = new GraphDatabaseFacadeFactory( DatabaseInfo.CORE, factory );
        GraphDatabaseFacade graphDatabaseFacade = graphDatabaseFacadeFactory.initFacade( storeDir, config, dependencies, this );
        PlatformModule platformModule = new PlatformModule( storeDir, config, databaseInfo, dependencies, graphDatabaseFacade );

        EnterpriseCoreEditionModule enterpriseCoreEditionModule =
                NoConsensusEnterpriseCoreEditionModule.refactorThisHack( platformModule, discoveryServiceFactory );
        CoreServerModule coreServerModule = enterpriseCoreEditionModule.getCoreServerModule();
        return coreServerModule.getCatchupServer();
    }

    /**
     * By providing the config publicly
     *
     * @return
     */
    public Config createCoreConfig( int transactionPort )
    {
        Config config = Config.defaults();
        boltConnector( config, transactionPort );
        return config;
    }

    private void boltConnector( Config config, int port )
    {
        BoltConnector boltConnector = new BoltConnector( "bolts" );
        config.augment( boltConnector.type, Connector.ConnectorType.BOLT.name() );
        config.augment( boltConnector.enabled, Boolean.TRUE.toString() );
        config.augment( boltConnector.listen_address, ":" + port );
        config.augment( boltConnector.advertised_address, "0.0.0.0:" + port );
    }

    /**
     * Call this method to start a simple cluster core
     */
    public void start( Config config )
    {
        try
        {
            lifeSupport.add( createCatchupServer( config ) );
            lifeSupport.start();
        }
        catch ( Throwable throwable )
        {
            throw new RuntimeException( throwable );
        }
    }
}
