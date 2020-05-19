/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.test.fabric;

import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseManagementServiceImpl;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.fabric.bolt.BoltFabricDatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.TestDatabaseManagementServiceFactory;
import org.neo4j.time.SystemNanoClock;

public class TestFabricDatabaseManagementServiceFactory extends TestDatabaseManagementServiceFactory
{
    private final Config config;

    public TestFabricDatabaseManagementServiceFactory( DbmsInfo dbmsInfo,
                                                       Function<GlobalModule,AbstractEditionModule> editionFactory,
                                                       boolean impermanent,
                                                       FileSystemAbstraction fileSystem,
                                                       SystemNanoClock clock,
                                                       LogProvider internalLogProvider,
                                                       Config config )
    {
        super( dbmsInfo, editionFactory, impermanent, fileSystem, clock, internalLogProvider );

        this.config = config;
    }

    @Override
    protected DatabaseManagementService createManagementService( GlobalModule globalModule, LifeSupport globalLife, Log internalLog,
                                                                 DatabaseManager<?> databaseManager )
    {
        return new DatabaseManagementServiceImpl( databaseManager, globalModule.getGlobalAvailabilityGuard(),
                                                  globalLife, globalModule.getDatabaseEventListeners(), globalModule.getTransactionEventListeners(),
                                                  internalLog )
        {
            @Override
            public GraphDatabaseService database( String name ) throws DatabaseNotFoundException
            {
                BoltFabricDatabaseManagementService fabricBoltDbms =
                        globalModule.getGlobalDependencies().resolveDependency( BoltFabricDatabaseManagementService.class );

                var baseDb = databaseManager.getDatabaseContext( name )
                                            .orElseThrow( () -> new DatabaseNotFoundException( name ) ).databaseFacade();
                // Bolt API behaves a little differently than the embedded one.
                // The embedded API expects a lookup of a database representation to succeed even if the database
                // is not available. GraphDatabaseService#isAvailable will return false in such case.
                // On the other hand, Bolt API throws UnavailableException when an unavailable
                // database is being looked up.
                // Therefore the lookup of Bolt API representation of a database has to be done lazily.
                return new TestFabricGraphDatabaseService( baseDb, config, () ->
                {
                    try
                    {
                        return fabricBoltDbms.database( name );
                    }
                    catch ( UnavailableException e )
                    {
                        throw new RuntimeException( e );
                    }
                } );
            }
        };
    }
}
