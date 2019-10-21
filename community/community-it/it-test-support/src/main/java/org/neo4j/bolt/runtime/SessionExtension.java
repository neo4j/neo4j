/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.runtime;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.impl.BoltKernelDatabaseManagementServiceProvider;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.runtime.statemachine.impl.BoltStateMachineFactoryImpl;
import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.security.auth.BasicAuthentication;
import org.neo4j.bolt.txtracking.DefaultReconciledTransactionTracker;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

import static java.time.Duration.ofSeconds;

public class SessionExtension implements BeforeEachCallback, AfterEachCallback
{
    private final Supplier<TestDatabaseManagementServiceBuilder> builderFactory;
    private GraphDatabaseAPI gdb;
    private BoltStateMachineFactoryImpl boltFactory;
    private List<BoltStateMachine> runningMachines = new ArrayList<>();
    private DatabaseManagementService managementService;
    private boolean authEnabled;

    public SessionExtension()
    {
        this( TestDatabaseManagementServiceBuilder::new );
    }

    public SessionExtension( Supplier<TestDatabaseManagementServiceBuilder> builderFactory )
    {
        this.builderFactory = builderFactory;
    }

    public BoltStateMachine newMachine( long version, BoltChannel boltChannel )
    {
        assertTestStarted();
        BoltStateMachine machine = boltFactory.newStateMachine( version, boltChannel );
        runningMachines.add( machine );
        return machine;
    }

    public DatabaseManagementService managementService()
    {
        assertTestStarted();
        return managementService;
    }

    public String defaultDatabaseName()
    {
        assertTestStarted();
        DependencyResolver resolver = gdb.getDependencyResolver();
        Config config = resolver.resolveDependency( Config.class );
        return config.get( GraphDatabaseSettings.default_database );
    }

    public DatabaseIdRepository databaseIdRepository()
    {
        assertTestStarted();
        var resolver = gdb.getDependencyResolver();
        var databaseManager = resolver.resolveDependency( DatabaseManager.class );
        return databaseManager.databaseIdRepository();
    }

    @Override
    public void beforeEach( ExtensionContext extensionContext )
    {
        managementService = builderFactory.get().impermanent().setConfig( GraphDatabaseSettings.auth_enabled, authEnabled ).build();
        gdb = (GraphDatabaseAPI) managementService.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        DependencyResolver resolver = gdb.getDependencyResolver();
        Authentication authentication = authentication( resolver.resolveDependency( AuthManager.class ) );
        Config config = resolver.resolveDependency( Config.class );
        SystemNanoClock clock = Clocks.nanoClock();
        var reconciledTxTracker = new DefaultReconciledTransactionTracker( NullLogService.getInstance() );
        BoltGraphDatabaseManagementServiceSPI databaseManagementService = new BoltKernelDatabaseManagementServiceProvider( managementService,
                reconciledTxTracker, new Monitors(), clock, ofSeconds( 30 ) );
        boltFactory = new BoltStateMachineFactoryImpl(
                databaseManagementService,
                authentication,
                clock,
                config,
                NullLogService.getInstance()
        );
    }

    @Override
    public void afterEach( ExtensionContext extensionContext )
    {
        try
        {
            if ( runningMachines != null )
            {
                IOUtils.closeAll( runningMachines );
            }
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
        }

        managementService.shutdown();
    }

    private void assertTestStarted()
    {
        if ( boltFactory == null || gdb == null )
        {
            throw new IllegalStateException( "Cannot access test environment before test is running." );
        }
    }

    private static Authentication authentication( AuthManager authManager )
    {
        return new BasicAuthentication( authManager );
    }

    public long lastClosedTxId()
    {
        return gdb.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();
    }

    public URL putTmpFile( String prefix, String suffix, String contents ) throws IOException
    {
        var tmpFile = File.createTempFile( prefix, suffix, null );
        tmpFile.deleteOnExit();
        try ( PrintWriter out = new PrintWriter( tmpFile ) )
        {
            out.println( contents);
        }
        return tmpFile.toURI().toURL();
    }

    public SessionExtension withAuthEnabled( boolean authEnabled )
    {
        this.authEnabled = authEnabled;
        return this;
    }
}
