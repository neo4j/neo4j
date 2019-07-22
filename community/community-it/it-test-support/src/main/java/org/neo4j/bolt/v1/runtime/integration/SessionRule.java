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
package org.neo4j.bolt.v1.runtime.integration;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.impl.BoltKernelDatabaseManagementServiceProvider;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.runtime.BoltStateMachineFactoryImpl;
import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.security.auth.BasicAuthentication;
import org.neo4j.bolt.txtracking.DefaultReconciledTransactionTracker;
import org.neo4j.bolt.v1.BoltProtocolV1;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

public class SessionRule implements TestRule
{
    private GraphDatabaseAPI gdb;
    private BoltStateMachineFactoryImpl boltFactory;
    private LinkedList<BoltStateMachine> runningMachines = new LinkedList<>();
    private boolean authEnabled;

    @Override
    public Statement apply( final Statement base, Description description )
    {
        return new Statement()
        {

            private DatabaseManagementService managementService;

            @Override
            public void evaluate() throws Throwable
            {
                Map<Setting<?>,String> configMap = new HashMap<>();
                managementService = new TestDatabaseManagementServiceBuilder()
                        .impermanent()
                        .setConfig( GraphDatabaseSettings.auth_enabled, authEnabled )
                        .build();
                gdb = (GraphDatabaseAPI) managementService.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
                DependencyResolver resolver = gdb.getDependencyResolver();
                Config config = gdb.getDependencyResolver().resolveDependency( Config.class );
                Authentication authentication = authentication( resolver.resolveDependency( AuthManager.class ),
                        resolver.resolveDependency( UserManagerSupplier.class ) );
                SystemNanoClock clock = Clocks.nanoClock();
                var reconciledTxTracker = new DefaultReconciledTransactionTracker( NullLogService.getInstance() );
                var boltDbManagementService = new BoltKernelDatabaseManagementServiceProvider( managementService, reconciledTxTracker, new Monitors(), clock );
                boltFactory = new BoltStateMachineFactoryImpl( boltDbManagementService,
                                        authentication,
                                        clock,
                                        config,
                                        NullLogService.getInstance()
                                    );
                try
                {
                    base.evaluate();
                }
                finally
                {
                    try
                    {
                        if ( runningMachines != null )
                        {
                            runningMachines.forEach( BoltStateMachine::close );
                        }
                    }
                    catch ( Throwable e )
                    {
                        e.printStackTrace();
                    }

                    managementService.shutdown();
                }
            }
        };
    }

    private static Authentication authentication( AuthManager authManager, UserManagerSupplier userManagerSupplier )
    {
        return new BasicAuthentication( authManager, userManagerSupplier );
    }

    BoltStateMachine newMachine( BoltChannel boltChannel )
    {
        return newMachine( BoltProtocolV1.VERSION, boltChannel );
    }

    public BoltStateMachine newMachine( long version, BoltChannel boltChannel )
    {
        if ( boltFactory == null )
        {
            throw new IllegalStateException( "Cannot access test environment before test is running." );
        }
        BoltStateMachine machine = boltFactory.newStateMachine( version, boltChannel );
        runningMachines.add( machine );
        return machine;
    }

    SessionRule withAuthEnabled( boolean authEnabled )
    {
        this.authEnabled = authEnabled;
        return this;
    }

    public URL putTmpFile( String prefix, String suffix, String contents ) throws IOException
    {
        File tmpFile = File.createTempFile( prefix, suffix, null );
        tmpFile.deleteOnExit();
        try ( PrintWriter out = new PrintWriter( tmpFile ) )
        {
            out.println( contents);
        }
        return tmpFile.toURI().toURL();
    }

    public GraphDatabaseAPI graph()
    {
        return gdb;
    }

    public long lastClosedTxId()
    {
        return gdb.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();
    }

}
