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
import java.time.Clock;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.security.auth.BasicAuthentication;
import org.neo4j.bolt.v1.runtime.BoltFactoryImpl;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.udc.UsageData;

class SessionRule implements TestRule
{
    private GraphDatabaseAPI gdb;
    private BoltFactoryImpl boltFactory;
    private LinkedList<BoltStateMachine> runningMachines = new LinkedList<>();
    private boolean authEnabled;

    @Override
    public Statement apply( final Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                Map<Setting<?>,String> config = new HashMap<>();
                config.put( GraphDatabaseSettings.auth_enabled, Boolean.toString( authEnabled ) );
                gdb = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase( config );
                DependencyResolver resolver = gdb.getDependencyResolver();
                Authentication authentication = authentication( resolver.resolveDependency( AuthManager.class ),
                        resolver.resolveDependency( UserManagerSupplier.class ) );
                boltFactory = new BoltFactoryImpl(
                                        gdb,
                                        new UsageData( null ),
                                        NullLogService.getInstance(),
                                        resolver.resolveDependency( ThreadToStatementContextBridge.class ),
                                        authentication,
                                        BoltConnectionTracker.NOOP,
                                        Config.defaults()
                                    );
                boltFactory.start();
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

                    gdb.shutdown();
                }
            }
        };
    }

    private Authentication authentication( AuthManager authManager, UserManagerSupplier userManagerSupplier )
    {
        return new BasicAuthentication( authManager, userManagerSupplier );
    }

    BoltStateMachine newMachine( BoltChannel boltChannel )
    {
        return newMachine( boltChannel, Clock.systemUTC() );
    }

    BoltStateMachine newMachine( BoltChannel boltChannel, Clock clock )
    {
        if ( boltFactory == null )
        {
            throw new IllegalStateException( "Cannot access test environment before test is running." );
        }
        BoltStateMachine connection = boltFactory.newMachine( boltChannel, clock );
        runningMachines.add( connection );
        return connection;
    }

    SessionRule withAuthEnabled( boolean authEnabled )
    {
        this.authEnabled = authEnabled;
        return this;
    }

    URL putTmpFile( String prefix, String suffix, String contents ) throws IOException
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

    long lastClosedTxId()
    {
        return gdb.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();
    }

}
