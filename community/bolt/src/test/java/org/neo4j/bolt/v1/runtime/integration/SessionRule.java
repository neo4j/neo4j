/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.security.auth.BasicAuthentication;
import org.neo4j.bolt.v1.runtime.Session;
import org.neo4j.bolt.v1.runtime.Sessions;
import org.neo4j.bolt.v1.runtime.internal.StandardSessions;
import org.neo4j.bolt.v1.runtime.internal.concurrent.ThreadedSessions;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.bolt.SessionTracker;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.udc.UsageData;

class SessionRule implements TestRule, Sessions
{
    private GraphDatabaseAPI gdb;
    private Sessions actual;
    private LinkedList<Session> startedSessions = new LinkedList<>();
    private final LifeSupport life = new LifeSupport();
    private boolean authEnabled = false;

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
                Neo4jJobScheduler scheduler = life.add( new Neo4jJobScheduler() );
                DependencyResolver resolver = gdb.getDependencyResolver();
                LogService logService = NullLogService.getInstance();

                Authentication authentication = authentication( resolver.resolveDependency( Config.class ),
                        resolver.resolveDependency( AuthManager.class ), logService );
                StandardSessions sessions = life.add(
                        new StandardSessions( gdb, new UsageData( scheduler ), logService,
                                resolver.resolveDependency( ThreadToStatementContextBridge.class ),
                                authentication, resolver.resolveDependency( NeoStoreDataSource.class ),
                                SessionTracker.NOOP )
                );
                actual = new ThreadedSessions( sessions, scheduler, logService );
                life.start();
                try
                {
                    base.evaluate();
                }
                finally
                {
                    try
                    {
                        startedSessions.forEach( Session::close );
                    }
                    catch ( Throwable e ) { e.printStackTrace(); }

                    gdb.shutdown();
                }
            }
        };
    }

    private Authentication authentication( Config config, AuthManager authManager, LogService logService )
    {

        if ( config.get( GraphDatabaseSettings.auth_enabled ) )
        {
            return new BasicAuthentication( authManager, logService.getInternalLogProvider() );
        }
        else
        {
            return Authentication.NONE;
        }
    }

    @Override
    public Session newSession( String connectionDescriptor, boolean isEncrypted )
    {
        if ( actual == null )
        {
            throw new IllegalStateException( "Cannot access test environment before test is running." );
        }
        Session session = actual.newSession( connectionDescriptor, isEncrypted );
        startedSessions.add( session );
        return session;
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
