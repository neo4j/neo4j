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

import org.neo4j.bolt.v1.runtime.Session;
import org.neo4j.bolt.v1.runtime.Sessions;
import org.neo4j.bolt.v1.runtime.internal.StandardSessions;
import org.neo4j.bolt.v1.runtime.internal.concurrent.ThreadedSessions;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.udc.UsageData;

class TestSessions implements TestRule, Sessions
{
    private GraphDatabaseFacade gdb;
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
                gdb = (GraphDatabaseFacade) new TestGraphDatabaseFactory().newImpermanentDatabase( config );
                Neo4jJobScheduler scheduler = life.add( new Neo4jJobScheduler() );
                DependencyResolver resolver = gdb.getDependencyResolver();
                StandardSessions sessions = life.add(
                        new StandardSessions( gdb, new UsageData( scheduler ), NullLogService.getInstance(),
                                resolver.resolveDependency( ThreadToStatementContextBridge.class ))
                );
                actual = new ThreadedSessions(
                        sessions,
                        scheduler, NullLogService.getInstance() );

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

    TestSessions withAuthEnabled( boolean authEnabled )
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

    long lastClosedTxId()
    {
        return gdb.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();
    }
}
