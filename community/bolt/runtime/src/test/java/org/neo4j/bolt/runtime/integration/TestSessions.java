/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.bolt.runtime.integration;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.LinkedList;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.bolt.runtime.Session;
import org.neo4j.bolt.runtime.Sessions;
import org.neo4j.bolt.runtime.internal.StandardSessions;
import org.neo4j.bolt.runtime.internal.concurrent.ThreadedSessions;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.udc.UsageData;

public class TestSessions implements TestRule, Sessions
{
    private GraphDatabaseService gdb;
    private Sessions actual;
    private LinkedList<Session> startedSessions = new LinkedList<>();
    private final LifeSupport life = new LifeSupport();

    @Override
    public Statement apply( final Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                gdb = new TestGraphDatabaseFactory().newImpermanentDatabase();
                Neo4jJobScheduler scheduler = life.add( new Neo4jJobScheduler() );
                StandardSessions sessions = life.add(
                        new StandardSessions( (GraphDatabaseAPI) gdb, new UsageData(), NullLogService.getInstance() ) );
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
                        for ( Session session : startedSessions )
                        {
                            session.close();
                        }
                    }
                    catch ( Throwable e ) { e.printStackTrace(); }

                    gdb.shutdown();
                }
            }
        };
    }

    @Override
    public Session newSession()
    {
        if ( actual == null )
        {
            throw new IllegalStateException( "Cannot access test environment before test is running." );
        }
        Session session = actual.newSession();
        startedSessions.add( session );
        return session;
    }
}
