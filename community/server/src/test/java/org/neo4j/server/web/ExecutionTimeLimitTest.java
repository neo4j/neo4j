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
package org.neo4j.server.web;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.UniformInterfaceException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;
import org.neo4j.test.ImpermanentGraphDatabase;

@Ignore("This feature has to be done some other way")
public class ExecutionTimeLimitTest
{
// ------------------------------ FIELDS ------------------------------

    private WrappingNeoServerBootstrapper testBootstrapper;
    private InternalAbstractGraphDatabase db;
    private long wait;

// -------------------------- OTHER METHODS --------------------------

    @Test(expected = UniformInterfaceException.class)
    public void expectLimitation()
    {
        wait = 2000;
        Client.create().resource( "http://localhost:7476/db/data/node/0" )
                .header( "accept", "application/json" )
                .get( String.class );
    }

    @Test(expected = UniformInterfaceException.class)
    public void expectLimitationByHeader()
    {
        wait = 200;
        Client.create().resource( "http://localhost:7476/db/data/node/0" )
                .header( "accept", "application/json" )
                .header( "max-execution-time", "100" )
                .get( String.class );
    }

    @Test
    public void expectNoLimitation()
    {
        wait = 200;
        Client.create().resource( "http://localhost:7476/db/data/node/0" )
                .header( "accept", "application/json" )
                .get( String.class );
    }

    @Before
    @SuppressWarnings("deprecation")
    public void setUp() throws Exception
    {
        db = new ImpermanentGraphDatabase()
        {
            @Override
            public Node getNodeById( long id )
            {
                try
                {
                    Thread.sleep( wait );
                } catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
                return super.getNodeById( id );
            }
        };

        ServerConfigurator config = new ServerConfigurator( db );
        config.configuration().setProperty( Configurator.WEBSERVER_PORT_PROPERTY_KEY, "7476" );
        config.configuration().setProperty( Configurator.WEBSERVER_LIMIT_EXECUTION_TIME_PROPERTY_KEY, "1000s" );
        testBootstrapper = new WrappingNeoServerBootstrapper( db, config );
        testBootstrapper.start();
    }

    @After
    public void tearDown()
    {
        testBootstrapper.stop();
        db.shutdown();
    }
}
