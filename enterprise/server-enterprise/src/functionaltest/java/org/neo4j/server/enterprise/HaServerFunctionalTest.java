/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

package org.neo4j.server.enterprise;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import java.net.URI;
import java.util.Map;
import javax.ws.rs.core.MediaType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.helpers.Pair;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;
import org.neo4j.test.server.ha.ServerCluster;

import static org.junit.Assert.*;

public class HaServerFunctionalTest
{
    private static final int[] ZOOKEEPER_PORTS = { 2181, 2182 };
    @SuppressWarnings( "unchecked" )
    private static final Pair<Integer/*ha port*/, Integer/*web port*/>[] SERVER_PORTS = new Pair[] {
            Pair.of( 6001, 7474 ), Pair.of( 6002, 7475 ) };
    private static final TargetDirectory dir = TargetDirectory.forTest( HaServerFunctionalTest.class );
    private static LocalhostZooKeeperCluster zooKeeper;
    public @Rule
    TestName testName = new TestName()
    {
        @Override
        public String getMethodName()
        {
            return HaServerFunctionalTest.class.getSimpleName() + "." + super.getMethodName();
        }
    };

    @BeforeClass
    public static void startZooKeeper()
    {
        if ( GraphDatabaseSetting.osIsWindows() ) return;
        zooKeeper = new LocalhostZooKeeperCluster( dir, ZOOKEEPER_PORTS );
    }

    @AfterClass
    public static void stopZooKeeper()
    {
        if ( zooKeeper != null ) zooKeeper.shutdown();
        zooKeeper = null;
        dir.cleanup();
    }

    private ServerCluster cluster;

    @After
    public void stopServer()
    {
        if ( cluster != null ) cluster.shutdown();
        cluster = null;
    }

    @Test
    public void canStartUpServerCluster() throws Exception
    {
        if ( GraphDatabaseSetting.osIsWindows() ) return;
        cluster = new ServerCluster( testName.getMethodName(), dir, zooKeeper, SERVER_PORTS );
    }

    @Test
    public void canWriteToOneServerInTheClusterAndReadFromAnother() throws Exception
    {
        if ( GraphDatabaseSetting.osIsWindows() ) return;
        cluster = new ServerCluster( testName.getMethodName(), dir, zooKeeper, SERVER_PORTS );
        URI base = cluster.getRandomServerUri();

        put( property( node( base, 0 ), "message" ), "hello world" );
        cluster.updateAll();
        base = cluster.getRandomServerUri( base );
        assertEquals( "hello world", get( property( node( base, 0 ), "message" ) ) );
    }

    @Test
    public void canWriteToOneServerInTheClusterThenReadFromAnotherAfterShuttingDownTheWriteServer() throws Exception
    {
        if ( GraphDatabaseSetting.osIsWindows() ) return;
        cluster = new ServerCluster( testName.getMethodName(), dir, zooKeeper, SERVER_PORTS );
        URI base = cluster.getRandomServerUri();

        put( property( node( base, 0 ), "message" ), "hello world" );
        cluster.updateAll();
        cluster.kill( base );
        base = cluster.getRandomServerUri();
        assertEquals( "hello world", get( property( node( base, 0 ), "message" ) ) );
    }

    private static URI node( URI base, int id )
    {
        return URI.create( base + "db/data/node/" + id );
    }

    private static URI property( URI entity, String key )
    {
        return URI.create( entity + "/properties/" + key );
    }

    private static Object get( URI property )
    {
        ClientResponse response = Client.create().resource( property ).accept( MediaType.APPLICATION_JSON_TYPE )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( ClientResponse.class );
        try
        {
            if ( 200 == response.getStatus() )
            {
                return JsonHelper.jsonToSingleValue( response.getEntity( String.class ) );
            }
            else
            {
                Map<String, Object> data = JsonHelper.jsonToMap( response.getEntity( String.class ) );
                throw new RuntimeException( data.get( "message" ).toString() );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private static void put( URI property, Object value )
    {
        Client.create().resource( property ).accept( MediaType.APPLICATION_JSON_TYPE )
                .type( MediaType.APPLICATION_JSON_TYPE ).entity( JsonHelper.createJsonFrom( value ) ).put();
    }
}
