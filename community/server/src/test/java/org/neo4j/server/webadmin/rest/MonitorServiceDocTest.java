/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.webadmin.rest;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.RrdDbWrapper;
import org.neo4j.server.database.WrappedDatabase;
import org.neo4j.server.rest.management.MonitorService;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.server.rrd.JobScheduler;
import org.neo4j.server.rrd.RrdFactory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.server.EntityOutputFormat;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MonitorServiceDocTest implements JobScheduler
{
    private RrdDbWrapper rrdDb;
    private MonitorService monitorService;
    private Database database;
    private EntityOutputFormat output;

    @Test
    public void correctRepresentation() throws Exception
    {
        Response resp = monitorService.getServiceDefinition();

        assertEquals( 200, resp.getStatus() );

        Map<String, Object> resultAsMap = output.getResultAsMap();
        @SuppressWarnings( "unchecked" ) Map<String, Object> resources = (Map<String, Object>) resultAsMap.get( "resources" );
        assertThat( (String) resources.get( "data_from" ), containsString( "/fetch/{start}" ) );
        assertThat( (String) resources.get( "data_period" ), containsString( "/fetch/{start}/{stop}" ) );
        String latest_data = (String) resources.get( "latest_data" );
        assertThat( latest_data, containsString( "/fetch" ) );
    }

    @Test
    public void canFetchData() throws URISyntaxException, UnsupportedEncodingException
    {
        UriInfo mockUri = mock( UriInfo.class );
        URI uri = new URI( "http://peteriscool.com:6666/" );
        when( mockUri.getBaseUri() ).thenReturn( uri );
        Response resp = monitorService.getData();

        String entity = new String( (byte[]) resp.getEntity(), "UTF-8" );

        assertEquals( entity, 200, resp.getStatus() );
        assertThat( entity, containsString( "timestamps" ) );
        assertThat( entity, containsString( "end_time" ) );
        assertThat( entity, containsString( "property_count" ) );
    }

    @Before
    public void setUp() throws Exception
    {
        database = new WrappedDatabase( (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase() );

        rrdDb = new RrdFactory( new Config(), NullLogProvider.getInstance() ).createRrdDbAndSampler( database, this );

        output = new EntityOutputFormat( new JsonFormat(), URI.create( "http://peteriscool.com:6666/" ), null );
        monitorService = new MonitorService( rrdDb.get(), output );
    }

    @After
    public void shutdownDatabase() throws Throwable
    {
        try
        {
            rrdDb.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        this.database.shutdown();
    }

    @Override
	public void scheduleAtFixedRate( Runnable job, String jobName, long delay, long period )
    {
    }
}
