/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.qa.features;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.qa.features.support.FileHelper.copyFile;
import static org.neo4j.qa.features.support.ProcessHelper.exec;

public class StartAndStopFeatureTest
{
    @Before
    public void The_Neo4j_package_is_expanded_at_neo4j_home() throws Exception
    {
        String product = "neo4j-community";
        String version = System.getProperty( "project_version" );
        String platform = "unix";
        String archiveName = product + "-" + version + "-" + platform + ".tar.gz";
        File copiedArchive = new File( workingDirectory, archiveName );
        copyFile( new File( "../standalone/target/" + archiveName ), copiedArchive );
        exec( workingDirectory, "tar", "xzf", copiedArchive.getName() );
        neo4jHome = new File( workingDirectory, product + "-" + version );
    }

    @Test
    public void The_Neo4j_server_should_start_and_stop_using_a_command_line_script() throws Exception
    {
        When_I_start_Neo4j_Server();
        And_wait_for_Server_started_at( "http://localhost:7474" );
        Then_it_should_provide_the_Neo4j_REST_interface_at( "http://localhost:7474" );

        When_I_stop_Neo4j_Server();
        And_wait_for_Server_stopped_at( "http://localhost:7474" );
        Then_it_should_not_provide_the_Neo4j_REST_interface_at( "http://localhost:7474" );
    }

    private final File workingDirectory = new File( "target/test-data/" + getClass().getName() );
    private File neo4jHome;

    private void When_I_start_Neo4j_Server() throws Exception
    {
        exec( neo4jHome, "bin/neo4j", "start" );
    }

    private void When_I_stop_Neo4j_Server() throws Exception
    {
        exec( neo4jHome, "bin/neo4j", "stop" );
    }

    private void And_wait_for_Server_started_at( String uri ) throws IOException, InterruptedException
    {
        boolean success = false;
        long startTime = System.currentTimeMillis();
        while ( !success && System.currentTimeMillis() - startTime < 60000 )
        {
            DefaultHttpClient httpClient = new DefaultHttpClient();
            try
            {
                success = statusCode( uri, httpClient ) == 200;
            }
            catch ( ConnectException e )
            {
                System.out.println( "Connection refused, sleeping" );
            }
            finally
            {
                httpClient.getConnectionManager().shutdown();
            }
            Thread.sleep( 1000 );
        }
        assertTrue( "Timed out waiting for " + uri, success );
    }

    private void And_wait_for_Server_stopped_at( String uri ) throws IOException, InterruptedException
    {
        DefaultHttpClient httpClient = new DefaultHttpClient();
        boolean success = false;
        long startTime = System.currentTimeMillis();
        while ( !success && System.currentTimeMillis() - startTime < 6000 )
        {
            try
            {
                statusCode( uri, httpClient );
                System.out.println( "Connection still available, sleeping" );
            }
            catch ( ConnectException e )
            {
                success = true;
            }
            Thread.sleep( 1000 );
        }
        assertTrue( "Timed out waiting for " + uri, success );
    }

    private void Then_it_should_provide_the_Neo4j_REST_interface_at( String uri ) throws Exception
    {
        assertEquals( 200, statusCode( uri, new DefaultHttpClient() ) );
    }

    private void Then_it_should_not_provide_the_Neo4j_REST_interface_at( String uri ) throws Exception
    {
        try
        {
            statusCode( uri, new DefaultHttpClient() );
            fail( "Should refuse connections" );
        }
        catch ( ConnectException e )
        {
            // expected
        }
    }

    private int statusCode( String uri, DefaultHttpClient httpClient ) throws IOException
    {
        HttpResponse response = httpClient.execute( new HttpGet( uri ) );
        EntityUtils.toString( response.getEntity() );
        return response.getStatusLine().getStatusCode();
    }
}
