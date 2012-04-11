package org.neo4j.server.webadmin.logging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.helpers.ServerBuilder;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.test.server.ExclusiveServerTestBase;

public class HTTPLoggingFunctionalTest extends ExclusiveServerTestBase
{
    private NeoServer server;
    private static final String logDirectory = "target/test-data/impermanent-db/log";

    @Before
    public void cleanUp() throws IOException
    {
        ServerHelper.cleanTheDatabase( server );
        removeHttpLogs();
    }

    private void removeHttpLogs() throws IOException
    {
        File logDir = new File( logDirectory );
        if ( logDir.exists() )
        {
            FileUtils.deleteDirectory( logDir );
        }
    }

    @After
    public void stopServer()
    {
        if ( server != null )
        {
            server.stop();
        }
    }

    @Test
    public void givenExplicitlyDisabledServerLoggingConfigurationShouldNotLogAccesses() throws Exception
    {
        // given
        server = ServerBuilder.server().withDefaultDatabaseTuning()
            .withProperty( Configurator.HTTP_LOGGING, "false" )
            .withProperty( Configurator.HTTP_LOG_CONFIG_LOCATION,
                getClass().getResource( "/neo4j-server-test-logback.xml" ).getFile() )
            .build();
        server.start();
        FunctionalTestHelper functionalTestHelper = new FunctionalTestHelper( server );

        // when
        String query = "?implicitlyDisabled" + UUID.randomUUID().toString();
        JaxRsResponse response = new RestRequest().get( functionalTestHelper.webAdminUri() + query );
        assertEquals( 200, response.getStatus() );
        response.close();

        // then
        assertFalse( occursIn( query, new File( logDirectory + File.separator + "http.log" ) ) );
    }

    @Test
    public void givenExplicitlyEnabledServerLoggingConfigurationShouldLogAccess() throws Exception
    {
        // given
        server = ServerBuilder.server().withDefaultDatabaseTuning()
            .withProperty( Configurator.HTTP_LOGGING, "true" )
            .withProperty( Configurator.HTTP_LOG_LOCATION, logDirectory )
            .withProperty( Configurator.HTTP_LOG_CONFIG_LOCATION,
                getClass().getResource( "/neo4j-server-test-logback.xml" ).getFile() )
            .build();
        server.start();

        FunctionalTestHelper functionalTestHelper = new FunctionalTestHelper( server );

        // when
        String query = "?explicitlyEnabled=" + UUID.randomUUID().toString();
        JaxRsResponse response = new RestRequest().get( functionalTestHelper.webAdminUri() + query );
        assertEquals( 200, response.getStatus() );
        response.close();

        // then
        assertTrue( occursIn( query, new File( logDirectory + File.separator + "http.log" ) ) );
    }

    private boolean occursIn( String lookFor, File file ) throws FileNotFoundException
    {
        if ( !file.exists() )
        {
            return false;
        }

        boolean result = false;
        Scanner scanner = new Scanner( file );
        while ( scanner.hasNext() )
        {
            if ( scanner.next().contains( lookFor ) )
            {
                result = true;
            }
        }

        scanner.close();

        return result;
    }


//    private String today()
//    {
//        int year = Calendar.getInstance().get( Calendar.YEAR );
//        int month = Calendar.getInstance().get( Calendar.MONTH );
//        int day = Calendar.getInstance().get( Calendar.DAY_OF_MONTH );
//
//        return String.valueOf( year ) + ensureDoubleDigitString( month ) + ensureDoubleDigitString( day );
//    }
//
//    private String ensureDoubleDigitString( int number )
//    {
//        if ( number > 31 )
//        {
//            throw new RuntimeException( "No day or month can be greater than 31" );
//        }
//
//        String result = "";
//        if ( number < 10 )
//        {
//            result = "0" + String.valueOf( number );
//        }
//        else
//        {
//            result = String.valueOf( number );
//        }
//
//        return result;
//    }
}
