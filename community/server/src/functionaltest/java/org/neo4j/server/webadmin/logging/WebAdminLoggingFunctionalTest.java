package org.neo4j.server.webadmin.logging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.NeoServerBootstrapper;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.server.ExclusiveServerTestBase;

public class WebAdminLoggingFunctionalTest extends AbstractRestFunctionalTestBase
{
    private FunctionalTestHelper functionalTestHelper;

    @Before
    public void cleanTheDatabase()
    {
        cleanDatabase();
        functionalTestHelper = new FunctionalTestHelper(server());
    }

    @Test
    public void givenNoServerLoggingConfigurationShouldLogAllAccessesToWebadmin() throws Exception
    {
        // given

        GraphDatabaseAPI database = functionalTestHelper.getDatabase();
        ServerConfigurator conf = new ServerConfigurator( database );
        conf.configuration().addProperty( Configurator.WEBADMIN_LOGGING_ENABLED, "enabled" );

        // when
        JaxRsResponse response = new RestRequest().get( functionalTestHelper.webAdminUri() );
        assertEquals( 200, response.getStatus() );
        response.close();

        // then
        File webadminHttpLogs = new File(
            new File( database.getStoreDir() + File.separator + ".." + File.separator + "log" ).getCanonicalPath() + File.separator + "access.log" );

        System.out.println("==> " + webadminHttpLogs.getCanonicalPath());

        assertTrue( webadminHttpLogs.exists() );

    }

    private String today()
    {
        int year = Calendar.getInstance().get( Calendar.YEAR );
        int month = Calendar.getInstance().get( Calendar.MONTH );
        int day = Calendar.getInstance().get( Calendar.DAY_OF_MONTH );

        return String.valueOf( year ) + ensureDoubleDigitString( month ) + ensureDoubleDigitString( day );
    }

    private String ensureDoubleDigitString( int number )
    {
        if ( number > 31 )
        {
            throw new RuntimeException( "No day or month can be greater than 31" );
        }

        String result = "";
        if ( number < 10 )
        {
            result = "0" + String.valueOf( number );
        }
        else
        {
            result = String.valueOf( number );
        }

        return result;
    }
}
