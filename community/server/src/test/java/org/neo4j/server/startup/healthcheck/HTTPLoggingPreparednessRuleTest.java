package org.neo4j.server.startup.healthcheck;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.matchers.JUnitMatchers.containsString;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.neo4j.server.configuration.Configurator;

public class HTTPLoggingPreparednessRuleTest
{
    @Test
    public void shouldPassWhenExplicitlyDisabled()
    {
        // given
        HTTPLoggingPreparednessRule rule = new HTTPLoggingPreparednessRule();
        final Properties properties = new Properties();
        properties.put( Configurator.HTTP_LOGGING, "false" );

        // when
        boolean result = rule.execute( properties );

        // then
        assertTrue( result );
        assertEquals( StringUtils.EMPTY, rule.getFailureMessage() );
    }

    @Test
    public void shouldPassWhenImplicitlyDisabled()
    {
        // given
        HTTPLoggingPreparednessRule rule = new HTTPLoggingPreparednessRule();
        final Properties properties = new Properties();

        // when
        boolean result = rule.execute( properties );

        // then
        assertTrue( result );
        assertEquals( StringUtils.EMPTY, rule.getFailureMessage() );
    }

    @Test
    public void shouldPassWhenEnabledWithGoodLoggingLocation() throws Exception
    {
        // given
        HTTPLoggingPreparednessRule rule = new HTTPLoggingPreparednessRule();
        final Properties properties = new Properties();
        properties.put( Configurator.HTTP_LOGGING, "true" );
        properties.put( Configurator.HTTP_LOG_LOCATION, temporaryDirectory().getAbsolutePath() );

        // when
        boolean result = rule.execute( properties );

        // then
        assertTrue( result );
        assertEquals( StringUtils.EMPTY, rule.getFailureMessage() );
    }


    @Test
    public void shouldFailWhenEnabledWithBadLoggingLocation() throws Exception
    {
        // given
        HTTPLoggingPreparednessRule rule = new HTTPLoggingPreparednessRule();
        final Properties properties = new Properties();
        properties.put( Configurator.HTTP_LOGGING, "true" );
        properties.put( Configurator.HTTP_LOG_LOCATION, badDirectory().getAbsolutePath() );

        // when
        boolean result = rule.execute( properties );

        // then
        assertFalse( result );
        assertThat( rule.getFailureMessage(), containsString( "HTTP log directory [" ) );
        assertThat( rule.getFailureMessage(), containsString( "] cannot be created" ) );
    }

    private File badDirectory() throws Exception
    {
        File f = new File( "/does-not-exist" );

        if ( f.exists() || f.canWrite() )
        {
            throw new RuntimeException(
                String.format( "File [%s] should not exist or be writable for this test", f.getAbsolutePath() ) );
        }

        return f;
    }

    private File temporaryDirectory() throws IOException
    {
        final File temp;

        temp = File.createTempFile( "temp", "dir" );

        if ( !(temp.delete()) )
        {
            throw new IOException( "Could not delete temp file: " + temp.getAbsolutePath() );
        }

        if ( !(temp.mkdir()) )
        {
            throw new IOException( "Could not create temp directory: " + temp.getAbsolutePath() );
        }

        return temp;
    }
}
