package org.neo4j.server.startup.healthcheck;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.neo4j.server.configuration.Configurator;

public class HTTPLoggingPreparednessRule implements StartupHealthCheckRule
{
    private String failureMessage = "";

    @Override
    public boolean execute( Properties properties )
    {
        boolean enabled = new Boolean(
            String.valueOf( properties.getProperty( Configurator.HTTP_LOGGING ) ) ).booleanValue();

        if ( !enabled )
        {
            return true;
        }

        File logLocation = new File( String.valueOf( properties.getProperty( Configurator.HTTP_LOG_LOCATION ) ) );

        boolean logLocationSuitable = true;

        try
        {
            FileUtils.forceMkdir(logLocation);
        }
        catch ( IOException e )
        {
            logLocationSuitable = false;
        }

        if ( !logLocation.exists() )
        {
            failureMessage = String.format( "HTTP log directory [%s] cannot be created",
                logLocation.getAbsolutePath() );
            return false;
        }

        if ( !logLocationSuitable )
        {
            failureMessage = String.format( "HTTP log directory [%s] does not exist", logLocation.getAbsolutePath() );
            return false;
        }
        else
        {
            logLocationSuitable = logLocation.canWrite();
        }

        if ( !logLocationSuitable )
        {
            failureMessage = String.format( "HTTP log directory [%s] is not writable", logLocation.getAbsolutePath() );
            return false;
        }

        return true;
    }

    @Override
    public String getFailureMessage()
    {
        return failureMessage;
    }
}
