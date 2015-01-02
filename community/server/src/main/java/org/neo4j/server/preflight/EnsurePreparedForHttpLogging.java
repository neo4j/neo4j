/**
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
package org.neo4j.server.preflight;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.configuration.ServerSettings;

import org.apache.commons.io.FileUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class EnsurePreparedForHttpLogging implements PreflightTask
{
    private String failureMessage = "";
	private Config config;

    public EnsurePreparedForHttpLogging( Config config )
    {
    	this.config = config;
    }
    
    @Override
    public boolean run()
    {
        boolean enabled = config.get( ServerSettings.http_logging_enabled );
        
        if ( !enabled )
        {
            return true;
        }

        File logLocation = extractLogLocationFromConfig( config.get( ServerSettings.http_log_config_File ).getAbsolutePath() );

        if ( logLocation != null )
        {
            if ( validateFileBasedLoggingConfig( logLocation ) )
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        else
        {
            // File logging is not configured, no other logging can be easily checked here
            return true;
        }
    }


    private boolean validateFileBasedLoggingConfig( File logLocation )
    {

        try
        {
            FileUtils.forceMkdir( logLocation );
        }
        catch ( IOException e )
        {
            failureMessage = String.format( "HTTP log directory [%s] does not exist",
                logLocation.getAbsolutePath() );
            return false;
        }

        if ( !logLocation.exists() )
        {
            failureMessage = String.format( "HTTP log directory [%s] cannot be created",
                logLocation.getAbsolutePath() );
            return false;
        }

        if ( !logLocation.canWrite() )
        {
            failureMessage = String.format( "HTTP log directory [%s] is not writable",
                logLocation.getAbsolutePath() );
            return false;
        }

        return true;
    }

    private File extractLogLocationFromConfig( String configLocation )
    {
        DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        try
        {
            final File file = new File( configLocation );

            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document doc = docBuilder.parse( file );

            final Node node = doc.getElementsByTagName( "file" ).item( 0 );

            return new File( node.getTextContent() ).getParentFile();
        }
        catch ( Exception e )
        {
            return null;
        }
    }

    @Override
    public String getFailureMessage()
    {
        return failureMessage;
    }
}
