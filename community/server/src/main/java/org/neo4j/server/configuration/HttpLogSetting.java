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
package org.neo4j.server.configuration;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.File;
import java.io.IOException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.neo4j.helpers.Function;
import org.neo4j.io.fs.FileUtils;

/**
 * Validates a config setting for the location of a HTTP log config file. It ensures that, if there is a log directory specified, that directory
 * exists and is writable.
 */
public class HttpLogSetting implements Function<String, File>
{
    @Override
    public File apply( String setting )
    {
        File file = new File( FileUtils.fixSeparatorsInPath( setting ) );

        File logTarget = extractLogLocationFromConfig( file );
        if(logTarget != null)
        {
            // User has specified an output file to log to, ensure it exists and is writable
            validateFileBasedLoggingConfig( logTarget );
        }

        return file;
    }

    private void validateFileBasedLoggingConfig( File logLocation )
    {
        try
        {
            org.apache.commons.io.FileUtils.forceMkdir( logLocation );
        }
        catch ( IOException e )
        {
            throw new IllegalArgumentException( String.format( "HTTP log directory [%s] does not exist or is not a directory.",
                    logLocation.getAbsolutePath() ));
        }

        if ( !logLocation.exists() )
        {
            throw new IllegalArgumentException( String.format( "HTTP log directory [%s] could not be created.",
                    logLocation.getAbsolutePath() ));
        }

        if ( !logLocation.canWrite() )
        {
            throw new IllegalArgumentException( String.format( "HTTP log directory [%s] is not writeable.",
                    logLocation.getAbsolutePath() ));
        }
    }

    private File extractLogLocationFromConfig( File configLocation )
    {
        DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        try
        {
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document doc = docBuilder.parse( configLocation );

            final Node node = doc.getElementsByTagName( "file" ).item( 0 );

            return new File( node.getTextContent() ).getParentFile();
        }
        catch ( Exception e )
        {
            return null;
        }
    }

    @Override
    public String toString()
    {
        return "a logback config file";
    }
}
