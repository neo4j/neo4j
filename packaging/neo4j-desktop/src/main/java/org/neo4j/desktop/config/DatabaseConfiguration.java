/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.desktop.config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.neo4j.desktop.ui.DesktopModel;

public class DatabaseConfiguration
{
    public static void copyDefaultDatabaseConfigurationProperties( File file ) throws IOException
    {
        InputStream input = null;
        FileOutputStream output = null;
        try
        {
            input = getDefaultDatabaseConfigurationContent();
            if ( input == null )
            {
                // Default configuration could not be found. This can safely be ignored because
                // the default configuration contains only comments.
                return;
            }
            output = new FileOutputStream( file );
            int bufferSize = 4096;
            byte[] buffer = new byte[bufferSize];
            int bytesRead;
            while ( (bytesRead = input.read( buffer )) != -1 )
            {
                output.write( buffer, 0, bytesRead );
            }
        }
        catch ( IOException e )
        {
            // Because the message from this cause may not mention which file it's about
            throw new IOException( String.format( "Could not default configuration to '%s'", file ), e );
        }
        finally
        {
            if ( input != null )
                input.close();
            if ( output != null )
                output.close();
        }
    }

    public static InputStream getDefaultDatabaseConfigurationContent()
    {
        return DesktopModel.class.getResourceAsStream( "/org/neo4j/server/config/community/neo4j-default.properties" );
    }
}
