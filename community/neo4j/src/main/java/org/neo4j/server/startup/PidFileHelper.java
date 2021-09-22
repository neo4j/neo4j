/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.server.startup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

public class PidFileHelper
{
    public static Long readPid( Path pidFile ) throws IOException
    {
        if ( Files.exists( pidFile ) )
        {
            try
            {
                return Long.parseLong( Files.readString( pidFile ).trim() );
            }
            catch ( NumberFormatException e )
            {
                remove( pidFile );
                return null;
            }
        }
        return null;
    }

    public static void storePid( Path pidFile, long pid ) throws IOException
    {
        Files.createDirectories( pidFile.getParent() );
        Files.write( pidFile, Long.toString( pid ).getBytes(), CREATE, WRITE, TRUNCATE_EXISTING );
    }

    public static void remove( Path pidFile )
    {
        try
        {
            Files.deleteIfExists( pidFile );
        }
        catch ( IOException ignored )
        {
        }
    }
}
