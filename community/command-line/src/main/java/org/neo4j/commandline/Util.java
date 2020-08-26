/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.commandline;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;
import org.neo4j.logging.log4j.Neo4jLoggerContext;

import static java.lang.String.format;
import static org.neo4j.io.fs.FileUtils.getCanonicalFile;

public final class Util
{
    private Util()
    {
    }

    public static boolean isSameOrChildFile( Path parent, Path candidate )
    {
        Path canonicalCandidate = getCanonicalFile( candidate );
        Path canonicalParentPath = getCanonicalFile( parent );
        return canonicalCandidate.startsWith( canonicalParentPath );
    }

    public static void wrapIOException( IOException e ) throws CommandFailedException
    {
        throw new CommandFailedException(
                format( "Unable to load database: %s: %s", e.getClass().getSimpleName(), e.getMessage() ), e );
    }

    public static Log4jLogProvider configuredLogProvider( Config config, OutputStream out )
    {
        Neo4jLoggerContext context = LogConfig.createBuilder( out, config.get( GraphDatabaseSettings.store_internal_log_level ) )
                .withTimezone( config.get( GraphDatabaseSettings.db_timezone ) ).build();
        return new Log4jLogProvider( context );
    }
}
