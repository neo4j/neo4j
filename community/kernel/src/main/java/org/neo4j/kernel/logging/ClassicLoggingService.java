/*
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
package org.neo4j.kernel.logging;

import java.io.File;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.neo4j.kernel.impl.util.StringLogger.DEFAULT_THRESHOLD_FOR_ROTATION;

/**
 * Implements the old-style logging with just one logger regardless of name.
 */
public class ClassicLoggingService extends SingleLoggingService
{
    public ClassicLoggingService( Config config )
    {
        this( config, false );
    }

    public ClassicLoggingService( Config config, boolean debugEnabled )
    {
        super( StringLogger.logger( new DefaultFileSystemAbstraction(), logFile( config ),
                DEFAULT_THRESHOLD_FOR_ROTATION, debugEnabled ) );
    }

    private static File logFile( Config config )
    {
        final File location = config.get( GraphDatabaseSettings.internal_log_location );
        if (location != null)
        {
            return location;
        }
        return new File( config.get( InternalAbstractGraphDatabase.Configuration.store_dir ), StringLogger.DEFAULT_NAME );
    }
}
