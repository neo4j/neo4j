/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.bolt.logging;

import java.io.File;
import java.util.concurrent.Executor;

import io.netty.channel.Channel;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;

public class BoltMessageLogging
{
    private final BoltMessageLog boltMessageLog;

    private BoltMessageLogging( BoltMessageLog boltMessageLog )
    {
        this.boltMessageLog = boltMessageLog;
    }

    public static BoltMessageLogging create( FileSystemAbstraction fs, JobScheduler scheduler, Config config, Log log )
    {
        return new BoltMessageLogging( createBoltMessageLog( fs, scheduler, config, log ) );
    }

    public static BoltMessageLogging none()
    {
        return new BoltMessageLogging( null );
    }

    public BoltMessageLogger newLogger( Channel channel )
    {
        return boltMessageLog == null ? NullBoltMessageLogger.getInstance()
                : new BoltMessageLoggerImpl( boltMessageLog, channel );
    }

    private static BoltMessageLog createBoltMessageLog( FileSystemAbstraction fs, JobScheduler scheduler,
            Config config, Log log )
    {
        if ( config.get( GraphDatabaseSettings.bolt_logging_enabled ) )
        {
            try
            {
                File boltLogFile = config.get( GraphDatabaseSettings.bolt_log_filename );
                Executor executor = scheduler.executor( JobScheduler.Groups.boltLogRotation );
                return new BoltMessageLog( fs, boltLogFile, executor );
            }
            catch ( Throwable t )
            {
                log.warn( "Unable to create bolt message log. It is thus disabled", t );
            }
        }
        return null;
    }
}
