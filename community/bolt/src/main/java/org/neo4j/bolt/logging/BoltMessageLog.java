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
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.RotatingFileOutputStreamSupplier;

public class BoltMessageLog extends LifecycleAdapter
{
    private static final long ROTATION_THRESHOLD_BYTES = ByteUnit.MebiByte.toBytes( 20 );
    private static final long ROTATION_DELAY_MS = TimeUnit.SECONDS.toMillis( 500 );
    private static final int MAX_ARCHIVES = 10;

    private final Log inner;

    public BoltMessageLog( FileSystemAbstraction fileSystem, File logFile, Executor executor ) throws IOException
    {
        RotatingFileOutputStreamSupplier outputStreamSupplier = new RotatingFileOutputStreamSupplier( fileSystem,
                logFile, ROTATION_THRESHOLD_BYTES, ROTATION_DELAY_MS, MAX_ARCHIVES, executor );
        DateTimeFormatter isoDateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
        FormattedLog formattedLog = FormattedLog.withUTCTimeZone().withDateTimeFormatter( isoDateTimeFormatter )
                .toOutputStream( outputStreamSupplier );
        formattedLog.setLevel( Level.DEBUG );

        this.inner = formattedLog;
    }

    public void error( String remoteAddress, String message )
    {
        inner.error( "%s %s", remoteAddress, message );
    }

    public void error( String remoteAddress, String message, String arg1 )
    {
        inner.error( "%s %s %s", remoteAddress, message, arg1 );
    }

    public void error( String remoteAddress, String message, String arg1, String arg2 )
    {
        inner.error( "%s %s %s %s", remoteAddress, message, arg1, arg2 );
    }

    public void warn( String remoteAddress, String message )
    {
        inner.warn( "%s %s", remoteAddress, message );
    }

    public void warn( String remoteAddress, String message, String arg1 )
    {
        inner.warn( "%s %s %s", remoteAddress, message, arg1 );
    }

    public void warn( String remoteAddress, String message, String arg1, String arg2 )
    {
        inner.warn( "%s %s %s %s", remoteAddress, message, arg1, arg2 );
    }

    public void info( String remoteAddress, String message )
    {
        inner.info( "%s %s", remoteAddress, message );
    }

    public void info( String remoteAddress, String message, String arg1 )
    {
        inner.info( "%s %s %s", remoteAddress, message, arg1 );
    }

    public void info( String remoteAddress, String message, String arg1, String arg2 )
    {
        inner.info( "%s %s %s %s", remoteAddress, message, arg1, arg2 );
    }

    public void info( String remoteAddress, String message, String arg1, String arg2, String arg3 )
    {
        inner.info( "%s %s %s %s", remoteAddress, message, arg1, arg2, arg3 );
    }

    public void debug( String remoteAddress, String message )
    {
        inner.debug( "%s %s", remoteAddress, message );
    }

    public void debug( String remoteAddress, String message, String arg1 )
    {
        inner.debug( "%s %s %s", remoteAddress, message, arg1 );
    }

    public void debug( String remoteAddress, String message, String arg1, String arg2 )
    {
        inner.debug( "%s %s %s %s", remoteAddress, message, arg1, arg2 );
    }
}
