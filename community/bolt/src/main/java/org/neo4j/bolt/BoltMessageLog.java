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

package org.neo4j.bolt;

import io.netty.channel.Channel;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.RotatingFileOutputStreamSupplier;

import java.io.File;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.Executor;

public class BoltMessageLog extends LifecycleAdapter
{
    private RotatingFileOutputStreamSupplier rotatingSupplier;
    private final Log inner;

    public static BoltMessageLog getInstance()
    {
        try
        {
            return new BoltMessageLog( null, new DefaultFileSystemAbstraction(), null );
        }
        catch ( IOException e )
        {
            return null;
        }
    }

    public BoltMessageLog( Config config, FileSystemAbstraction fileSystem, Executor executor )
            throws IOException
    {
        // TODO: draw settings from config, plumb in the executor, etc
        FormattedLog.Builder builder = FormattedLog.withUTCTimeZone();
        File logFile = new File( "/tmp/bolt.log" );

        rotatingSupplier = new RotatingFileOutputStreamSupplier( fileSystem, logFile, 256L, 1000L * 60L * 60L * 24L, 10,
                                                                 null );

        FormattedLog formattedLog = builder.toOutputStream( rotatingSupplier );
        formattedLog.setLevel( Level.DEBUG );

        this.inner = formattedLog;
    }

    public void error( Channel channel, String message )
    {
        inner.error( "[%s] %s", remoteAddress( channel ), message );
    }

    public void error( Channel channel, String message, String arg1 )
    {
        inner.error( "[%s] %s %s", remoteAddress( channel ), message, arg1 );
    }

    public void error( Channel channel, String message, String arg1, String arg2 )
    {
        inner.error( "[%s] %s %s %s", remoteAddress( channel ), message, arg1, arg2 );
    }

    public void warn( Channel channel, String message )
    {
        inner.warn( "[%s] %s", remoteAddress( channel ), message );
    }

    public void warn( Channel channel, String message, String arg1 )
    {
        inner.warn( "[%s] %s %s", remoteAddress( channel ), message, arg1 );
    }

    public void warn( Channel channel, String message, String arg1, String arg2 )
    {
        inner.warn( "[%s] %s %s %s", remoteAddress( channel ), message, arg1, arg2 );
    }

    public void info( Channel channel, String message )
    {
        inner.info( "[%s] %s", remoteAddress( channel ), message );
    }

    public void info( Channel channel, String message, String arg1 )
    {
        inner.info( "[%s] %s %s", remoteAddress( channel ), message, arg1 );
    }

    public void info( Channel channel, String message, String arg1, String arg2 )
    {
        inner.info( "[%s] %s %s %s", remoteAddress( channel ), message, arg1, arg2 );
    }

    public void info( Channel channel, String message, String arg1, String arg2, String arg3 )
    {
        inner.info( "[%s] %s %s %s", remoteAddress( channel ), message, arg1, arg2, arg3 );
    }

    public void debug( Channel channel, String message )
    {
        inner.debug( "[%s] %s", remoteAddress( channel ), message );
    }

    public void debug( Channel channel, String message, String arg1 )
    {
        inner.debug( "[%s] %s %s", remoteAddress( channel ), message, arg1 );
    }

    public void debug( Channel channel, String message, String arg1, String arg2 )
    {
        inner.debug( "[%s] %s %s %s", remoteAddress( channel ), message, arg1, arg2 );
    }

    private SocketAddress remoteAddress( Channel channel )
    {
        return channel == null ? null : channel.remoteAddress();
    }

}
