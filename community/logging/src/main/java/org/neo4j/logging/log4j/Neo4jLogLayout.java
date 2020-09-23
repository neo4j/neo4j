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
package org.neo4j.logging.log4j;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.impl.DefaultLogEventFactory;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.spi.AbstractLogger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.function.Consumer;

import org.neo4j.exceptions.UnsatisfiedDependencyException;
import org.neo4j.logging.Log;

import static org.apache.logging.log4j.core.layout.PatternLayout.newSerializerBuilder;

@Plugin( name = "Neo4jLogLayout", category = Node.CATEGORY, elementType = Layout.ELEMENT_TYPE, printObject = true )
public class Neo4jLogLayout extends AbstractStringLayout
{
    protected final Serializer eventSerializer;
    protected volatile Consumer<Log> headerLogger;
    protected volatile String headerClassName;

    protected Neo4jLogLayout( String pattern, DefaultConfiguration config )
    {
        super( config, StandardCharsets.UTF_8, null, null );
        this.eventSerializer = newSerializerBuilder()
                .setConfiguration( config )
                .setAlwaysWriteExceptions( true )
                .setDisableAnsi( false )
                .setNoConsoleNoAnsi( false )
                .setPattern( pattern )
                .build();
    }

    @PluginFactory
    public static Neo4jLogLayout createLayout( @PluginAttribute( "pattern" ) String pattern )
    {
        return new Neo4jLogLayout( pattern, new DefaultConfiguration() );
    }

    @Override
    public byte[] getHeader()
    {
        if ( headerLogger == null )
        {
            return super.getHeader();
        }
        ByteArrayLogger byteArrayLogger = new ByteArrayLogger();
        Log4jLog log = new Log4jLog( byteArrayLogger );
        try
        {
            headerLogger.accept( log );
        }
        catch ( UnsatisfiedDependencyException e )
        {
            // This will happen if we are asked to rotate to the next file before all dependencies are set up.
            // Most likely scenario would be that a log file already exist on start up that is close to rotating.
            // The only thing that happens is that the header will not be printed in the beginning of the file,
            // but since the same diagnostics are printed on start up after all dependencies are met the file
            // will still get the information only later.
        }
        return byteArrayLogger.getBytes();
    }

    @Override
    public String toSerializable( LogEvent event )
    {
        return eventSerializer.toSerializable( event );
    }

    // Will make all log files created after this is set get their header from the consumer.
    public void setHeaderLogger( Consumer<Log> headerLogger, String className )
    {
        this.headerLogger = headerLogger;
        this.headerClassName = className;
    }

    private class ByteArrayLogger extends AbstractLogger
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        @Override
        public boolean isEnabled( Level level, Marker marker, Message message, Throwable t )
        {
            return true;
        }

        @Override
        public boolean isEnabled( Level level, Marker marker, CharSequence message, Throwable t )
        {
            return true;
        }

        @Override
        public boolean isEnabled( Level level, Marker marker, Object message, Throwable t )
        {
            return true;
        }

        @Override
        public boolean isEnabled( Level level, Marker marker, String message, Throwable t )
        {
            return true;
        }

        @Override
        public boolean isEnabled( Level level, Marker marker, String message )
        {
            return true;
        }

        @Override
        public boolean isEnabled( Level level, Marker marker, String message, Object... params )
        {
            return true;
        }

        @Override
        public boolean isEnabled( Level level, Marker marker, String message, Object p0 )
        {
            return true;
        }

        @Override
        public boolean isEnabled( Level level, Marker marker, String message, Object p0, Object p1 )
        {
            return true;
        }

        @Override
        public boolean isEnabled( Level level, Marker marker, String message, Object p0, Object p1, Object p2 )
        {
            return true;
        }

        @Override
        public boolean isEnabled( Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3 )
        {
            return true;
        }

        @Override
        public boolean isEnabled( Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4 )
        {
            return true;
        }

        @Override
        public boolean isEnabled( Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5 )
        {
            return true;
        }

        @Override
        public boolean isEnabled( Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6 )
        {
            return true;
        }

        @Override
        public boolean isEnabled( Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6,
                Object p7 )
        {
            return true;
        }

        @Override
        public boolean isEnabled( Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6,
                Object p7, Object p8 )
        {
            return true;
        }

        @Override
        public boolean isEnabled( Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6,
                Object p7, Object p8, Object p9 )
        {
            return true;
        }

        @Override
        public void logMessage( String fqcn, Level level, Marker marker, Message message, Throwable t )
        {
            String logMsg = toSerializable(
                    DefaultLogEventFactory.getInstance().createEvent( headerClassName, marker, fqcn, level, message, Collections.emptyList(), t ) );
            try
            {
                baos.write( logMsg.getBytes( StandardCharsets.UTF_8 ) );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }

        @Override
        public Level getLevel()
        {
            return null;
        }

        public byte[] getBytes()
        {
            return baos.toByteArray();
        }
    }
}
