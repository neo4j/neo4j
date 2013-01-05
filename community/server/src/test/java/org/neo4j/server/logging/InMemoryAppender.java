/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.logging;

import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;

public class InMemoryAppender
{
    private StringWriter stringWriter = new StringWriter();
    private Handler stringHandler;
    private final java.util.logging.Logger julLogger;
    private final Level level;

    public InMemoryAppender( Logger logger )
    {
        this( logger, Level.ALL );
    }

    private InMemoryAppender( Logger logger, Level level )
    {
        this.level = level;
        julLogger = java.util.logging.Logger.getLogger( this.getClass()
                .toString() );
        changeLogger( logger, julLogger );
        reset();
    }

    private void changeLogger( Logger logger, java.util.logging.Logger julLogger )
    {
        Field loggerField = findLoggerField( logger );
        try
        {
            loggerField.setAccessible( true );
            loggerField.set( logger, julLogger );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private Field findLoggerField( Logger logger )
    {
        try
        {
            return logger.getClass()
                    .getDeclaredField( "logger" );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "The field [logger] must be present for testing", e );
        }
    }

    @Override
    public String toString()
    {
        return stringWriter.toString();
    }

    public void reset()
    {
        stringWriter = new StringWriter();
        stringHandler = new Handler()
        {
            @Override
            public void publish( java.util.logging.LogRecord record )
            {
                stringWriter.append( getFormatter().format( record ) );
            };

            @Override
            public void close() throws SecurityException
            {
            }

            @Override
            public void flush()
            {
            }
        };
        stringHandler.setFormatter( new SimpleFormatter() );
        julLogger.addHandler( stringHandler );

        julLogger.setLevel( level );
    }
}
