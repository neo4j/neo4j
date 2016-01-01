/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.impl.util.StringLogger.LineLogger;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.LogMarker;
import org.neo4j.kernel.logging.Logging;

class FakeLogger extends StringLogger implements Logging, LineLogger
{
    private final StringBuilder messages = new StringBuilder();

    public String getMessages()
    {
        return messages.toString();
    }

    private void appendLine( String mess )
    {
        messages.append( mess ).append( "\n" );
    }

    @Override
    protected void doDebug( String msg, Throwable cause, boolean flush, LogMarker logMarker )
    {
        appendLine( msg );
    }

    @Override
    public void info( String msg, Throwable cause, boolean flush, LogMarker logMarker )
    {
        appendLine( msg );
    }

    @Override
    public void warn( String msg, Throwable cause, boolean flush, LogMarker logMarker )
    {
        appendLine( msg );
    }

    @Override
    public void error( String msg, Throwable cause, boolean flush, LogMarker logMarker )
    {
        appendLine( msg );
    }

    @Override
    public void logLongMessage( String msg, Visitor<LineLogger, RuntimeException> source, boolean flush )
    {
        appendLine( msg );
        source.visit( new LineLogger()
        {
            @Override
            public void logLine( String line )
            {
                appendLine( line );
            }
        } );
    }

    @Override
    public void addRotationListener( Runnable listener )
    {
    }

    @Override
    public void flush()
    {
    }

    @Override
    public void close()
    {
    }

    @Override
    public void logLine( String line )
    {
        appendLine( line );
    }

    @Override
    public StringLogger getMessagesLog( Class loggingClass )
    {
        return this;
    }

    @Override
    public ConsoleLogger getConsoleLog( Class loggingClass )
    {
        return new ConsoleLogger( StringLogger.SYSTEM );
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
    }

    @Override
    public void stop() throws Throwable
    {
    }

    @Override
    public void shutdown() throws Throwable
    {
    }
}
