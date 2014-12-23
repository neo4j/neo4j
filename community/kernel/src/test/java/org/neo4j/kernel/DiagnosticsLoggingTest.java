/**
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
package org.neo4j.kernel;

import org.junit.Test;

import java.util.HashMap;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.LogMarker;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.test.ImpermanentGraphDatabase;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

public class DiagnosticsLoggingTest
{
    @Test
    public void shouldSeeHelloWorld()
    {
        FakeDatabase db = new FakeDatabase( new HashMap<String,String>() );
        FakeLogger logger = db.getLogger();
        String messages = logger.getMessages();
        assertThat( messages, containsString( "Network information" ) );
        assertThat( messages, containsString( "Disk space on partition" ) );
        assertThat( messages, containsString( "Local timezone" ) );
        db.shutdown();
    }

    @Test
    public void shouldSeePageCacheConfigurationWithDumpConfigurationEnabled()
    {
        HashMap<String,String> settings = new HashMap<>();
        settings.put( GraphDatabaseSettings.dump_configuration.name(), "true" );
        settings.put( GraphDatabaseSettings.pagecache_memory.name(), "8M" );
        FakeDatabase db = new FakeDatabase( settings );
        FakeLogger logger = db.getLogger();
        String messages = logger.getMessages();
        assertThat( messages, containsString( "Page cache size: 8MB" ) );
        db.shutdown();
    }

    private class FakeLogger extends StringLogger implements Logging
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
        public void logMessage( String msg, boolean flush )
        {
            appendLine( msg );
        }

        @Override
        public void logMessage( String msg, LogMarker marker )
        {
            appendLine( msg );
        }

        @Override
        public void logMessage( String msg, Throwable cause, boolean flush )
        {
            appendLine( msg );
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
        protected void logLine( String line )
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

    @SuppressWarnings("deprecation")
    private class FakeDatabase extends ImpermanentGraphDatabase
    {
        public FakeDatabase( HashMap<String, String> settings )
        {
            super( settings );
        }

        @Override
        protected Logging createLogging()
        {
            return new FakeLogger();
        }

        public FakeLogger getLogger()
        {
            return (FakeLogger) logging;
        }
    }
}
