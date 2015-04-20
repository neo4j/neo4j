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
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.LogMarker;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

public class DiagnosticsLoggingTest
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldSeeHelloWorld()
    {
        FakeLogger logger = new FakeLogger();
        String storeDir = folder.getRoot().getAbsolutePath();
        GraphDatabaseService db =
                new TestGraphDatabaseFactory().setLogging( logger ).newEmbeddedDatabase( storeDir );

        String messages = logger.getMessages();
        assertThat( messages, containsString( "Network information" ) );
        assertThat( messages, containsString( "Disk space on partition" ) );
        assertThat( messages, containsString( "Local timezone" ) );
        db.shutdown();
    }

    @Test
    public void shouldSeePageCacheConfigurationWithDumpConfigurationEnabled()
    {
        FakeLogger logger = new FakeLogger();
        GraphDatabaseService db = new TestGraphDatabaseFactory().
                setLogging( logger ).
                newEmbeddedDatabaseBuilder( folder.getRoot().getAbsolutePath() ).
                setConfig( GraphDatabaseSettings.dump_configuration, Settings.TRUE ).
                setConfig( GraphDatabaseSettings.pagecache_memory, "4M" ).
                newGraphDatabase();

        String messages = logger.getMessages();
        assertThat( messages, containsString( "Page cache size: 4 MiB" ) );
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
}
