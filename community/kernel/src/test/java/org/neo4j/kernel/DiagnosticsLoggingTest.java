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
package org.neo4j.kernel;

import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.containsString;

import java.io.File;

import org.junit.Test;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.test.TargetDirectory;

public class DiagnosticsLoggingTest
{
    @Test
    public void shouldSeeHelloWorld()
    {
        File temp = TargetDirectory.forTest( getClass() ).directory( "temp", true );
        // We use an EmbeddedDatabase because Impermanent does not create the directory and returns 0 for disk space
        FakeDatabase db = new FakeDatabase( temp.getAbsolutePath() );
        FakeLogger logger = db.getLogger();
        String messages = logger.getMessages();
        assertThat( messages, containsString( "Network information" ) );
        assertThat( messages, containsString( "Disk space on partition" ) );
        assertThat( messages, containsString( "Local timezone" ) );
        db.shutdown();
    }

    private class FakeLogger extends StringLogger implements Logging
    {
        private StringBuilder messages = new StringBuilder();

        public String getMessages()
        {
            return messages.toString();
        }

        private void appendLine( String mess )
        {
            messages.append( mess ).append( "\n" );
        }

        @Override
        public void logLongMessage( String msg, Visitor<LineLogger> source, boolean flush )
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
        public StringLogger getLogger( Class loggingClass )
        {
            if ( loggingClass.equals( DiagnosticsManager.class ) )
            {
                return this;
            }
            else
            {
                return StringLogger.DEV_NULL;
            }
        }
    }

    private class FakeDatabase extends EmbeddedGraphDatabase
    {
        public FakeDatabase( String path )
        {
            super( path );
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
