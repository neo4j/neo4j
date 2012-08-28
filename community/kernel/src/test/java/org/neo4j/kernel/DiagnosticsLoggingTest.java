package org.neo4j.kernel;

import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.containsString;

import org.junit.Test;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Loggers;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.test.ImpermanentGraphDatabase;


public class DiagnosticsLoggingTest
{
    @Test
    public void shouldSeeHelloWorld()
    {
        FakeDatabase db = new FakeDatabase();
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
        public StringLogger getLogger( String name )
        {
            if ( name.equals( Loggers.DIAGNOSTICS ) )
            {
                return this;
            } else
            {
                return StringLogger.DEV_NULL;
            }
        }
    }

    private class FakeDatabase extends ImpermanentGraphDatabase
    {
        @Override
        protected Logging createStringLogger()
        {
            return new FakeLogger();
        }

        public FakeLogger getLogger()
        {
            return (FakeLogger) logging;
        }
    }
}
