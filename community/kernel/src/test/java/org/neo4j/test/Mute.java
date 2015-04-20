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
package org.neo4j.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

/**
 * Mutes outputs such as System.out, System.err and java.util.logging for example when running a test.
 * It's also a {@link TestRule} which makes it fit in nicely in JUnit.
 * 
 * The muting occurs visitor-style and if there's an exception in the code executed when being muted
 * all the logging that was temporarily muted will be resent to the peers as if they weren't muted to begin with.
 */
public final class Mute implements TestRule
{
    public static Mute mute( Mutable... mutables )
    {
        return new Mute( mutables );
    }
    
    public static Mute muteAll()
    {
        return mute( System.out, System.err, java_util_logging );
    }

    public enum System implements Mutable
    {
        out
        {
            @Override
            PrintStream replace( PrintStream replacement )
            {
                PrintStream old = java.lang.System.out;
                java.lang.System.setOut( replacement );
                return old;
            }
        },
        err
        {
            @Override
            PrintStream replace( PrintStream replacement )
            {
                PrintStream old = java.lang.System.err;
                java.lang.System.setErr( replacement );
                return old;
            }
        };

        @Override
        public Voice mute()
        {
            final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            final PrintStream old = replace( new PrintStream( buffer ) );
            return new Voice()
            {
                @Override
                void restore( boolean failure ) throws IOException
                {
                    replace( old ).flush();
                    if ( failure )
                    {
                        old.write( buffer.toByteArray() );
                    }
                }
            };
        }

        abstract PrintStream replace( PrintStream replacement );
    }

    public static final Mutable java_util_logging = java_util_logging( null, null );

    public static Mutable java_util_logging( OutputStream redirectTo, Level level )
    {
        final Handler replacement = redirectTo == null ? null : new StreamHandler( redirectTo, new SimpleFormatter() );
        if ( replacement != null && level != null )
        {
            replacement.setLevel( level );
        }
        return new Mutable()
        {
            @Override
            public Voice mute()
            {
                final Logger logger = LogManager.getLogManager().getLogger( "" );
                final Level level = logger.getLevel();
                final Handler[] handlers = logger.getHandlers();
                for ( Handler handler : handlers )
                {
                    logger.removeHandler( handler );
                }
                if ( replacement != null )
                {
                    logger.addHandler( replacement );
                    logger.setLevel( Level.ALL );
                }
                return new Voice()
                {
                    @Override
                    void restore( boolean failure ) throws IOException
                    {
                        for ( Handler handler : handlers )
                        {
                            logger.addHandler( handler );
                        }
                        logger.setLevel( level );
                        if ( replacement != null )
                        {
                            logger.removeHandler( replacement );
                        }
                    }
                };
            }
        };
    }

    public <T> T call( Callable<T> callable ) throws Exception
    {
        Voice[] voices = captureVoices();
        boolean failure = true;
        try
        {
            T result = callable.call();
            failure = false;
            return result;
        }
        finally
        {
            releaseVoices( voices, failure );
        }
    }

    private final Mutable[] mutables;

    private Mute( Mutable[] mutables )
    {
        this.mutables = mutables;
    }

    @Override
    public Statement apply( final Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                Voice[] voices = captureVoices();
                boolean failure = true;
                try
                {
                    base.evaluate();
                    failure = false;
                }
                finally
                {
                    releaseVoices( voices, failure );
                }
            }
        };
    }

    public interface Mutable
    {
        Voice mute();
    }

    private static abstract class Voice
    {
        abstract void restore( boolean failure ) throws IOException;
    }

    Voice[] captureVoices()
    {
        Voice[] voices = new Voice[mutables.length];
        boolean ok = false;
        try
        {
            for ( int i = 0; i < voices.length; i++ )
            {
                voices[i] = mutables[i].mute();
            }
            ok = true;
        }
        finally
        {
            if ( !ok )
            {
                releaseVoices( voices, false );
            }
        }
        return voices;
    }

    void releaseVoices( Voice[] voices, boolean failure )
    {
        List<Throwable> failures = null;
        try
        {
            failures = new ArrayList<>( voices.length );
        }
        catch ( Throwable oom )
        {
            // nothing we can do...
        }
        for ( Voice voice : voices )
        {
            if ( voice != null )
            {
                try
                {
                    voice.restore( failure );
                }
                catch ( Throwable exception )
                {
                    if ( failures != null )
                    {
                        failures.add( exception );
                    }
                }
            }
        }
        if ( failures != null && !failures.isEmpty() )
        {
            for ( Throwable exception : failures )
            {
                exception.printStackTrace();
            }
        }
    }
}
