/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.shell;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A common implementation of a {@link ShellClient}.
 */
public abstract class AbstractClient implements ShellClient
{
    /**
     * The session key for the prompt key, just like in Bash.
     */
    public static final String PROMPT_KEY = "PS1";

    /**
     * The session key for whether or not to print stack traces for exceptions. 
     */
    public static final String STACKTRACES_KEY = "STACKTRACES";

    /**
     * When displaying node ids this variable is also used for getting an
     * appropriate property value from that node to display as the title.
     * This variable can contain many property keys (w/ regex) separated by
     * comma prioritized in order.
     */
    public static final String TITLE_KEYS_KEY = "TITLE_KEYS";

    /**
     * The maximum length of titles to be displayed.
     */
    public static final String TITLE_MAX_LENGTH = "TITLE_MAX_LENGTH";

    private static final Set<String> EXIT_COMMANDS = new HashSet<String>(
        Arrays.asList( "exit", "quit" ) );

    private Console console;

    public void grabPrompt()
    {
        this.init();
        while ( true )
        {
            try
            {
                this.console.format( tryGetProperPromptString() );
                String line = this.readLine();
                if ( EXIT_COMMANDS.contains( line ) )
                {
                    break;
                }

                String result = this.getServer().interpretLine(
                    line, this.session(), this.getOutput() );
                if ( result == null || result.trim().length() == 0 )
                {
                    continue;
                }
                if ( result.contains( "e" ) )
                {
                    break;
                }
            }
            catch ( Exception e )
            {
                if ( this.shouldPrintStackTraces() )
                {
                    e.printStackTrace();
                }
                this.console.format( getShortExceptionMessage( e ) + "\n" );
            }
        }
        this.shutdown();
    }

    protected String getShortExceptionMessage( Exception e )
    {
        String result = e instanceof ShellException ?
            "" : e.getClass().getName() + ": ";
        result += e.getMessage();
        return result;
    }

    protected String tryGetProperPromptString() throws ShellException
    {
        String result = null;
        try
        {
            result = ( String ) getSessionVariable( PROMPT_KEY, null, true );
        }
        catch ( Exception e )
        {
            result = ( String ) getSessionVariable( PROMPT_KEY, null, false );
        }
        return result;
    }

    protected void shutdown()
    {
    }

    private boolean shouldPrintStackTraces()
    {
        try
        {
            Object value = this.session().get( STACKTRACES_KEY );
            return this.getSafeBooleanValue( value, false );
        }
        catch ( RemoteException e )
        {
            return true;
        }
    }

    private boolean getSafeBooleanValue( Object value, boolean def )
    {
        if ( value == null )
        {
            return def;
        }
        return Boolean.parseBoolean( value.toString() );
    }

    private void init()
    {
        try
        {
            possiblyGrabDefaultVariableFromServer( PROMPT_KEY, "$ " );
            possiblyGrabDefaultVariableFromServer( TITLE_KEYS_KEY, null );
            possiblyGrabDefaultVariableFromServer( TITLE_MAX_LENGTH, null );
            this.getOutput().println( this.getServer().welcome() );

            // Grab a jline console if available, else a standard one.
            this.console = JLineConsole.newConsoleOrNullIfNotFound( this );
            if ( this.console == null )
            {
                System.out.println( "Want bash-like features? throw in " +
                "jLine on the classpath" );
                this.console = new StandardConsole();
            }
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    protected void possiblyGrabDefaultVariableFromServer( String key,
        Serializable defaultValue )
    {
        try
        {
            if ( this.session().get( key ) == null )
            {
                Serializable value = this.getServer().getProperty( key );
                if ( value == null )
                {
                    value = defaultValue;
                }
                if ( value != null )
                {
                    this.session().set( key, value );
                }
            }
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    protected Serializable getSessionVariable( String key,
        Serializable defaultValue, boolean interpret ) throws ShellException
    {
        try
        {
            Serializable result = this.session().get( key );
            if ( result == null )
            {
                result = defaultValue;
            }
            if ( interpret && result != null )
            {
                result = this.getServer().interpretVariable( key, result,
                    session() );
            }
            return result;
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    public String readLine()
    {
        return this.console.readLine();
    }

    static String[] getExitCommands()
    {
        return EXIT_COMMANDS.toArray( new String[ EXIT_COMMANDS.size() ] );
    }
}
