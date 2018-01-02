/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.shell.impl;

import java.io.Serializable;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.helpers.Cancelable;
import org.neo4j.shell.Console;
import org.neo4j.shell.CtrlCHandler;
import org.neo4j.shell.Output;
import org.neo4j.shell.Response;
import org.neo4j.shell.ShellClient;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellServer;
import org.neo4j.shell.Variables;
import org.neo4j.shell.Welcome;

/**
 * A common implementation of a {@link ShellClient}.
 */
public abstract class AbstractClient implements ShellClient
{
    private final CtrlCHandler signalHandler;
    public static final String WARN_UNTERMINATED_INPUT =
            "Warning: Exiting with unterminated multi-line input.";
    private static final Set<String> EXIT_COMMANDS = new HashSet<>(
        Arrays.asList( "exit", "quit", null ) );

    private Console console;
    private long timeConnection;
    private volatile boolean end;
    private final Collection<String> multiLine = new ArrayList<>();
    private Serializable id;
    private String prompt;

    private final Map<String, Serializable> initialSession;
    
    public AbstractClient( Map<String, Serializable> initialSession, CtrlCHandler signalHandler )
    {
        this.signalHandler = signalHandler;
        this.initialSession = initialSession;
    }

    private Runnable getTerminateAction()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    getServer().terminate( getId() );
                }
                catch ( Exception e )
                {
                    printStackTrace( e );
                }
            }
        };
    }

    public void grabPrompt()
    {
        init();
        Runnable ctrlcAction = getTerminateAction();
        while ( !end )
        {
            String command = readLine( getPrompt() );
            Cancelable cancelable = null;
            try
            {
                cancelable = signalHandler.install( ctrlcAction );
                evaluate( command );
            }
            catch ( Exception e )
            {
                printStackTrace( e );
            }
            finally
            {
                if ( cancelable != null )
                {
                    cancelable.cancel();
                }
            }
        }
        this.shutdown();
    }

    private void printStackTrace( Exception e )
    {
        if ( this.shouldPrintStackTraces() )
        {
            e.printStackTrace();
        }
        else
        {
            this.console.format( getShortExceptionMessage( e ) + "\n" );
        }
    }

    @Override
    public void evaluate( String line ) throws ShellException
    {
        evaluate( line, getOutput() );
    }

    @Override
    public void evaluate( String line, Output out ) throws ShellException
    {
        if ( EXIT_COMMANDS.contains( line ) )
        {
            end(); 
            return;
        }
        
        boolean success = false;
        try
        {
            String expandedLine = fullLine( line );
            Response response = getServer().interpretLine( id, expandedLine, out );
            switch ( response.getContinuation() )
            {
            case INPUT_COMPLETE:
                endMultiLine();
                break;
            case INPUT_INCOMPLETE:
                multiLine.add( line );
                break;
            case EXIT:
                getServer().leave( id );
                end();
                break;
            case EXCEPTION_CAUGHT:
                endMultiLine();
                break;
            }
            prompt = response.getPrompt();
            success = true;
        }
        catch ( RemoteException e )
        {
            throw ShellException.wrapCause( e );
        }
        finally
        {
            if ( !success )
                endMultiLine();
        }
    }

    private void endMultiLine()
    {
        multiLine.clear();
    }
    
    private String fullLine( String line )
    {
        if ( multiLine.isEmpty() )
        {
            return line;
        }
        StringBuilder result = new StringBuilder();
        for ( String oneLine : multiLine )
        {
            result.append( result.length() > 0 ? "\n" : "" ).append( oneLine );
        }
        return result.append( "\n" + line ).toString();
    }

    @Override
    public void end()
    {
        end = true;
    }

    protected static String getShortExceptionMessage( Exception e )
    {
        return e.getMessage();
    }

    public String getPrompt()
    {
        if ( !multiLine.isEmpty() )
        {
            return "> ";
        }
        return prompt;
    }

    public boolean shouldPrintStackTraces()
    {
        try
        {
            String stringValue = (String) getServer().interpretVariable( id, Variables.STACKTRACES_KEY );
            return Boolean.parseBoolean( stringValue );
        }
        catch ( Exception e )
        {
            return true;
        }
    }

    protected void init()
    {
        try
        {
            // To make sure we have a connection to our server.
            getServer();

            // Grab a jline console if available, else a standard one.
            console = JLineConsole.newConsoleOrNullIfNotFound( this );
            if ( console == null )
            {
                System.out.println( "Want bash-like features? throw in " +
                        "jLine (http://jline.sourceforge.net) on the classpath" );
                console = new StandardConsole();
            }
            getOutput().println();
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    protected void sayHi( ShellServer server ) throws RemoteException, ShellException
    {
        Welcome welcome = server.welcome( initialSession );
        id = welcome.getId();
        prompt = welcome.getPrompt();
        if ( !welcome.getMessage().isEmpty() )
        {
            getOutput().println( welcome.getMessage() );
        }
    }

    protected String readLine( String prompt )
    {
        return console.readLine( prompt );
    }

    protected void updateTimeForMostRecentConnection()
    {
        this.timeConnection = System.currentTimeMillis();
    }
    
    public long timeForMostRecentConnection()
    {
        return timeConnection;
    }
    
    public void shutdown()
    {
        if ( !multiLine.isEmpty() )
        {
            try
            {
                getOutput().println( WARN_UNTERMINATED_INPUT );
            }
            catch ( RemoteException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
    
    @Override
    public Serializable getId()
    {
        return id;
    }

    protected void tryUnexport( Remote remote )
    {
    	try
    	{
    		UnicastRemoteObject.unexportObject( remote, true );
    	}
    	catch ( NoSuchObjectException e )
    	{
    		System.out.println( "Couldn't unexport: " + remote );
    	}
    }
    
    @Override
    public void setSessionVariable( String key, Serializable value ) throws ShellException
    {
        try
        {
            getServer().setSessionVariable( id, key, value );
        }
        catch ( RemoteException e )
        {
            throw ShellException.wrapCause( e );
        }
    }
}
