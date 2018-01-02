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
package org.neo4j.shell;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * A general shell exception when an error occurs. Standard Java exceptions (java.*)
 * gets wrapped normally in a ShellException. For other unknown exceptions the
 * actual stacktrace is taken out and injected as a string. This is done because
 * we don't know how the clients classpath looks compared to the server and
 * we guard from ClassNotFoundExceptions on the client this way.
 */
public class ShellException extends Exception
{
    private static final long serialVersionUID = 1L;
    
	private final String stackTraceAsString;

	public ShellException( String message )
	{
	    this( message, (String) null );
	}
	
	private ShellException( String message, Throwable cause )
    {
        super( message, cause );
        this.stackTraceAsString = null;
    }

    private ShellException( String message, String stackTraceAsString )
	{
	    super( message );
        this.stackTraceAsString = stackTraceAsString;
	}

    @Override
    public void printStackTrace( PrintStream s )
    {
        if ( stackTraceAsString != null )
        {
            s.print( stackTraceAsString );
        }
        else if ( getCause() != null )
        {
            getCause().printStackTrace( s );
        }
        else
        {
            super.printStackTrace( s );
        }
    }

    @Override
    public void printStackTrace( PrintWriter s )
    {
        if ( stackTraceAsString != null )
        {
            s.print( stackTraceAsString );
        }
        else if ( getCause() != null )
        {
            getCause().printStackTrace( s );
        }
        else
        {
            super.printStackTrace( s );
        }
    }
    
	/**
	 * Serializes a {@link Throwable} to a String and uses that as a message
	 * in a {@link ShellException}. This is because we can't rely on the
	 * client having the full classpath the server has.
	 * @param cause the {@link Throwable} to wrap in a {@link ShellException}.
	 * @return the {@link ShellException} wrapped around the {@code cause}.
	 */
	public static ShellException wrapCause( Throwable cause )
	{
        if ( isCompletelyRecognizedException( cause ) )
        {
            return cause instanceof ShellException ? (ShellException) cause : new ShellException( getFirstMessage( cause ), cause );
        }
        else
        {
            return softWrap( cause );
        }
	}
	
	private static ShellException softWrap( Throwable cause )
	{
	    String stackTraceAsString = stackTraceAsString( cause );
	    String message = getFirstMessage( cause );
	    if ( !( cause instanceof ShellException ) )
	    {
	        message = cause.getClass().getSimpleName() + ": " + message;
	    }
        return new ShellException( message, stackTraceAsString );
	}

    public static String getFirstMessage( Throwable cause )
    {
        while ( cause != null )
        {
            String message = cause.getMessage();
            if ( message != null && message.length() > 0 )
            {
                return message;
            }
            cause = cause.getCause();
        }
        return null;
    }

    private static boolean isCompletelyRecognizedException( Throwable e )
    {
        String packageName = e.getClass().getPackage().getName();
        if ( !( e instanceof ShellException ) &&
                !packageName.startsWith( "java." ) )
        {
            return false;
        }
        Throwable cause = e.getCause();
        return cause == null ? true : isCompletelyRecognizedException( cause );
    }
    
    public static String stackTraceAsString( Throwable cause )
    {
        StringWriter writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter( writer, false );
        cause.printStackTrace( printWriter );
        printWriter.close();
        return writer.getBuffer().toString();
    }

    public String getStackTraceAsString()
    {
        return stackTraceAsString;
    }
}
