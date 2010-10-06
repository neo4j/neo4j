/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * A general shell exception when an error occurs.
 */
public class ShellException extends Exception
{
	/**
	 * @param message the description of the exception.
	 */
	public ShellException( String message )
	{
		super( message );
	}

	/**
	 * @param cause the cause of the exception.
	 */
	public ShellException( Throwable cause )
	{
		super( cause );
	}

	public static ShellException wrapCause( Throwable cause )
	{
        if ( cause instanceof ShellException )
        {
            return isCompletelyRecognizedException( cause ) ? (ShellException) cause :
                    softWrap( cause );
        }
        else if ( isCompletelyRecognizedException( cause ) )
        {
            return new ShellException( cause );
        }
        else
        {
            return softWrap( cause );
        }
	}
	
	private static ShellException softWrap( Throwable cause )
	{
	    return new ShellException( stackTraceAsString( cause ) );
	}

    private static boolean isCompletelyRecognizedException( Throwable e )
    {
        String packageName = e.getClass().getPackage().getName();
        if ( !( e instanceof ShellException ) &&
                !packageName.startsWith( "java" ) )
        {
            return false;
        }
        Throwable cause = e.getCause();
        return cause == null ? true : isCompletelyRecognizedException( cause );
    }
    
    private static String stackTraceAsString( Throwable cause )
    {
        StringWriter writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter( writer, false );
        cause.printStackTrace( printWriter );
        printWriter.close();
        return writer.getBuffer().toString();
    }
}
