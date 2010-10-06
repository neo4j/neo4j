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

package org.neo4j.remote;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.List;

public final class Agent
{
    @SuppressWarnings( "serial" )
    private static class StartupException extends Exception
    {
        StartupException( String message )
        {
            super( message );
        }
    }

    private Agent( boolean attached ) throws StartupException,
                                     URISyntaxException
    {
        if ( attached )
        {
            // TODO: implement this
            jarfile = null;
            listMethod = null;
            attachMethod = null;
            simpleAttachMethod = null;
            loadMethod = null;
            throw new StartupException( "TODO: Implement this!" );
        }
        else
        {
            jarfile = new File(
                    getClass().getProtectionDomain().getCodeSource().getLocation().toURI() ).getAbsolutePath();
            try
            {
                Class<?> vmClass = Class.forName( "com.sun.tools.attach.VirtualMachine" );
                listMethod = vmClass.getMethod( "list" );
                attachMethod = vmClass.getMethod(
                        "attach",
                        Class.forName( "com.sun.tools.attach.VirtualMachineDescriptor" ) );
                simpleAttachMethod = vmClass.getMethod( "attach", String.class );
                loadMethod = vmClass.getMethod( "loadAgent", String.class,
                        String.class );
            }
            catch ( Exception e )
            {
                throw new StartupException(
                        "Remote Graph Database Agent requires Java 6" );
            }
        }
    }

    private static class Arguments
    {
        String pid = null;
        boolean read_only = false;
        String path = null;
        String uri = null; // TODO: parse this!

        // FIXME: state machine is broken: optional positional pid then more...
        enum ParseState
        {
            GENERAL
            {
                @Override
                ParseState parse( String arg, Arguments args )
                {
                    if ( "-pid".equalsIgnoreCase( arg ) )
                    {
                        return PID;
                    }
                    else if ( "-read_only".equalsIgnoreCase( arg ) )
                    {
                        args.read_only = true;
                        return this;
                    }
                    else
                    {
                        args.uri = arg;
                        if ( args.pid == null )
                        {
                            return PID_SHIFT;
                        }
                        else
                        {
                            return PATH;
                        }
                    }
                }
            },
            PID
            {
                @Override
                ParseState parse( String arg, Arguments args )
                {
                    args.pid = arg;
                    return GENERAL;
                }
            },
            PID_SHIFT
            {
                @Override
                ParseState parse( String arg, Arguments args )
                {
                    args.pid = args.uri;
                    args.uri = arg;
                    return PATH;
                }
            },
            PATH
            {
                @Override
                ParseState parse( String arg, Arguments args )
                {
                    args.pid = args.uri;
                    args.uri = arg;
                    return DONE;
                }
            },
            DONE
            {
                @Override
                ParseState parse( String arg, Arguments args )
                        throws StartupException
                {
                    throw new StartupException( "Too many arguments." );
                }
            };
            abstract ParseState parse( String arg, Arguments args )
                    throws StartupException;
        }

        Arguments( String[] args ) throws StartupException
        {
            ParseState state = ParseState.GENERAL;
            for ( String arg : args )
            {
                state = state.parse( arg, this );
            }
            if ( path == null )
            {
                throw new StartupException( "No path specified!" );
            }
            if ( uri == null )
            {
                throw new StartupException( "No resource uri specified!" );
            }
        }

        Arguments( String agentArgs )
        {
            int pos = 0;
            while ( pos < agentArgs.length() )
            {
                int start = pos;
                pos = agentArgs.indexOf( ':', pos );
                check( pos );
                String key = agentArgs.substring( start, pos );
                start = ( pos-- ) + 1;
                do
                {
                    pos += 2;
                    pos = agentArgs.indexOf( ';', pos );
                    check( pos );
                    if ( pos + 1 == agentArgs.length() ) break;
                }
                while ( agentArgs.charAt( pos + 1 ) == ';' );
                String value = agentArgs.substring( start, pos );
                pos++;
                if ( key.equalsIgnoreCase( "read_only" ) )
                {
                    read_only = Boolean.parseBoolean( value );
                }
                else if ( key.equalsIgnoreCase( "path" ) )
                {
                    path = value.replace( ";;", ";" );
                }
                else if ( key.equalsIgnoreCase( "uri" ) )
                {
                    uri = value.replace( ";;", ";" );
                }
            }
        }

        private static void check( int pos )
        {
            if ( pos == -1 )
            {
                throw new IllegalArgumentException(
                        "Illegally formatted string representation of Arguments." );
            }
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj instanceof Arguments )
            {
                Arguments other = (Arguments) obj;
                if ( other.read_only == read_only )
                    if ( path == other.path || path.equals( other.path ) )
                        if ( uri == other.uri || uri.equals( other.uri ) )
                            return true;
            }
            return false;
        }

        @Override
        public String toString()
        {
            StringBuilder result = new StringBuilder( "read_only:" );
            result.append( read_only );
            result.append( ";" );
            if ( path != null )
            {
                result.append( "path:" );
                result.append( path.replace( ";", ";;" ) );
                result.append( ";" );
            }
            if ( uri != null )
            {
                result.append( "uri:" );
                result.append( uri.replace( ";", ";;" ) );
                result.append( ";" );
            }
            return result.toString();
        }
    }

    private static volatile boolean agent_vm = false;

    private final String jarfile;
    private final Method listMethod;
    private final Method attachMethod;
    private final Method simpleAttachMethod;
    private final Method loadMethod;

    public static void main( String[] args ) throws URISyntaxException
    {
        final Arguments arguments;
        final Agent agent;
        try
        {
            arguments = new Arguments( args );
            agent = new Agent( false );
        }
        catch ( StartupException err )
        {
            System.err.println( err.getMessage() );
            System.exit( 1 );
            return;
        }
        // <TEST>
        if ( !arguments.equals( new Arguments( arguments.toString() ) ) )
        {
            System.err.println( "Implementation error in serialization!" );
            System.exit( 1 );
            return;
        }
        // </TEST>
        agent_vm = true;
        try
        {
            agent.attach( arguments );
        }
        catch ( Exception ex )
        {
            System.err.println( "Attachment process failed with an unexpected exception." );
            ex.printStackTrace();
        }
        finally
        {
            agent_vm = false;
        }
    }

    public static void agentmain( String agentArgs )
    {
        if ( !agent_vm )
        {
            final Agent agent;
            try
            {
                agent = new Agent( true );
            }
            catch ( StartupException exception )
            {
                return; // Neo4j is not loaded in this JVM
            }
            catch ( URISyntaxException ex ) // Should never happen
            {
                throw new RuntimeException( ex );
            }
            try
            {
                agent.dispatch( new Arguments( agentArgs ) );
            }
            catch ( Exception ex )
            {
                final Throwable exception;
                if ( ex instanceof InvocationTargetException )
                {
                    exception = ( (InvocationTargetException) ex ).getTargetException();
                }
                else
                {
                    exception = ex;
                }
                System.err.println( "Failed to attach Remote Graph Database Agent." );
                exception.printStackTrace();
            }
        }
    }

    private void attach( Arguments arguments ) throws Exception
    {
        if ( arguments.pid != null )
        {
            attach( simpleAttachMethod, arguments.pid, arguments );
        }
        else
        {
            for ( Object descriptor : (List<?>) listMethod.invoke( null ) )
            {
                attach( attachMethod, descriptor, arguments );
            }
        }
    }

    private void attach( Method attach, Object id, Arguments arguments )
            throws Exception
    {
        try
        {
            Object vm = attach.invoke( null, id );
            loadMethod.invoke( vm, jarfile, arguments.toString() );
        }
        catch ( InvocationTargetException ex )
        {
            System.err.println( "Could not attach to " + id );
        }
    }

    private void dispatch( Arguments arguments ) throws Exception
    {
        /* TODO: implement this.
         * o We need some way to access all loaded EmbeddedGraphDBs
         * o We need to be able to match the path of the graphdb
         *   with the supplied path.
         */
    }
}
