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

package org.neo4j.remote.inspect;

import java.io.PrintStream;

import org.neo4j.graphdb.Direction;
import org.neo4j.remote.RemoteConnection;
import org.neo4j.remote.RemoteResponse;

/**
 * An adapter for simple implementation of {@link Inspector}.
 * @author Tobias Ivarsson
 */
public abstract class InspectionAdapter implements Inspector
{
    /**
     * The types of the events in a {@link RemoteConnection}.
     * @author Tobias Ivarsson
     */
    public enum Event
    {
        /** Event received when a new remote connection is opened. */
        OPEN,
        /** Event received when a remote connection is closed. */
        CLOSE,
        /** Event received when a new transaction is started. */
        BEGIN_TRANSACTION,
        /** Event received when a new node is created. */
        CREATE_NODE( "tx", "id" ),
        /** Event received when a new relationship is created. */
        CREATE_RELATIONSHIP( "tx", "start", "end", "type" ),
        /** Event received when a relationship is fetched (by id). */
        FETCH_RELATIONSHIP( "tx", "id" ),
        /** Event received when properties for a node is requested. */
        FETCH_NODE_PROPERTIES( "tx", "id" ),
        /** Event received when properties for a relationship is requested. */
        FETCH_RELATIONSHIP_PROPERTIES( "tx", "id" ),
        /** Event received when relationships for a node is requested. */
        FETCH_RELATIONSHIPS( "tx", "node", "direction", "types" ),
        /** Event received when a transaction is committed. */
        COMMIT( "tx" ),
        /** Event received when a transaction is rolled back. */
        ROLLBACK( "tx" ),
        /** Event received when a node is deleted. */
        DELETE_NODE( true, "id" ),
        /** Event received when a relationship is deleted. */
        DELETE_RELATIONSHIP( true, "id" ),
        /** Event received when a property is set on a node. */
        SET_NODE_PROPERTY( true, "id", "key", "value" ),
        /** Event received when a property is set on a relationship. */
        SET_RELATIONSHIP_PROPERTY( true, "id", "key", "value" );
        private final String indentation;
        private final String[] argNames;

        private Event( String... argNames )
        {
            this( false, argNames );
        }

        private Event( boolean indent, String... argNames )
        {
            this.indentation = indent ? "    " : "";
            this.argNames = argNames;
        }

        private void print( PrintStream stream, String message )
        {
            stream.println( indentation + message );
        }

        private String call( Object[] args )
        {
            StringBuffer buffer = new StringBuffer( name() );
            buffer.append( "(" );
            appendArgs( buffer, args, argNames );
            buffer.append( ")" );
            return buffer.toString();
        }

        void traceFailure( PrintStream stream, Object[] args, Throwable ex )
        {
            print( stream, call( args ) + " failed with exception:" );
            ex.printStackTrace( stream );
        }

        void traceSuccess( PrintStream stream, Object[] args )
        {
            print( stream, call( args ) + " sucessfull." );
        }

        void traceSuccess( PrintStream stream, Object[] args, Object result )
        {
            print( stream, call( args ) + " sucessfully returned " + result
                + "." );
        }
    }

    /**
     * Generic call. Called for any method that is not handled explicitly by a
     * subclass.
     * @param <T>
     *            the return type of the invoked method.
     * @param call
     *            the marker identifying the event.
     * @param type
     *            the return type of the invoked method.
     * @param args
     *            the arguments to the method.
     * @return an object that handles the status of the event.
     */
    protected abstract <T> CallBack<T> call( Event call, Class<T> type,
        Object... args );

    /**
     * Create a tracer that simply prints all events to a stream.
     * @param stream
     *            The stream to print the events to.
     * @return A tracing inspector.
     */
    public static final Inspector trace( final PrintStream stream )
    {
        return new InspectionAdapter()
        {
            @Override
            protected <T> CallBack<T> call( final Event call,
                final Class<T> type, final Object... args )
            {
                if ( call == Event.COMMIT )
                {
                    StringBuffer buffer = new StringBuffer( "PREPARE_COMMIT(" );
                    appendArgs( buffer, args, Event.COMMIT.argNames );
                    buffer.append( "):" );
                    stream.println( buffer.toString() );
                }
                return new CallBack<T>()
                {
                    public void failure( Throwable ex )
                    {
                        call.traceFailure( stream, args, ex );
                    }

                    public void success( T result )
                    {
                        if ( call == Event.COMMIT )
                        {
                            stream.println( "    sucessfull." );
                        }
                        else if ( type.equals( RemoteResponse.class )
                            || type.equals( Void.class ) )
                        {
                            call.traceSuccess( stream, args );
                        }
                        else
                        {
                            call.traceSuccess( stream, args, result );
                        }
                    }
                };
            }
        };
    }

    private static void appendArgs( StringBuffer buffer, Object[] args,
        String[] argNames )
    {
        boolean addComma = false;
        for ( int i = 0; i < args.length; i++ )
        {
            Object arg = args[ i ];
            if ( addComma )
            {
                buffer.append( ", " );
            }
            if ( argNames != null && i < argNames.length )
            {
                buffer.append( argNames[ i ] );
                buffer.append( "=" );
            }
            if ( arg.getClass().isArray() )
            {
                buffer.append( "[" );
                appendArgs( buffer, ( Object[] ) arg, null );
                buffer.append( "]" );
            }
            else if ( arg instanceof String )
            {
                String string = ( String ) arg;
                buffer.append( '"' );
                buffer.append( string );
                buffer.append( '"' );
            }
            else
            {
                buffer.append( arg.toString() );
            }
            addComma = true;
        }
    }

    public CallBack<Void> open()
    {
        return call( Event.OPEN, Void.class );
    }

    public CallBack<Void> close()
    {
        return call( Event.CLOSE, Void.class );
    }

    public CallBack<Integer> beginTransaction()
    {
        return call( Event.BEGIN_TRANSACTION, Integer.class );
    }

    public CallBack<RemoteResponse> createNode( int transactionId )
    {
        return call( Event.CREATE_NODE, RemoteResponse.class, transactionId );
    }

    public CallBack<RemoteResponse> createRelationship( int transactionId,
        long startNodeId, long endNodeId, String relationshipTypeName )
    {
        return call( Event.CREATE_RELATIONSHIP, RemoteResponse.class,
            transactionId, startNodeId, endNodeId, relationshipTypeName );
    }

    public CallBack<RemoteResponse> fetchRelationship( int transactionId,
        long relationshipId )
    {
        return call( Event.FETCH_RELATIONSHIP, RemoteResponse.class,
            transactionId, relationshipId );
    }

    public CallBack<RemoteResponse> fetchNodeProperties( int transactionId,
        long nodeId )
    {
        return call( Event.FETCH_NODE_PROPERTIES, RemoteResponse.class,
            transactionId, nodeId );
    }

    public CallBack<RemoteResponse> fetchRelationshipProperties(
        int transactionId, long relationshipId )
    {
        return call( Event.FETCH_RELATIONSHIP_PROPERTIES, RemoteResponse.class,
            transactionId, relationshipId );
    }

    public CallBack<RemoteResponse> fetchRelationships( int transactionId,
        long rootNodeId, Direction direction, String[] typeNames )
    {
        return call( Event.FETCH_RELATIONSHIPS, RemoteResponse.class,
            transactionId, rootNodeId, direction, typeNames );
    }

    public CallBack<Void> commit( int transactionId )
    {
        return call( Event.COMMIT, Void.class, transactionId );
    }

    public CallBack<Void> rollback( int transactionId )
    {
        return call( Event.ROLLBACK, Void.class, transactionId );
    }

    public CallBack<Void> setNodeProperty( long id, String key, Object value )
    {
        return call( Event.SET_NODE_PROPERTY, Void.class, id, key, value );
    }

    public CallBack<Void> setRelationshipProperty( long id, String key,
        Object value )
    {
        return call( Event.SET_RELATIONSHIP_PROPERTY, Void.class, id, key,
            value );
    }

    public CallBack<Void> deleteNode( long nodeId )
    {
        return call( Event.DELETE_NODE, Void.class, nodeId );
    }

    public CallBack<Void> deleteRelationship( long relationshipId )
    {
        return call( Event.DELETE_RELATIONSHIP, Void.class, relationshipId );
    }
}
