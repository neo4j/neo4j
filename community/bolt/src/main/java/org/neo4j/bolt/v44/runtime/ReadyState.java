/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.v44.runtime;

import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.routing.RoutingTableGetter;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineState;
import org.neo4j.bolt.runtime.statemachine.StateMachineContext;
import org.neo4j.bolt.security.auth.AuthenticationException;
import org.neo4j.bolt.v44.messaging.request.BeginMessage;
import org.neo4j.bolt.v44.messaging.request.RouteMessage;
import org.neo4j.bolt.v44.messaging.request.RunMessage;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_NAME;

public class ReadyState extends org.neo4j.bolt.v43.runtime.ReadyState
{
    public ReadyState( RoutingTableGetter routingTableGetter )
    {
        super( routingTableGetter );
    }

    @Override
    public BoltStateMachineState processUnsafe( RequestMessage message, StateMachineContext context ) throws Exception
    {
        if ( message instanceof RouteMessage || message instanceof RunMessage || message instanceof BeginMessage )
        {
            return super.processUnsafe( message, context );
        }

        return null;
    }

    @Override
    protected BoltStateMachineState processRouteMessage( org.neo4j.bolt.v43.messaging.request.RouteMessage message, StateMachineContext context )
            throws Exception
    {
        var routeMessage = (RouteMessage) message;
        context.impersonateUser( this.authenticateImpersonation( context, routeMessage.impersonatedUser() ) );

        try
        {
            return super.processRouteMessage( message, context );
        }
        finally
        {
            context.impersonateUser( null );
        }
    }

    @Override
    protected void onRoutingTableReceived( StateMachineContext context, org.neo4j.bolt.v43.messaging.request.RouteMessage message, MapValue routingTable )
    {
        var databaseName = message.getDatabaseName();
        if ( databaseName == null || ABSENT_DB_NAME.equals( message.getDatabaseName() ) )
        {
            databaseName = context.getDefaultDatabase();
        }

        super.onRoutingTableReceived( context, message, routingTable
                .updatedWith( "db", Values.stringValue( databaseName ) ) );
    }

    @Override
    protected BoltStateMachineState processRunMessage( org.neo4j.bolt.v3.messaging.request.RunMessage message, StateMachineContext context ) throws Exception
    {
        var runMessage = (RunMessage) message;

        context.impersonateUser( this.authenticateImpersonation( context, runMessage.impersonatedUser() ) );

        try
        {
            return super.processRunMessage( message, context );
        }
        finally
        {
            context.impersonateUser( null );
        }
    }

    @Override
    protected BoltStateMachineState processBeginMessage( org.neo4j.bolt.v3.messaging.request.BeginMessage message, StateMachineContext context )
            throws Exception
    {
        var beginMessage = (BeginMessage) message;

        context.impersonateUser( this.authenticateImpersonation( context, beginMessage.impersonatedUser() ) );

        return super.processBeginMessage( message, context );
    }

    /**
     * Authenticates the impersonation of a given target user and returns the associated login context which is used as a substitute for following operations.
     *
     * @param context  the state machine context.
     * @param username the desired target user.
     * @return a substitute login context.
     */
    private LoginContext authenticateImpersonation( StateMachineContext context, String username ) throws AuthenticationException
    {
        if ( username != null )
        {
            return context.boltSpi().impersonate( context.getLoginContext(), username );
        }

        return null;
    }
}
