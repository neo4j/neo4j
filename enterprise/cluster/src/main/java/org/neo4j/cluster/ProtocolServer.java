/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

package org.neo4j.cluster;

import java.net.URI;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageProcessor;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.statemachine.StateMachine;
import org.neo4j.cluster.statemachine.StateMachineConversations;
import org.neo4j.cluster.statemachine.StateMachineProxyFactory;
import org.neo4j.cluster.statemachine.StateTransitionListener;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * A ProtocolServer ties together the underlying ConnectedStateMachines with an understanding of ones
 * own server address (me), and provides a proxy factory for creating clients to invoke the CSM.
 */
public class ProtocolServer
{
    private URI me;
    protected StateMachineProxyFactory proxyFactory;
    protected final ConnectedStateMachines connectedStateMachines;
    private Iterable<BindingListener> bindingListeners = Listeners.newListeners();
    private final StringLogger msgLog;

    public ProtocolServer( ConnectedStateMachines connectedStateMachines, StringLogger msgLog )
    {
        this.connectedStateMachines = connectedStateMachines;
        this.msgLog = msgLog;

        FromHeaderMessageProcessor fromHeaderMessageProcessor = new FromHeaderMessageProcessor();
        addBindingListener( fromHeaderMessageProcessor );
        connectedStateMachines.addMessageProcessor( fromHeaderMessageProcessor );

        StateMachineConversations conversations = new StateMachineConversations();
        proxyFactory = new StateMachineProxyFactory( connectedStateMachines, conversations );
        connectedStateMachines.addMessageProcessor( proxyFactory );

        addBindingListener( conversations );
        addBindingListener( proxyFactory );
    }

    public void addBindingListener( BindingListener listener )
    {
        bindingListeners = Listeners.addListener( listener, bindingListeners );
        try
        {
            if ( me != null )
            {
                listener.listeningAt( me );
            }
        }
        catch ( Throwable t )
        {
            msgLog.logMessage( "Failed while adding BindingListener", t );
        }
    }

    public void removeBindingListener( BindingListener listener )
    {
        bindingListeners = Listeners.removeListener( listener, bindingListeners );
    }

    public void listeningAt( final URI me )
    {
        this.me = me;

        Listeners.notifyListeners( bindingListeners, new Listeners.Notification<BindingListener>()
        {
            @Override
            public void notify( BindingListener listener )
            {
                listener.listeningAt( me );
            }
        } );
    }

    /**
     * Ok to have this accessible like this?
     *
     * @return server id
     */
    public URI getServerId()
    {
        return me;
    }

    public ConnectedStateMachines getConnectedStateMachines()
    {
        return connectedStateMachines;
    }

    public void addStateTransitionListener( StateTransitionListener stateTransitionListener )
    {
        connectedStateMachines.addStateTransitionListener( stateTransitionListener );
    }

    public void removeStateTransitionListener( StateTransitionListener stateTransitionListener )
    {
        connectedStateMachines.removeStateTransitionListener( stateTransitionListener );
    }

    public <T> T newClient( Class<T> clientProxyInterface )
    {
        return proxyFactory.newProxy( clientProxyInterface );
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("Instance URI:")).append(me.toString()).append("\n");
        for( StateMachine stateMachine : connectedStateMachines.getStateMachines() )
        {
            builder.append( "  "+stateMachine).append("\n");
        }
        return builder.toString();
    }

    public Timeouts getTimeouts()
    {
        return connectedStateMachines.getTimeouts();
    }

    private class FromHeaderMessageProcessor
            implements MessageProcessor, BindingListener
    {
        private String me;

        @Override
        public void listeningAt( URI me )
        {
            this.me = me.toString();
        }

        @Override
        public void process( Message<? extends MessageType> message )
        {
            if ( message.hasHeader( Message.TO ) && me != null )
            {
                message.setHeader( Message.FROM, me );
            }
        }
    }
}
