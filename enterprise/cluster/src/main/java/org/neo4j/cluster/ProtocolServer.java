/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.cluster.com.BindingNotifier;
import org.neo4j.cluster.statemachine.StateMachine;
import org.neo4j.cluster.statemachine.StateMachineConversations;
import org.neo4j.cluster.statemachine.StateMachineProxyFactory;
import org.neo4j.cluster.statemachine.StateTransitionListener;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.Listeners;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * A ProtocolServer ties together the underlying StateMachines with an understanding of ones
 * own server address (me), and provides a proxy factory for creating clients to invoke the CSM.
 */
public class ProtocolServer
        extends LifecycleAdapter
        implements BindingNotifier
{
    private final InstanceId me;
    private URI boundAt;
    protected StateMachineProxyFactory proxyFactory;
    protected final StateMachines stateMachines;
    private Iterable<BindingListener> bindingListeners = Listeners.newListeners();
    private Log msgLog;

    public ProtocolServer( InstanceId me, StateMachines stateMachines, LogProvider logProvider )
    {
        this.me = me;
        this.stateMachines = stateMachines;
        this.msgLog = logProvider.getLog( getClass() );

        StateMachineConversations conversations = new StateMachineConversations(me);
        proxyFactory = new StateMachineProxyFactory( stateMachines, conversations, me );
        stateMachines.addMessageProcessor( proxyFactory );
    }

    @Override
    public void shutdown() throws Throwable
    {
        msgLog = null;
    }

    @Override
    public void addBindingListener( BindingListener listener )
    {
        bindingListeners = Listeners.addListener( listener, bindingListeners );
        try
        {
            if ( boundAt != null )
            {
                listener.listeningAt( boundAt );
            }
        }
        catch ( Throwable t )
        {
            msgLog.error( "Failed while adding BindingListener", t );
        }
    }

    @Override
    public void removeBindingListener( BindingListener listener )
    {
        bindingListeners = Listeners.removeListener( listener, bindingListeners );
    }

    public void listeningAt( final URI me )
    {
        this.boundAt = me;

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
    public InstanceId getServerId()
    {
        return me;
    }

    public StateMachines getStateMachines()
    {
        return stateMachines;
    }

    public void addStateTransitionListener( StateTransitionListener stateTransitionListener )
    {
        stateMachines.addStateTransitionListener( stateTransitionListener );
    }

    public <T> T newClient( Class<T> clientProxyInterface )
    {
        return proxyFactory.newProxy( clientProxyInterface );
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append( "Instance URI: " ).append( boundAt.toString() ).append( "\n" );
        for( StateMachine stateMachine : stateMachines.getStateMachines() )
        {
            builder.append( "  " ).append( stateMachine ).append( "\n" );
        }
        return builder.toString();
    }

    public Timeouts getTimeouts()
    {
        return stateMachines.getTimeouts();
    }

    public URI boundAt()
    {
        return boundAt;
    }
}
