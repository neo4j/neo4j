/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

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
    private final Listeners<BindingListener> bindingListeners = new Listeners<>();
    private Log msgLog;

    public ProtocolServer( InstanceId me, StateMachines stateMachines, LogProvider logProvider )
    {
        this.me = me;
        this.stateMachines = stateMachines;
        this.msgLog = logProvider.getLog( getClass() );

        StateMachineConversations conversations = new StateMachineConversations( me );
        proxyFactory = new StateMachineProxyFactory( stateMachines, conversations, me, logProvider );
        stateMachines.addMessageProcessor( proxyFactory );
    }

    @Override
    public void shutdown()
    {
        msgLog = null;
    }

    @Override
    public void addBindingListener( BindingListener listener )
    {
        bindingListeners.add( listener );
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
        bindingListeners.remove( listener );
    }

    public void listeningAt( URI me )
    {
        this.boundAt = me;

        bindingListeners.notify( listener -> listener.listeningAt( me ) );
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
        for ( StateMachine stateMachine : stateMachines.getStateMachines() )
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
