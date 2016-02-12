/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.ConfigurationContext;
import org.neo4j.cluster.protocol.LoggingContext;
import org.neo4j.cluster.protocol.TimeoutsContext;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.helpers.collection.Iterables.toList;

class AbstractContextImpl
        implements TimeoutsContext, LoggingContext, ConfigurationContext
{
    protected final org.neo4j.cluster.InstanceId me;
    protected final CommonContextState commonState;
    protected final Logging logging;
    protected final Timeouts timeouts;

    AbstractContextImpl( org.neo4j.cluster.InstanceId me, CommonContextState commonState,
                         Logging logging,
                         Timeouts timeouts )
    {
        this.me = me;
        this.commonState = commonState;
        this.logging = logging;
        this.timeouts = timeouts;
    }

    @Override
    public StringLogger getLogger( Class loggingClass )
    {
        return logging.getMessagesLog( loggingClass );
    }

    @Override
    public ConsoleLogger getConsoleLogger( Class loggingClass )
    {
        return logging.getConsoleLog( loggingClass );
    }

    // TimeoutsContext
    @Override
    public void setTimeout( Object key, Message<? extends MessageType> timeoutMessage )
    {
        timeouts.setTimeout( key, timeoutMessage );
    }

    @Override
    public void cancelTimeout( Object key )
    {
        timeouts.cancelTimeout( key );
    }

    // ConfigurationContext
    @Override
    public List<URI> getMemberURIs()
    {
        return toList( commonState.configuration().getMemberURIs() );
    }

    @Override
    public org.neo4j.cluster.InstanceId getMyId()
    {
        return me;
    }

    @Override
    public URI boundAt()
    {
        return commonState.boundAt();
    }

    @Override
    public List<URI> getAcceptors()
    {
        return commonState.configuration().getMemberURIs();
    }

    @Override
    public Map<org.neo4j.cluster.InstanceId, URI> getMembers()
    {
        return commonState.configuration().getMembers();
    }

    @Override
    public org.neo4j.cluster.InstanceId getCoordinator()
    {
        return commonState.configuration().getElected( ClusterConfiguration.COORDINATOR );
    }

    @Override
    public URI getUriForId( org.neo4j.cluster.InstanceId node )
    {
        return commonState.configuration().getUriForId( node );
    }

    @Override
    public org.neo4j.cluster.InstanceId getIdForUri( URI uri )
    {
        return commonState.configuration().getIdForUri( uri );
    }

    @Override
    public synchronized boolean isMe( org.neo4j.cluster.InstanceId server )
    {
        return me.equals( server );
    }
}
