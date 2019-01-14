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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.ConfigurationContext;
import org.neo4j.cluster.protocol.LoggingContext;
import org.neo4j.cluster.protocol.TimeoutsContext;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.helpers.collection.Iterables.asList;

/**
 * This serves as a base class for contexts of distributed state machines, and holds
 * various generally useful information, and provides access to logging.
 */
class AbstractContextImpl
        implements TimeoutsContext, LoggingContext, ConfigurationContext
{
    protected final org.neo4j.cluster.InstanceId me;
    protected final CommonContextState commonState;
    protected final LogProvider logProvider;
    protected final Timeouts timeouts;

    AbstractContextImpl( InstanceId me, CommonContextState commonState,
            LogProvider logProvider, Timeouts timeouts )
    {
        this.me = me;
        this.commonState = commonState;
        this.logProvider = logProvider;
        this.timeouts = timeouts;
    }

    // LoggingContext
    @Override
    public Log getLog( Class loggingClass )
    {
        return logProvider.getLog( loggingClass );
    }

    // TimeoutsContext
    @Override
    public void setTimeout( Object key, Message<? extends MessageType> timeoutMessage )
    {
        timeouts.setTimeout( key, timeoutMessage );
    }

    @Override
    public long getTimeoutFor( Message<? extends MessageType> timeoutMessage )
    {
        return timeouts.getTimeoutFor( timeoutMessage );
    }

    @Override
    public Message<? extends MessageType> cancelTimeout( Object key )
    {
        return timeouts.cancelTimeout( key );
    }

    // ConfigurationContext
    @Override
    public List<URI> getMemberURIs()
    {
        return asList( commonState.configuration().getMemberURIs() );
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
    public Map<InstanceId, URI> getMembers()
    {
        return commonState.configuration().getMembers();
    }

    @Override
    public InstanceId getCoordinator()
    {
        return commonState.configuration().getElected( ClusterConfiguration.COORDINATOR );
    }

    @Override
    public URI getUriForId( InstanceId node )
    {
        return commonState.configuration().getUriForId( node );
    }

    @Override
    public InstanceId getIdForUri( URI uri )
    {
        return commonState.configuration().getIdForUri( uri );
    }

    @Override
    public synchronized boolean isMe( InstanceId server )
    {
        return me.equals( server );
    }
}
