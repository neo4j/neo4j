/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.ConfigurationContext;
import org.neo4j.cluster.protocol.TimeoutsContext;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.helpers.collection.Iterables.limit;
import static org.neo4j.helpers.collection.Iterables.toList;

/**
 * This serves as a base class for contexts of distributed state machines, and holds
 * various generally useful information, and provides access to logging.
 */
class AbstractContextImpl
        implements TimeoutsContext, LogService, ConfigurationContext
{
    protected final org.neo4j.cluster.InstanceId me;
    protected final CommonContextState commonState;
    protected final LogService logService;
    protected final Timeouts timeouts;

    AbstractContextImpl( org.neo4j.cluster.InstanceId me, CommonContextState commonState,
                         LogService logService,
                         Timeouts timeouts )
    {
        this.me = me;
        this.commonState = commonState;
        this.logService = logService;
        this.timeouts = timeouts;
    }

    @Override
    public LogProvider getUserLogProvider()
    {
        return logService.getUserLogProvider();
    }

    @Override
    public Log getUserLog( Class loggingClass )
    {
        return logService.getUserLog( loggingClass );
    }

    @Override
    public LogProvider getInternalLogProvider()
    {
        return logService.getInternalLogProvider();
    }

    @Override
    public Log getInternalLog( Class loggingClass )
    {
        return logService.getInternalLog( loggingClass );
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
        // Only use 2f+1 acceptors
        return toList( limit( commonState.configuration()
                .getAllowedFailures() * 2 + 1, commonState.configuration().getMemberURIs() ) );
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
    public URI getUriForId(InstanceId node )
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
