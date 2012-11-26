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

package org.neo4j.cluster.member.paxos;

import static org.neo4j.helpers.collection.Iterables.append;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.toList;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Map;

import org.neo4j.cluster.Binding;
import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.snapshot.SnapshotProvider;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Paxos based implementation of {@link org.neo4j.cluster.member.ClusterMemberAvailability}
 */
public class PaxosClusterMemberAvailability implements ClusterMemberAvailability, Lifecycle
{
    private URI serverClusterId;
    private StringLogger logger;
    protected AtomicBroadcastSerializer serializer;
    private Binding binding;
    private AtomicBroadcast atomicBroadcast;
    private BindingListener bindingListener;

    public PaxosClusterMemberAvailability( Binding binding, AtomicBroadcast atomicBroadcast, StringLogger logger)
    {
        this.binding = binding;
        this.atomicBroadcast = atomicBroadcast;
        this.logger = logger;

        bindingListener = new BindingListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                serverClusterId = me;
                PaxosClusterMemberAvailability.this.logger.logMessage( "Listening at:" + me );
            }
        };
    }

    @Override
    public void init()
            throws Throwable
    {
        serializer = new AtomicBroadcastSerializer();

        binding.addBindingListener( bindingListener );
    }

    @Override
    public void start()
            throws Throwable
    {
    }

    @Override
    public void stop()
            throws Throwable
    {
    }

    @Override
    public void shutdown()
            throws Throwable
    {
        binding.removeBindingListener( bindingListener );
    }

    @Override
    public void memberIsAvailable( String role, URI roleUri )
    {
        try
        {
            Payload payload = serializer.broadcast( new MemberIsAvailable( role, serverClusterId, roleUri ) );
            serializer.receive( payload );
            atomicBroadcast.broadcast( payload );
        }
        catch ( Throwable e )
        {
            logger.warn( "Could not distribute member availability", e );
        }
    }

    @Override
    public void memberIsUnavailable( String role )
    {
        try
        {
            Payload payload = serializer.broadcast( new MemberIsUnavailable( role, serverClusterId ) );
            serializer.receive( payload );
            atomicBroadcast.broadcast( payload );
        }
        catch ( Throwable e )
        {
            logger.warn( "Could not distribute member unavailability", e );
        }
    }
}
