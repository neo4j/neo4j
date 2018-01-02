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
package org.neo4j.cluster.member.paxos;

import java.net.URI;

import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.BindingNotifier;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Paxos based implementation of {@link org.neo4j.cluster.member.ClusterMemberAvailability}
 */
public class PaxosClusterMemberAvailability implements ClusterMemberAvailability, Lifecycle
{
    private volatile URI serverClusterId;
    private Log log;
    protected AtomicBroadcastSerializer serializer;
    private final InstanceId myId;
    private BindingNotifier binding;
    private AtomicBroadcast atomicBroadcast;
    private BindingListener bindingListener;
    private ObjectInputStreamFactory objectInputStreamFactory;
    private ObjectOutputStreamFactory objectOutputStreamFactory;

    public PaxosClusterMemberAvailability( InstanceId myId, BindingNotifier binding, AtomicBroadcast atomicBroadcast,
                                           LogProvider logProvider, ObjectInputStreamFactory objectInputStreamFactory,
                                           ObjectOutputStreamFactory objectOutputStreamFactory )
    {
        this.myId = myId;
        this.binding = binding;
        this.atomicBroadcast = atomicBroadcast;
        this.objectInputStreamFactory = objectInputStreamFactory;
        this.objectOutputStreamFactory = objectOutputStreamFactory;
        this.log = logProvider.getLog( getClass() );

        bindingListener = new BindingListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                serverClusterId = me;
                PaxosClusterMemberAvailability.this.log.info( "Listening at:" + me );
            }
        };
    }

    @Override
    public void init()
            throws Throwable
    {
        serializer = new AtomicBroadcastSerializer( objectInputStreamFactory, objectOutputStreamFactory );

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
    public void memberIsAvailable( String role, URI roleUri, StoreId storeId )
    {
        try
        {
            MemberIsAvailable message = new MemberIsAvailable( role, myId, serverClusterId, roleUri, storeId );
            Payload payload = serializer.broadcast( message );
            atomicBroadcast.broadcast( payload );
        }
        catch ( Throwable e )
        {
            log.warn( "Could not distribute member availability", e );
        }
    }

    @Override
    public void memberIsUnavailable( String role )
    {
        try
        {
            MemberIsUnavailable message = new MemberIsUnavailable( role, myId, serverClusterId );
            Payload payload = serializer.broadcast( message );
            atomicBroadcast.broadcast( payload );
        }
        catch ( Throwable e )
        {
            log.warn( "Could not distribute member unavailability", e );
        }
    }
}
