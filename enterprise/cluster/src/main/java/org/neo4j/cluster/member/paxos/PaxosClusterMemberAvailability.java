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

        bindingListener = me ->
        {
            serverClusterId = me;
            PaxosClusterMemberAvailability.this.log.info( "Listening at:" + me );
        };
    }

    @Override
    public void init()
    {
        serializer = new AtomicBroadcastSerializer( objectInputStreamFactory, objectOutputStreamFactory );

        binding.addBindingListener( bindingListener );
    }

    @Override
    public void start()
    {
    }

    @Override
    public void stop()
    {
    }

    @Override
    public void shutdown()
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
