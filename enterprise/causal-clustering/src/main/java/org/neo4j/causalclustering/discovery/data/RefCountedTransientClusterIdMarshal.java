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
package org.neo4j.causalclustering.discovery.data;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.discovery.TransientClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class RefCountedTransientClusterIdMarshal extends SafeChannelMarshal<RefCounted<TransientClusterId>>
{

    public static final RefCountedTransientClusterIdMarshal INSTANCE = new RefCountedTransientClusterIdMarshal();
    private static final TransientClusterId.Marshal clusterIdMarshal = TransientClusterId.Marshal.INSTANCE;
    private static final MemberId.Marshal memberIdMarshal = MemberId.Marshal.INSTANCE;

    @Override
    protected RefCounted<TransientClusterId> unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        TransientClusterId clusterId = clusterIdMarshal.unmarshal( channel );
        int numReferences = channel.getInt();
        Set<MemberId> references = new HashSet<>();
        for ( int i = 0; i < numReferences; i++ )
        {
            references.add( memberIdMarshal.unmarshal( channel ) );
        }
        return new RefCounted<>( clusterId, references );
    }

    @Override
    public void marshal( RefCounted<TransientClusterId> ref, WritableChannel channel ) throws IOException
    {
        clusterIdMarshal.marshal( ref.value(), channel );
        channel.putInt( ref.references().size() );
        for( MemberId m : ref.references() )
        {
            memberIdMarshal.marshal( m, channel );
        }
    }
}
