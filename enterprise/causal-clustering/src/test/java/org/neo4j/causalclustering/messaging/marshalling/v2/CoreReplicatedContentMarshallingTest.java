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
package org.neo4j.causalclustering.messaging.marshalling.v2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashSet;
import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import org.neo4j.causalclustering.core.replication.DistributedOperation;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.LocalOperationId;
import org.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.TokenType;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.NetworkFlushableChannelNetty4;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;

import static org.junit.Assert.assertEquals;

@RunWith( Parameterized.class )
public class CoreReplicatedContentMarshallingTest
{
    @Parameterized.Parameter()
    public ReplicatedContent replicatedContent;
    private ByteBuf buffer;

    @Parameterized.Parameters( name = "{0}" )
    public static ReplicatedContent[] data()
    {
        return new ReplicatedContent[]{
                new DummyRequest( new byte[]{1, 2, 3} ), new ReplicatedTransaction( new byte[]{'a', 3, 'b'} ),
                new MemberIdSet( new HashSet<MemberId>()
                {{
                    add( new MemberId( UUID.randomUUID() ) );
                }} ), new ReplicatedTokenRequest( TokenType.LABEL, "token", new byte[]{'c', 'o', 5} ), new NewLeaderBarrier(),
                new ReplicatedLockTokenRequest( new MemberId( UUID.randomUUID() ), 2 ), new DistributedOperation(
                new DistributedOperation( new ReplicatedTransaction( new byte[]{1, 2, 3, 4, 5, 6} ),
                        new GlobalSession( UUID.randomUUID(), new MemberId( UUID.randomUUID() ) ), new LocalOperationId( 1, 2 ) ),
                new GlobalSession( UUID.randomUUID(), new MemberId( UUID.randomUUID() ) ), new LocalOperationId( 4, 5 ) )};
    }

    @Before
    public void setUpBuffer()
    {
        buffer = Unpooled.buffer();
    }

    @After
    public void releaseBuffer()
    {
        if ( buffer != null )
        {
            ReferenceCountUtil.release( buffer );
        }
    }

    @Test
    public void shouldSerializeAndDeserialize() throws Exception
    {
        ChannelMarshal<ReplicatedContent> coreReplicatedContentSerializer = new CoreReplicatedContentSerializer();
        NetworkFlushableChannelNetty4 channel = new NetworkFlushableChannelNetty4( buffer );
        coreReplicatedContentSerializer.marshal( replicatedContent, channel );

        NetworkReadableClosableChannelNetty4 readChannel = new NetworkReadableClosableChannelNetty4( buffer );
        ReplicatedContent result = coreReplicatedContentSerializer.unmarshal( readChannel );

        assertEquals( replicatedContent, result );
    }
}
