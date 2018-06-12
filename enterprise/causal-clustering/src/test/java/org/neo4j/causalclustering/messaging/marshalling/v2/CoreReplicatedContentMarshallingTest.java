/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.neo4j.causalclustering.messaging.marshalling.CoreReplicatedContentMarshal;

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
        ChannelMarshal<ReplicatedContent> coreReplicatedContentSerializer = new CoreReplicatedContentMarshal();
        NetworkFlushableChannelNetty4 channel = new NetworkFlushableChannelNetty4( buffer );
        coreReplicatedContentSerializer.marshal( replicatedContent, channel );

        NetworkReadableClosableChannelNetty4 readChannel = new NetworkReadableClosableChannelNetty4( buffer );
        ReplicatedContent result = coreReplicatedContentSerializer.unmarshal( readChannel );

        assertEquals( replicatedContent, result );
    }
}
