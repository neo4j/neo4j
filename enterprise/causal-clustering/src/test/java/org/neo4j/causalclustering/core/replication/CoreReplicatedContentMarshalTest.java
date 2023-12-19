/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.replication;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

import org.neo4j.causalclustering.messaging.NetworkFlushableByteBuf;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import org.neo4j.causalclustering.messaging.CoreReplicatedContentMarshal;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequestSerializer;
import org.neo4j.causalclustering.core.state.machines.token.TokenType;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionFactory;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.storageengine.api.StorageCommand;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class CoreReplicatedContentMarshalTest
{
    private final ChannelMarshal<ReplicatedContent> marshal = new CoreReplicatedContentMarshal();

    @Test
    public void shouldMarshalTransactionReference() throws Exception
    {
        ByteBuf buffer = Unpooled.buffer();
        PhysicalTransactionRepresentation representation =
                new PhysicalTransactionRepresentation( Collections.emptyList() );
        representation.setHeader( new byte[]{0}, 1, 1, 1, 1, 1, 1 );

        ReplicatedContent replicatedTx =
                ReplicatedTransactionFactory.createImmutableReplicatedTransaction( representation );

        assertMarshalingEquality( buffer, replicatedTx );
    }

    @Test
    public void shouldMarshalTransactionReferenceWithMissingHeader() throws Exception
    {
        ByteBuf buffer = Unpooled.buffer();
        PhysicalTransactionRepresentation representation =
                new PhysicalTransactionRepresentation( Collections.emptyList() );

        ReplicatedContent replicatedTx =
                ReplicatedTransactionFactory.createImmutableReplicatedTransaction( representation );

        assertMarshalingEquality( buffer, replicatedTx );
    }

    @Test
    public void shouldMarshalMemberSet() throws Exception
    {
        ByteBuf buffer = Unpooled.buffer();
        ReplicatedContent message = new MemberIdSet( asSet(
                new MemberId( UUID.randomUUID() ),
                new MemberId( UUID.randomUUID() )
        ) );

        assertMarshalingEquality( buffer, message );
    }

    @Test
    public void shouldMarshalIdRangeRequest() throws Exception
    {
        ByteBuf buffer = Unpooled.buffer();
        ReplicatedContent message = new ReplicatedIdAllocationRequest(
                new MemberId( UUID.randomUUID() ), IdType.PROPERTY, 100, 200 );

        assertMarshalingEquality( buffer, message );
    }

    @Test
    public void shouldMarshalTokenRequest() throws Exception
    {
        ByteBuf buffer = Unpooled.buffer();

        ArrayList<StorageCommand> commands = new ArrayList<>();
        LabelTokenRecord before = new LabelTokenRecord( 0 );
        LabelTokenRecord after = new LabelTokenRecord( 0 );
        after.setInUse( true );
        after.setCreated();
        after.setNameId( 3232 );
        commands.add( new Command.LabelTokenCommand( before, after ) );
        ReplicatedContent message = new ReplicatedTokenRequest( TokenType.LABEL, "theLabel",
                ReplicatedTokenRequestSerializer.commandBytes( commands ) );

        assertMarshalingEquality( buffer, message );
    }

    private void assertMarshalingEquality( ByteBuf buffer, ReplicatedContent replicatedTx ) throws IOException, EndOfStreamException
    {
        marshal.marshal( replicatedTx, new NetworkFlushableByteBuf( buffer ) );

        assertThat( marshal.unmarshal( new NetworkReadableClosableChannelNetty4( buffer ) ), equalTo( replicatedTx ) );
    }
}
