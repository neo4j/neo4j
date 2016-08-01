/*
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
package org.neo4j.coreedge.core.consensus.log;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import org.neo4j.coreedge.messaging.NetworkFlushableByteBuf;
import org.neo4j.coreedge.core.consensus.membership.MemberIdSet;
import org.neo4j.coreedge.messaging.CoreReplicatedContentMarshal;
import org.neo4j.coreedge.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.coreedge.core.replication.ReplicatedContent;
import org.neo4j.coreedge.core.state.machines.id.ReplicatedIdAllocationRequest;
import org.neo4j.coreedge.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.coreedge.core.state.machines.tx.ReplicatedTransactionFactory;
import org.neo4j.coreedge.messaging.EndOfStreamException;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.storageengine.api.StorageCommand;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class RaftContentByteBufferMarshalTest
{
    private MemberId memberId = new MemberId( UUID.randomUUID() );

    @Test
    public void shouldSerializeMemberSet() throws Exception
    {
        // given
        CoreReplicatedContentMarshal serializer = new CoreReplicatedContentMarshal();
        MemberIdSet in = new MemberIdSet( asSet(
                new MemberId( UUID.randomUUID() ),
                new MemberId( UUID.randomUUID() )
        ) );

        // when
        ByteBuf buf = Unpooled.buffer();
        assertMarshalingEquality( serializer, buf, in );
    }

    @Test
    public void shouldSerializeTransactionRepresentation() throws Exception
    {
        // given
        CoreReplicatedContentMarshal serializer = new CoreReplicatedContentMarshal();
        Collection<StorageCommand> commands = new ArrayList<>();

        IndexCommand.AddNodeCommand addNodeCommand = new IndexCommand.AddNodeCommand();
        addNodeCommand.init( 0, 0, 0, 0 );

        commands.add( addNodeCommand );

        byte[] extraHeader = new byte[0];

        PhysicalTransactionRepresentation txIn = new PhysicalTransactionRepresentation( commands );
        txIn.setHeader( extraHeader, -1, -1, 0, 0, 0, 0 );
        ReplicatedTransaction in = ReplicatedTransactionFactory.createImmutableReplicatedTransaction( txIn );

        // when
        ByteBuf buf = Unpooled.buffer();
        serializer.marshal( in, new NetworkFlushableByteBuf( buf ) );
        ReplicatedTransaction out =
                (ReplicatedTransaction) serializer.unmarshal( new NetworkReadableClosableChannelNetty4( buf ) );

        TransactionRepresentation txOut = ReplicatedTransactionFactory.extractTransactionRepresentation( out,
                extraHeader );

        // then
        assertEquals( in, out );
        assertEquals( txIn, txOut );
    }

    @Test
    public void txSerializationShouldNotResultInExcessZeroes() throws Exception
    {
        /*
         * This test ensures that the buffer used to serialize a transaction and then extract the byte array for
         * sending over the wire is trimmed properly. Not doing so will result in sending too many trailing garbage
         * (zeroes) that will be ignored from the other side, as zeros are interpreted as null entries from the
         * LogEntryReader and stop the deserialization process.
         * The test creates a single transaction which has just a header, no commands. That should amount to 40 bytes
         * as ReplicatedTransactionFactory.TransactionSerializer.write() makes it out at the time of this writing. If
         * that code changes, this test will break.
         */
        byte[] extraHeader = new byte[0];

        PhysicalTransactionRepresentation txIn = new PhysicalTransactionRepresentation( new ArrayList<>() );
        txIn.setHeader( extraHeader, -1, -1, 0, 0, 0, 0 );

        // when
        ReplicatedTransaction in = ReplicatedTransactionFactory.createImmutableReplicatedTransaction( txIn );

        // then
        assertEquals( 40, in.getTxBytes().length );
    }

    @Test
    public void shouldSerializeIdRangeRequest() throws Exception
    {
        // given
        CoreReplicatedContentMarshal serializer = new CoreReplicatedContentMarshal();
        ReplicatedContent in = new ReplicatedIdAllocationRequest( memberId, IdType.NODE, 100, 200 );

        // when
        ByteBuf buf = Unpooled.buffer();
        assertMarshalingEquality( serializer, buf, in );
    }

    private void assertMarshalingEquality( CoreReplicatedContentMarshal marshal,
                                           ByteBuf buffer,
                                           ReplicatedContent replicatedTx ) throws IOException, EndOfStreamException
    {
        marshal.marshal( replicatedTx, new NetworkFlushableByteBuf( buffer ) );

        assertThat( marshal.unmarshal( new NetworkReadableClosableChannelNetty4( buffer ) ), equalTo( replicatedTx ) );
    }
}
