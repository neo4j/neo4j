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
package org.neo4j.coreedge.raft.replication;

import java.util.ArrayList;
import java.util.Collections;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import org.neo4j.coreedge.raft.membership.CoreMemberSet;
import org.neo4j.coreedge.raft.net.CoreReplicatedContentMarshal;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdAllocationRequest;
import org.neo4j.coreedge.raft.replication.token.ReplicatedTokenRequest;
import org.neo4j.coreedge.raft.replication.token.ReplicatedTokenRequestSerializer;
import org.neo4j.coreedge.raft.replication.token.TokenType;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionFactory;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.ByteBufMarshal;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.storageengine.api.StorageCommand;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.neo4j.helpers.collection.Iterators.asSet;

public class CoreReplicatedContentByteBufferMarshalTest
{
    private final ByteBufMarshal<ReplicatedContent> marshal = new CoreReplicatedContentMarshal();

    @Test
    public void shouldMarshalTransactionReference() throws Exception
    {
        ByteBuf buffer = Unpooled.buffer();
        PhysicalTransactionRepresentation representation = new PhysicalTransactionRepresentation( Collections.emptyList() );
        representation.setHeader( new byte[]{0}, 1, 1, 1, 1, 1, 1 );

        ReplicatedContent replicatedTx = ReplicatedTransactionFactory.createImmutableReplicatedTransaction( representation );

        marshal.marshal( replicatedTx, buffer );

        assertThat( marshal.unmarshal( buffer ), equalTo( replicatedTx ) );
    }

    @Test
    public void shouldMarshalTransactionReferenceWithMissingHeader() throws Exception
    {
        ByteBuf buffer = Unpooled.buffer();
        PhysicalTransactionRepresentation representation = new PhysicalTransactionRepresentation( Collections.emptyList() );

        ReplicatedContent replicatedTx = ReplicatedTransactionFactory.createImmutableReplicatedTransaction( representation );
        marshal.marshal( replicatedTx, buffer );

        assertThat( marshal.unmarshal( buffer ), equalTo( replicatedTx ) );
    }

    @Test
    public void shouldMarshalMemberSet() throws Exception
    {
        // given
        ByteBuf buffer = Unpooled.buffer();
        ReplicatedContent message = new CoreMemberSet( asSet(
                new CoreMember( new AdvertisedSocketAddress( "host_a:1" ), new AdvertisedSocketAddress( "host_a:2" )),
                new CoreMember( new AdvertisedSocketAddress( "host_b:101" ),
                        new AdvertisedSocketAddress( "host_b:102" ) )
        ) );

        // when
        marshal.marshal( message, buffer );

        // then
        assertThat( marshal.unmarshal( buffer ), equalTo( message ) );
    }

    @Test
    public void shouldMarshalIdRangeRequest() throws Exception
    {
        // given
        ByteBuf buffer = Unpooled.buffer();
        ReplicatedContent message = new ReplicatedIdAllocationRequest(
                new CoreMember(
                        new AdvertisedSocketAddress( "host_a:1" ),
                        new AdvertisedSocketAddress( "host_a:2" )
                ), IdType.PROPERTY, 100, 200 );

        // when
        marshal.marshal( message, buffer );

        // then
        assertThat( marshal.unmarshal( buffer ), equalTo( message ) );
    }

    @Test
    public void shouldMarshalTokenRequest() throws Exception
    {
        // given
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

        // when
        marshal.marshal( message, buffer );

        // then
        assertThat( marshal.unmarshal( buffer ), equalTo( message ) );
    }
}
