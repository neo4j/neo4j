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
package org.neo4j.coreedge.raft.log;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import org.junit.Test;

import org.neo4j.coreedge.raft.membership.CoreMemberSet;
import org.neo4j.coreedge.raft.replication.RaftContentSerializer;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.raft.replication.storeid.SeedStoreId;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdAllocationRequest;
import org.neo4j.coreedge.raft.replication.session.GlobalSession;
import org.neo4j.coreedge.raft.replication.session.LocalOperationId;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransaction;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionFactory;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;

import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.coreedge.server.AdvertisedSocketAddress.address;

public class RaftContentSerializerTest
{
    CoreMember coreMember = new CoreMember( address( "core:1" ), address( "raft:1" ) );
    GlobalSession globalSession = new GlobalSession( UUID.randomUUID(), coreMember );

    @Test
    public void shouldSerializeMemberSet() throws Exception
    {
        // given
        RaftContentSerializer serializer = new RaftContentSerializer();
        CoreMemberSet in = new CoreMemberSet( asSet(
                new CoreMember( address( "host1:1001" ), address( "host1:1002" ) ),
                new CoreMember( address( "host2:1002" ), address( "host2:1002" ) )
        ) );

        // when
        ByteBuffer buffer = serializer.serialize( in );
        ReplicatedContent out = serializer.deserialize( buffer );

        // then
        assertEquals( in, out );
    }

    @Test
    public void shouldSerializeSeedStoreId() throws Exception
    {
        // given
        RaftContentSerializer serializer = new RaftContentSerializer();
        SeedStoreId in = new SeedStoreId( new StoreId() );

        // when
        ByteBuffer buffer = serializer.serialize( in );
        ReplicatedContent out = serializer.deserialize( buffer );

        // then
        assertEquals( in, out );
    }

    @Test
    public void shouldSerializeTransactionRepresentation() throws Exception
    {
        // given
        RaftContentSerializer serializer = new RaftContentSerializer();
        Collection<Command> commands = new ArrayList<>(  );

        IndexCommand.AddNodeCommand addNodeCommand = new IndexCommand.AddNodeCommand();
        addNodeCommand.init( 0, 0, 0, 0 );

        commands.add( addNodeCommand );

        byte[] extraHeader = new byte[0];

        PhysicalTransactionRepresentation txIn = new PhysicalTransactionRepresentation( commands );
        txIn.setHeader( extraHeader, -1, -1, 0, 0, 0, 0 );
        ReplicatedTransaction in = ReplicatedTransactionFactory.createImmutableReplicatedTransaction( txIn, globalSession, new LocalOperationId( 0, 0 ) );

        // when
        ByteBuffer buffer = serializer.serialize( in );
        ReplicatedTransaction out = (ReplicatedTransaction)serializer.deserialize( buffer );

        TransactionRepresentation txOut = ReplicatedTransactionFactory.extractTransactionRepresentation( out, extraHeader );

        // then
        assertEquals( in, out );
        assertEquals( txIn, txOut );
    }

    @Test
    public void shouldSerializeIdRangeRequest() throws Exception
    {
        // given
        RaftContentSerializer serializer = new RaftContentSerializer();
        ReplicatedContent in = new ReplicatedIdAllocationRequest( coreMember, IdType.NODE, 100, 200 );

        // when
        ByteBuffer buffer = serializer.serialize( in );
        ReplicatedContent out = serializer.deserialize( buffer );

        // then
        assertEquals( in, out );
    }
}
