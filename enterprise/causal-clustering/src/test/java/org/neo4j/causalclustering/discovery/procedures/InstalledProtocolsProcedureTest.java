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
package org.neo4j.causalclustering.discovery.procedures;

import org.junit.Test;

import java.util.stream.Stream;

import org.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols;
import org.neo4j.causalclustering.scenarios.InstalledProtocolsProcedureIT;
import org.neo4j.collection.RawIterator;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.arrayContaining;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 * @see InstalledProtocolsProcedureIT
 */
public class InstalledProtocolsProcedureTest
{
    private Pair<AdvertisedSocketAddress,ProtocolStack> outbound1 =
            Pair.of( new AdvertisedSocketAddress( "host1", 1 ),
                    new ProtocolStack( TestApplicationProtocols.RAFT_1, asList( TestModifierProtocols.SNAPPY ) ) );
    private Pair<AdvertisedSocketAddress,ProtocolStack> outbound2 =
            Pair.of( new AdvertisedSocketAddress( "host2", 2 ),
                    new ProtocolStack( TestApplicationProtocols.RAFT_2, asList( TestModifierProtocols.SNAPPY, TestModifierProtocols.ROT13 ) ) );

    private Pair<SocketAddress,ProtocolStack> inbound1 =
            Pair.of( new SocketAddress( "host3", 3 ),
                    new ProtocolStack( TestApplicationProtocols.RAFT_3, asList( TestModifierProtocols.SNAPPY ) ) );
    private Pair<SocketAddress,ProtocolStack> inbound2 =
            Pair.of( new SocketAddress( "host4", 4 ),
                    new ProtocolStack( TestApplicationProtocols.RAFT_4, emptyList() ) );

    @Test
    public void shouldHaveEmptyOutputIfNoInstalledProtocols() throws Throwable
    {
        // given
        InstalledProtocolsProcedure installedProtocolsProcedure =
                new InstalledProtocolsProcedure( Stream::empty, Stream::empty );

        // when
        RawIterator<Object[],ProcedureException> result = installedProtocolsProcedure.apply( null, null, null );

        // then
        assertFalse( result.hasNext() );
    }

    @Test
    public void shouldListOutboundProtocols() throws Throwable
    {
        // given
        InstalledProtocolsProcedure installedProtocolsProcedure =
                new InstalledProtocolsProcedure( () -> Stream.of( outbound1, outbound2 ), Stream::empty );

        // when
        RawIterator<Object[],ProcedureException> result = installedProtocolsProcedure.apply( null, null, null );

        // then
        assertThat( result.next(), arrayContaining( "outbound", "host1:1", "raft", 1L, "[TestSnappy]" ) );
        assertThat( result.next(), arrayContaining( "outbound", "host2:2", "raft", 2L, "[TestSnappy,ROT13]" ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void shouldListInboundProtocols() throws Throwable
    {
        // given
        InstalledProtocolsProcedure installedProtocolsProcedure =
                new InstalledProtocolsProcedure( Stream::empty, () -> Stream.of( inbound1, inbound2 ) );

        // when
        RawIterator<Object[],ProcedureException> result = installedProtocolsProcedure.apply( null, null, null );

        // then
        assertThat( result.next(), arrayContaining( "inbound", "host3:3", "raft", 3L, "[TestSnappy]" ) );
        assertThat( result.next(), arrayContaining( "inbound", "host4:4", "raft", 4L, "[]" ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void shouldListInboundAndOutboundProtocols() throws Throwable
    {
        // given
        InstalledProtocolsProcedure installedProtocolsProcedure =
                new InstalledProtocolsProcedure( () -> Stream.of( outbound1, outbound2 ), () -> Stream.of( inbound1, inbound2 ) );

        // when
        RawIterator<Object[],ProcedureException> result = installedProtocolsProcedure.apply( null, null, null );

        // then
        assertThat( result.next(), arrayContaining( "outbound", "host1:1", "raft", 1L, "[TestSnappy]" ) );
        assertThat( result.next(), arrayContaining( "outbound", "host2:2", "raft", 2L, "[TestSnappy,ROT13]" ) );
        assertThat( result.next(), arrayContaining( "inbound", "host3:3", "raft", 3L, "[TestSnappy]" ) );
        assertThat( result.next(), arrayContaining( "inbound", "host4:4", "raft", 4L, "[]" ) );
        assertFalse( result.hasNext() );
    }
}
