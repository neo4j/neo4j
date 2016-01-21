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
package org.neo4j.coreedge.server.core.locks;

import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.server.RaftTestMarshal;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import static org.neo4j.coreedge.server.RaftTestMember.*;

public class OnDiskReplicatedLockTokenStateTest
{
    @Rule
    public final TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldRoundtripGlobalSessionTrackerState() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        fsa.mkdir( testDir.directory() );

        OnDiskReplicatedLockTokenState<RaftTestMember> oldState =
                new OnDiskReplicatedLockTokenState<>( fsa, testDir.directory(), 100, new RaftTestMarshal(),
                        mock( Supplier.class ) );

        oldState.set( new ReplicatedLockTokenRequest<>( member( 1 ), 99 ), 0 );

        // when
        OnDiskReplicatedLockTokenState<RaftTestMember> newState =
                new OnDiskReplicatedLockTokenState<>( fsa, testDir.directory(), 100, new RaftTestMarshal(),
                        mock( Supplier.class ) );

        // then
        assertEquals( oldState.get().owner(), newState.get().owner() );
        assertEquals( oldState.get().id(), newState.get().id() );
    }
}