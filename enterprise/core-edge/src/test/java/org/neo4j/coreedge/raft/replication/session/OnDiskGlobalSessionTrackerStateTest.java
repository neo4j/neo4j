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
package org.neo4j.coreedge.raft.replication.session;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.server.RaftTestMember.RaftTestMemberMarshal;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TargetDirectory;

import static java.util.UUID.randomUUID;

import static junit.framework.Assert.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import static org.neo4j.coreedge.server.RaftTestMember.member;

public class OnDiskGlobalSessionTrackerStateTest extends BaseGlobalSessionTrackerStateTest
{
    @Rule
    public final TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    private final EphemeralFileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();

    @Test
    public void shouldRoundtripGlobalSessionTrackerState() throws Exception
    {
        // given
        GlobalSessionTrackerState<RaftTestMember> oldState = instantiateSessionTracker();

        final GlobalSession<RaftTestMember> globalSession = new GlobalSession<>( randomUUID(), member( 1 ) );

        oldState.update( globalSession, new LocalOperationId( 1, 0 ), 0 );

        // when
        OnDiskGlobalSessionTrackerState<RaftTestMember> newState = new OnDiskGlobalSessionTrackerState<>( fsa,
                testDir.directory(), new RaftTestMemberMarshal(), 100, mock( Supplier.class ),
                NullLogProvider.getInstance() );

        // then
        assertTrue( oldState.validateOperation( globalSession, new LocalOperationId( 1, 1 ) ) );
        assertTrue( newState.validateOperation( globalSession, new LocalOperationId( 1, 1 ) ) );

        assertFalse( oldState.validateOperation( globalSession, new LocalOperationId( 1, 3 ) ) );
        assertFalse( newState.validateOperation( globalSession, new LocalOperationId( 1, 3 ) ) );
    }

    @Test
    public void shouldPersistOnSessionCreation() throws Exception
    {
        // given
        GlobalSessionTrackerState<RaftTestMember> state = instantiateSessionTracker();

        // when
        state.update( new GlobalSession<>( randomUUID(), member( 1 ) ), new LocalOperationId( 1, 0 ), 0 );

        // then
        assertThat( fsa.getFileSize( new File( testDir.directory(), OnDiskGlobalSessionTrackerState.FILENAME + "a" )
        ), greaterThan( 0L ) );
    }

    @Test
    public void shouldPersistOnSessionUpdate() throws Exception
    {
        // given
        GlobalSessionTrackerState<RaftTestMember> state = instantiateSessionTracker();
        File fileName = new File( testDir.directory(), OnDiskGlobalSessionTrackerState.FILENAME + "a" );

        GlobalSession<RaftTestMember> globalSession = new GlobalSession<>( randomUUID(), member( 1 ) );
        state.update( globalSession, new LocalOperationId( 1, 0 ), 0 );

        long initialFileSize = fsa.getFileSize( fileName );

        // when
        // the global session exists and this local operation id is a valid next value
        state.update( globalSession, new LocalOperationId( 1, 1 ), 1 );

        // then
        assertThat( fsa.getFileSize( fileName ), greaterThan( initialFileSize ) );
    }

    @Override
    protected GlobalSessionTrackerState<RaftTestMember> instantiateSessionTracker()
    {
        fsa.mkdir( testDir.directory() );

        try
        {
            return new OnDiskGlobalSessionTrackerState<>( fsa, testDir.directory(), new RaftTestMemberMarshal(), 100,
                    mock( Supplier.class ), NullLogProvider.getInstance() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}