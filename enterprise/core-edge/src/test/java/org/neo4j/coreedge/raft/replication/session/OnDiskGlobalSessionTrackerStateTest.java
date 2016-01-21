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
import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.server.RaftTestMember.RaftTestMemberMarshal;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.TargetDirectory;

import static java.util.UUID.randomUUID;

import static junit.framework.Assert.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import static org.neo4j.coreedge.server.RaftTestMember.member;

public class OnDiskGlobalSessionTrackerStateTest
{
    @Rule
    public final TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldRoundtripGlobalSessionTrackerState() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        fsa.mkdir( testDir.directory() );

        OnDiskGlobalSessionTrackerState<RaftTestMember> oldState = new OnDiskGlobalSessionTrackerState<>( fsa,
                testDir.directory(), new RaftTestMemberMarshal(), 100, mock( Supplier.class ) );

        final GlobalSession<RaftTestMember> globalSession = new GlobalSession<>( randomUUID(), member( 1 ) );

        oldState.validateAndTrackOperationAtLogIndex( globalSession, new LocalOperationId( 1, 0 ), 0 );

        // when
        OnDiskGlobalSessionTrackerState<RaftTestMember> newState = new OnDiskGlobalSessionTrackerState<>( fsa,
                testDir.directory(), new RaftTestMemberMarshal(), 100, mock( Supplier.class ) );

        // then
        assertTrue( oldState.validateAndTrackOperationAtLogIndex( globalSession, new LocalOperationId( 1, 1 ), 99 ) );
        assertTrue( newState.validateAndTrackOperationAtLogIndex( globalSession, new LocalOperationId( 1, 1 ), 99 ) );

        assertFalse( oldState.validateAndTrackOperationAtLogIndex( globalSession, new LocalOperationId( 1, 3 ), 99 ) );
        assertFalse( newState.validateAndTrackOperationAtLogIndex( globalSession, new LocalOperationId( 1, 3 ), 99 ) );
    }

    @Test
    public void shouldPersistOnSessionCreation() throws Exception
    {
        // given
        FileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();

        OnDiskGlobalSessionTrackerState<RaftTestMember> state = new OnDiskGlobalSessionTrackerState<>( fsa,
                testDir.directory(), new RaftTestMemberMarshal(), 10, mock( Supplier.class ) );

        // when
        state.validateAndTrackOperationAtLogIndex( new GlobalSession<>( randomUUID(), member( 1 ) ),
                new LocalOperationId( 1, 0 ), 0 );

        // then
        assertThat( fsa.getFileSize( new File( testDir.directory(), OnDiskGlobalSessionTrackerState.FILENAME + "a" )
        ), greaterThan( 0L ) );
    }

    @Test
    public void shouldPersistOnSessionUpdate() throws Exception
    {
        // given
        FileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();

        OnDiskGlobalSessionTrackerState<RaftTestMember> state = new OnDiskGlobalSessionTrackerState<>( fsa,
                testDir.directory(), new RaftTestMemberMarshal(), 10, mock( Supplier.class ) );
        File fileName = new File( testDir.directory(), OnDiskGlobalSessionTrackerState.FILENAME + "a" );

        GlobalSession<RaftTestMember> globalSession = new GlobalSession<>( randomUUID(), member( 1 ) );
        state.validateAndTrackOperationAtLogIndex( globalSession, new LocalOperationId( 1, 0 ), 0 );

        long initialFileSize = fsa.getFileSize( fileName );

        // when
        // the global session exists and this local operation id is a valid next value
        state.validateAndTrackOperationAtLogIndex( globalSession, new LocalOperationId( 1, 1 ), 1 );

        // then
        assertThat( fsa.getFileSize( fileName ), greaterThan( initialFileSize ) );
    }

    @Test
    public void shouldNotPersistOnNegativeSessionCheckForExistingGlobalSession() throws Exception
    {
        // given
        FileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();

        OnDiskGlobalSessionTrackerState<RaftTestMember> state = new OnDiskGlobalSessionTrackerState<>( fsa,
                testDir.directory(), new RaftTestMemberMarshal(), 10, mock( Supplier.class ) );
        File fileName = new File( testDir.directory(), OnDiskGlobalSessionTrackerState.FILENAME + "a" );

        GlobalSession<RaftTestMember> globalSession = new GlobalSession<>( randomUUID(), member(
                1 ) );
        state.validateAndTrackOperationAtLogIndex( globalSession, new LocalOperationId( 1, 0 ), 0 );

        long initialFileSize = fsa.getFileSize( fileName );
        assertThat( initialFileSize, greaterThan( 0L ) );

        // when
        // The global session exists but this local operation id is not a valid next value
        state.validateAndTrackOperationAtLogIndex( globalSession, new LocalOperationId( 2, 4 ), 1 );

        // then
        assertThat( fsa.getFileSize( fileName ), equalTo( initialFileSize ) );
    }


    @Test
    public void shouldNotPersistOnNegativeSessionCheckForNewGlobalSession() throws Exception
    {
        // given
        FileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();

        OnDiskGlobalSessionTrackerState<RaftTestMember> state = new OnDiskGlobalSessionTrackerState<>( fsa,
                testDir.directory(), new RaftTestMemberMarshal(), 10, mock( Supplier.class ) );
        File fileName = new File( testDir.directory(), OnDiskGlobalSessionTrackerState.FILENAME + "a" );

        GlobalSession<RaftTestMember> globalSession = new GlobalSession<>( randomUUID(), member(
                1 ) );

        // when
        // this is the first time we see globalSession but the local operation id is invalid
        state.validateAndTrackOperationAtLogIndex( globalSession, new LocalOperationId( 1, 1 ), 0 );

        // then
        assertThat( fsa.getFileSize( fileName ), equalTo( 0L ) );
    }
}