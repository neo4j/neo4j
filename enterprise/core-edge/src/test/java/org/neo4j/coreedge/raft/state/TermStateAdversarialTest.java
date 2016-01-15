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
package org.neo4j.coreedge.raft.state;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.adversaries.ClassGuardedAdversary;
import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.state.term.OnDiskTermState;
import org.neo4j.coreedge.raft.state.term.TermState;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.SelectiveFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TermStateAdversarialTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    public TermState createTermStore( FileSystemAbstraction fileSystem ) throws IOException
    {
        final Supplier mock = mock( Supplier.class );
        when(mock.get()).thenReturn( mock( DatabaseHealth.class) );

        return new OnDiskTermState( fileSystem, testDir.directory(), 100, mock );
    }

    @Test
    public void shouldDiscardTermIfChannelFails() throws Exception
    {
        ClassGuardedAdversary adversary = new ClassGuardedAdversary( new CountingAdversary( 1, false ),
                OnDiskTermState.class );
        adversary.disable();

        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        FileSystemAbstraction fileSystem = new SelectiveFileSystemAbstraction( new File( testDir.directory(),
                "term.a" ), new AdversarialFileSystemAbstraction( adversary, fs ), fs );
        TermState log = createTermStore( fileSystem );

        log.update( 21 );
        adversary.enable();

        try
        {
            log.update( 23 );
            fail( "Should have thrown exception" );
        }
        catch ( RaftStorageException e )
        {
            // expected
        }

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fileSystem, new TermVerifier()
        {
            public void verifyTerm( TermState termState ) throws RaftStorageException
            {
                assertEquals( 21, termState.currentTerm() );
            }
        } );
    }

    private void verifyCurrentLogAndNewLogLoadedFromFileSystem(
            TermState log, FileSystemAbstraction fileSystem, TermVerifier termVerifier ) throws RaftStorageException,
            IOException
    {
        termVerifier.verifyTerm( log );
        termVerifier.verifyTerm( createTermStore( fileSystem ) );
    }

    private interface TermVerifier
    {
        void verifyTerm( TermState termState ) throws RaftStorageException;
    }
}
