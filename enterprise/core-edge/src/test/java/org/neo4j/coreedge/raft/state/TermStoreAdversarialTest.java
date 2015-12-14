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
package org.neo4j.coreedge.raft.state;

import org.junit.Test;

import java.io.File;

import org.neo4j.adversaries.ClassGuardedAdversary;
import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.SelectiveFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TermStoreAdversarialTest
{
    public TermStore createTermStore( FileSystemAbstraction fileSystem )
    {
        File directory = new File( "raft-log" );
        fileSystem.mkdir( directory );
        return new DurableTermStore( fileSystem, directory );
    }

    @Test
    public void shouldDiscardTermIfChannelFails() throws Exception
    {
        ClassGuardedAdversary adversary = new ClassGuardedAdversary( new CountingAdversary( 1, false ),
                DurableTermStore.class );
        adversary.disable();

        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        FileSystemAbstraction fileSystem = new SelectiveFileSystemAbstraction(
                new File( "raft-log/term.state" ), new AdversarialFileSystemAbstraction( adversary, fs ), fs );
        TermStore log = createTermStore( fileSystem );

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
            public void verifyTerm( TermStore termStore ) throws RaftStorageException
            {
                assertEquals( 21, termStore.currentTerm() );
            }
        } );
    }

    private void verifyCurrentLogAndNewLogLoadedFromFileSystem(
            TermStore log,FileSystemAbstraction fileSystem, TermVerifier termVerifier ) throws RaftStorageException
    {
        termVerifier.verifyTerm( log );
        termVerifier.verifyTerm( createTermStore( fileSystem ) );
    }

    private interface TermVerifier
    {
        void verifyTerm( TermStore termStore ) throws RaftStorageException;
    }
}
