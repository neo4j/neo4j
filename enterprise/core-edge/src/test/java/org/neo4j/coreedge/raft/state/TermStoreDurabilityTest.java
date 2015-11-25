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

import java.io.File;

import org.junit.Test;

import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.state.DurableTermStore;
import org.neo4j.coreedge.raft.state.TermStore;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;

import static org.junit.Assert.assertEquals;

public class TermStoreDurabilityTest
{
    public TermStore createTermStore( EphemeralFileSystemAbstraction fileSystem )
    {
        File directory = new File( "raft-log" );
        fileSystem.mkdir( directory );
        return new DurableTermStore( fileSystem, directory );
    }

    @Test
    public void shouldStoreTerm() throws Exception
    {
        EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
        TermStore termStore = createTermStore( fileSystem );

        termStore.update( 23 );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( termStore, fileSystem, new TermVerifier()
        {
            public void verifyTerm( TermStore termStore ) throws RaftStorageException
            {
                assertEquals( 23, termStore.currentTerm() );
            }
        } );
    }

    @Test
    public void emptyFileShouldImplyZeroTerm() throws Exception
    {
        EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
        TermStore termStore = createTermStore( fileSystem );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( termStore, fileSystem, new TermVerifier()
        {
            public void verifyTerm( TermStore termStore ) throws RaftStorageException
            {
                assertEquals( 0, termStore.currentTerm() );
            }
        } );
    }

    private void verifyCurrentLogAndNewLogLoadedFromFileSystem( TermStore termStore,
                                                                EphemeralFileSystemAbstraction fileSystem,
                                                                TermVerifier termVerifier ) throws RaftStorageException
    {
        termVerifier.verifyTerm( termStore );
        termVerifier.verifyTerm( createTermStore( fileSystem ) );
        fileSystem.crash();
        termVerifier.verifyTerm( createTermStore( fileSystem ) );
    }

    private interface TermVerifier
    {
        void verifyTerm( TermStore termStore ) throws RaftStorageException;
    }
}
