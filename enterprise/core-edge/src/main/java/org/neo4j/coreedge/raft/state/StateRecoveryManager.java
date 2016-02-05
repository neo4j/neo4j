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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;
import org.neo4j.storageengine.api.ReadableChannel;

public class StateRecoveryManager<STATE>
{
    public static class RecoveryStatus<STATE>
    {
        private final File activeFile;
        private final STATE recoveredState;

        public RecoveryStatus( File activeFile, STATE recoveredState )
        {
            this.activeFile = activeFile;
            this.recoveredState = recoveredState;
        }

        public STATE recoveredState()
        {
            return recoveredState;
        }

        public File activeFile()
        {
            return activeFile;
        }
    }

    protected final FileSystemAbstraction fileSystem;
    private final StateMarshal<STATE> marshal;

    public StateRecoveryManager( FileSystemAbstraction fileSystem, StateMarshal<STATE> marshal )
    {
        this.fileSystem = fileSystem;
        this.marshal = marshal;
    }

    /**
     * @return RecoveryStatus containing the previously active and previously inactive files. The previously active
     * file contains the latest readable log index (though it may also contain some garbage) and the inactive file is
     * safe to become the new state holder.
     * @throws IOException if any IO goes wrong.
     */
    public RecoveryStatus<STATE> recover( File fileA, File fileB ) throws IOException
    {
        assert fileA != null && fileB != null;

        ensureExists( fileA );
        ensureExists( fileB );

        STATE a = readLastEntryFrom( fileA );
        STATE b = readLastEntryFrom( fileB );

        if ( marshal.ordinal( a ) > marshal.ordinal( b ) )
        {
            return new RecoveryStatus<>( fileB, a );
        }
        else
        {
            return new RecoveryStatus<>( fileA, b );
        }
    }

    private void ensureExists( File file ) throws IOException
    {
        if ( !fileSystem.fileExists( file ) )
        {
            fileSystem.mkdirs( file.getParentFile() );
            fileSystem.create( file ).close();
        }
    }

    public STATE readLastEntryFrom( File file )
            throws IOException
    {
        final ReadableChannel channel = new ReadAheadChannel<>( fileSystem.open( file, "r" ) );

        STATE result = marshal.startState();
        STATE lastRead;

        while ( (lastRead = marshal.unmarshal( channel)) != null )
        {
            result = lastRead;
        }

        return result;
    }
}
