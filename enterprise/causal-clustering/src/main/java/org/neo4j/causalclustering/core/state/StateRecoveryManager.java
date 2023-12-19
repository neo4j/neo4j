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
package org.neo4j.causalclustering.core.state;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.core.state.storage.StateMarshal;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosableChannel;

public class StateRecoveryManager<STATE>
{
    public static class RecoveryStatus<STATE>
    {
        private final File activeFile;
        private final STATE recoveredState;

        RecoveryStatus( File activeFile, STATE recoveredState )
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

        STATE a = readLastEntryFrom( fileA );
        STATE b = readLastEntryFrom( fileB );

        if ( a == null && b == null )
        {
            throw new IllegalStateException( "no recoverable state" );
        }

        if ( a == null )
        {
            return new RecoveryStatus<>( fileA, b );
        }
        else if ( b == null )
        {
            return new RecoveryStatus<>( fileB, a );
        }
        else if ( marshal.ordinal( a ) > marshal.ordinal( b ) )
        {
            return new RecoveryStatus<>( fileB, a );
        }
        else
        {
            return new RecoveryStatus<>( fileA, b );
        }
    }

    private STATE readLastEntryFrom( File file ) throws IOException
    {
        try ( ReadableClosableChannel channel = new ReadAheadChannel<>( fileSystem.open( file, OpenMode.READ ) ) )
        {
            STATE result = null;
            STATE lastRead;

            try
            {
                while ( (lastRead = marshal.unmarshal( channel )) != null )
                {
                    result = lastRead;
                }
            }
            catch ( EndOfStreamException e )
            {
                // ignore; just use previous complete entry
            }

            return result;
        }
    }
}
