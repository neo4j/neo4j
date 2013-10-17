/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.storemigration.legacystore;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.impl.nioneo.store.Buffer;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.OperationType;
import org.neo4j.kernel.impl.nioneo.store.PersistenceRow;
import org.neo4j.kernel.impl.nioneo.store.PersistenceWindow;
import org.neo4j.kernel.impl.nioneo.store.PersistenceWindowPool;
import org.neo4j.kernel.impl.util.StringLogger;

public class LegacyNeoStoreReader
{
    private static final int RECORD_LENGTH = 9;

    private final FileChannel fileChannel;
    private final PersistenceWindowPool windowPool;

    public LegacyNeoStoreReader( FileSystemAbstraction fs, File fileName, StringLogger log ) throws IOException
    {
        fileChannel = fs.open( fileName, "r" );
        windowPool = new PersistenceWindowPool( fileName,
                RECORD_LENGTH, fileChannel, 0,
                true, true, new ConcurrentHashMap<Long, PersistenceRow>(), log );
    }

    private long getRecord( long id )
    {
        PersistenceWindow window = windowPool.acquire( id, OperationType.READ );
        try
        {
            Buffer buffer = window.getOffsettedBuffer( id );
            buffer.get();
            return buffer.getLong();
        }
        finally
        {
            windowPool.release( window );
        }
    }

    public long getCreationTime()
    {
        return getRecord( 0 );
    }

    public long getRandomNumber()
    {
        return getRecord( 1 );
    }

    public long getVersion()
    {
        return getRecord( 2 );
    }

    public long getLastCommittedTx()
    {
        return getRecord( 3 );
    }

    public void close() throws IOException
    {
        fileChannel.close();
    }
}
