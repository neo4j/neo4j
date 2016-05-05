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
package org.neo4j.coreedge.raft.log.physical;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Keeps a pool of store channels available.
 */
class StoreChannelPool
{
    private final FileSystemAbstraction fsa;
    private final File file;
    private final String mode;
    private final Log log;

    private int openChannelCount;
    private final Deque<StoreChannel> channels = new ArrayDeque<>();
    private boolean markedForDisposal;
    private Runnable onDisposal;
    private boolean disposed;

    StoreChannelPool( FileSystemAbstraction fsa, File file, String mode, LogProvider logProvider )
    {
        this.fsa = fsa;
        this.file = file;
        this.mode = mode;

        this.log = logProvider.getLog( getClass() );
    }

    private StoreChannel create() throws IOException
    {
        openChannelCount++;
        return fsa.open( file, mode );
    }

    synchronized StoreChannel acquire( long byteOffset ) throws IOException, DisposedException
    {
        if( markedForDisposal )
        {
            throw new DisposedException();
        }

        StoreChannel storeChannel = channels.pollFirst();
        if ( storeChannel == null )
        {
            storeChannel = create();
        }
        storeChannel.position( byteOffset );
        return storeChannel;
    }

    synchronized void release( StoreChannel channel )
    {
        channels.addFirst( channel );
        checkForDisposal();
    }

    private void checkForDisposal()
    {
        if ( markedForDisposal && channels.size() == openChannelCount )
        {
            closeAllChannels();
            disposed = true;
            onDisposal.run();
        }
    }

    private void closeAllChannels()
    {
        for ( StoreChannel channel : channels )
        {
            try
            {
                channel.close();
            }
            catch ( IOException e )
            {
                log.error( "Error closing: " + file, e );
            }
        }
    }

    synchronized void markForDisposal( Runnable onDisposal ) throws DisposedException
    {
        if( markedForDisposal )
        {
            throw new DisposedException();
        }

        this.markedForDisposal = true;
        this.onDisposal = onDisposal;

        checkForDisposal();
    }

    boolean isDisposed()
    {
        return disposed;
    }
}
