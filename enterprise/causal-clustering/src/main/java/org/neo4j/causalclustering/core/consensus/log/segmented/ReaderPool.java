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
package org.neo4j.causalclustering.core.consensus.log.segmented;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Optional.empty;
import static java.util.Optional.of;

class ReaderPool
{
    private ArrayList<Reader> pool;
    private final int maxSize;
    private final Log log;
    private final FileNames fileNames;
    private final FileSystemAbstraction fsa;
    private final Clock clock;

    ReaderPool( int maxSize, LogProvider logProvider, FileNames fileNames, FileSystemAbstraction fsa, Clock clock )
    {
        this.pool = new ArrayList<>( maxSize );
        this.maxSize = maxSize;
        this.log = logProvider.getLog( getClass() );
        this.fileNames = fileNames;
        this.fsa = fsa;
        this.clock = clock;
    }

    Reader acquire( long version, long byteOffset ) throws IOException
    {
        Reader reader = getFromPool( version );
        if ( reader == null )
        {
            reader = createFor( version );
        }
        reader.channel().position( byteOffset );
        return reader;
    }

    void release( Reader reader )
    {
        reader.setTimeStamp( clock.millis() );
        Optional<Reader> optionalOverflow = putInPool( reader );
        optionalOverflow.ifPresent( this::dispose );
    }

    private synchronized Reader getFromPool( long version )
    {
        Iterator<Reader> itr = pool.iterator();
        while ( itr.hasNext() )
        {
            Reader reader = itr.next();
            if ( reader.version() == version )
            {
                itr.remove();
                return reader;
            }
        }
        return null;
    }

    private synchronized Optional<Reader> putInPool( Reader reader )
    {
        pool.add( reader );
        return pool.size() > maxSize ? of( pool.remove( 0 ) ) : empty();
    }

    private Reader createFor( long version ) throws IOException
    {
        return new Reader( fsa, fileNames.getForVersion( version ), version );
    }

    synchronized void prune( long maxAge, TimeUnit unit )
    {
        if ( pool == null )
        {
            return;
        }

        long endTimeMillis = clock.millis() - unit.toMillis( maxAge );

        Iterator<Reader> itr = pool.iterator();
        while ( itr.hasNext() )
        {
            Reader reader = itr.next();
            if ( reader.getTimeStamp() < endTimeMillis )
            {
                dispose( reader );
                itr.remove();
            }
        }
    }

    private void dispose( Reader reader )
    {
        try
        {
            reader.close();
        }
        catch ( IOException e )
        {
            log.error( "Failed to close reader", e );
        }
    }

    synchronized void close() throws IOException
    {
        for ( Reader reader : pool )
        {
            reader.close();
        }
        pool.clear();
        pool = null;
    }

    public synchronized void prune( long version )
    {
        if ( pool == null )
        {
            return;
        }

        Iterator<Reader> itr = pool.iterator();
        while ( itr.hasNext() )
        {
            Reader reader = itr.next();
            if ( reader.version() == version )
            {
                dispose( reader );
                itr.remove();
            }
        }
    }
}
