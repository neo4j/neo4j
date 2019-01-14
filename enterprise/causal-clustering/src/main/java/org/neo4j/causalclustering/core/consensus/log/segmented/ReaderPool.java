/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
