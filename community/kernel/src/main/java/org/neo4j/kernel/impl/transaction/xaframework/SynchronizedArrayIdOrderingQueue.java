/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

public class SynchronizedArrayIdOrderingQueue implements IdOrderingQueue
{
    private long[] queue;
    private int offerIndex, headIndex; // absolute indexes, mod:ed on access
    
    public SynchronizedArrayIdOrderingQueue( int initialMaxSize )
    {
        this.queue = new long[initialMaxSize];
    }
    
    @Override
    public synchronized void offer( long id )
    {
        if ( offerIndex - headIndex >= queue.length )
        {
            extendArray();
        }
        assert offerIndex == headIndex || (offerIndex-1)%queue.length < id : "Was offered ids out-of-order, " + id +
                " whereas last offered was " + ((offerIndex-1)%queue.length);
        queue[(offerIndex++)%queue.length] = id;
    }

    @Override
    public synchronized void awaitHead( long id )
    {
        while ( offerIndex == headIndex /*empty*/ || queue[headIndex%queue.length] != id /*head is not our id*/ )
        {
            try
            {
                wait();
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( "Got interrupted while awaiting head " + id, e );
            }
        }
    }

    @Override
    public synchronized void removeHead( long expectedId )
    {
        if ( queue[headIndex%queue.length] != expectedId )
        {
            throw new IllegalStateException( "Was about to remove head and expected it to be " +
                    expectedId + ", but it was " + queue[headIndex] );
        }
        headIndex++;
        notifyAll();
    }
    
    @Override
    public synchronized boolean isEmpty()
    {
        return offerIndex == headIndex;
    }

    private void extendArray()
    {
        long[] newQueue = new long[queue.length << 1];
        int length = offerIndex-headIndex;
        for ( int i = 0; i < length; i++ )
        {
            newQueue[i] = queue[(headIndex+i)%queue.length];
        }
        
        queue = newQueue;
        offerIndex = length;
        headIndex = 0;
    }
}
