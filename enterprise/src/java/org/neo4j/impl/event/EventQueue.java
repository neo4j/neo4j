/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.event;

import java.util.LinkedList;
import java.util.logging.Logger;

// TODO: rewrite this one, use new concurrent util stuff

class EventQueue extends Thread
{
    private static Logger log = Logger.getLogger( EventQueue.class.getName() );

    private final LinkedList<EventElement> queueList = 
        new LinkedList<EventElement>();
    // time to wait before next flush
    private int waitTime = 50;
    // how many new elements before notify for flush
    private int notifyOnCount = 100;
    private volatile boolean run = true;
    private volatile boolean destroyed = false;
    private int elementCount = 0;

    private final EventManager eventManager;

    EventQueue( EventManager eventManager )
    {
        super( "EventQueueConsumer" );
        this.eventManager = eventManager;
    }

    private void queue( EventElement eventElement )
    {
        queueList.add( eventElement );
        elementCount++;

        if ( elementCount > notifyOnCount )
        {
            elementCount = 0;
            this.notify();
        }
    }

    private int flushAll()
    {
        int numElements = 0;
        while ( queueList.size() > 0 )
        {
            sendEvent( queueList.removeFirst() );
            numElements++;
        }
        elementCount = 0;
        return numElements;
    }

    private void sendEvent( EventElement eventElement )
    {
        eventManager.sendReActiveEvent( eventElement.getEvent(), 
            eventElement.getEventData() );
    }

    public synchronized void run()
    {
        try
        {
            EventElement eventElement = eventManager.getNextEventElement();
            while ( eventElement != null )
            {
                queue( eventElement );
                eventElement = eventManager.getNextEventElement();
            }

            while ( run || queueList.size() > 0 )
            {
                flushAll();
                try
                {
                    // if count low increase sleep time?
                    // if count high decrease sleep time?
                    wait( waitTime );
                }
                catch ( InterruptedException e )
                { // ok
                }

                eventElement = eventManager.getNextEventElement();
                while ( eventElement != null )
                {
                    this.queue( eventElement );
                    eventElement = eventManager.getNextEventElement();
                }
            }
            eventElement = eventManager.getNextEventElement();
            while ( eventElement != null )
            {
                queueList.add( eventElement );
                eventElement = eventManager.getNextEventElement();
            }
        }
        catch ( Throwable t )
        {
            t.printStackTrace();
            log.severe( "Event consumer queue caught thowable, "
                + "queue destroyed" );
        }
        destroyed = true;
    }

    void shutdown()
    {
        run = false;
    }

    void waitForDestroy()
    {
        while ( !destroyed )
        {
            try
            {
                Thread.sleep( waitTime );
            }
            catch ( InterruptedException e )
            {
                System.out.println( "Error " + e );
            }
        }
    }

    void setWaitTime( int time )
    {
        waitTime = time;
    }

    int getWaitTime()
    {
        return waitTime;
    }

    void setNotifyOnCount( int count )
    {
        notifyOnCount = count;
    }

    int getNotifyOnCount()
    {
        return notifyOnCount;
    }
}