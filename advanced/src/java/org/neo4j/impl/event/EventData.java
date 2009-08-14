/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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

/**
 * <CODE>EventData</CODE> encapsulates data that is passed to the pro-active
 * and re-active event listeners receiving the event.
 */
public class EventData
{
    private final Object data;
    private Thread originatingThread = null;

    /**
     * Sets the data to encapsulate.
     * 
     * @param data
     *            the event data
     */
    public EventData( Object data )
    {
        this.data = data;
    }

    /**
     * Sets the thread that originated this event.
     * @param originatingThread
     *            the thread that originated this event
     */
    void setOriginatingThread( Thread originatingThread )
    {
        this.originatingThread = originatingThread;
    }

    /**
     * Returns the encapsulated data.
     * 
     * @return the event data
     */
    public Object getData()
    {
        return data;
    }

    /**
     * Gets the thread that originated this event.
     * @return the thread that originated this event
     */
    public Thread getOriginatingThread()
    {
        return this.originatingThread;
    }
}