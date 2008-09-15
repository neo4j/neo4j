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

/**
 * Re-active event listeners should implement this interface. When a re-active
 * event is generated the <CODE>reActiveEventReceived</CODE> method will be
 * invoked on all re-active listeners registered on that event.
 */
public interface ReActiveEventListener
{
    // EE: The contract should specify whether event and data can ever
    // be null
    /**
     * Invoked if <CODE>this</CODE> is registered on event type <CODE>event</CODE>
     * and an event of type <CODE>event</CODE> has been generated.
     * 
     * @param event
     *            the generated event type
     * @param data
     *            the event data
     */
    public void reActiveEventReceived( Event event, EventData data );
}