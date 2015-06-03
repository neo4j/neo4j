/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.concurrent;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks an (approximate) set of recently seen unique elements in a stream, based on a concurrent LRU implementation.
 *
 * @param <Type> the entry type stored
 */
public class RecentK<Type> implements Iterable<Type>
{
    private final int maxItems;

    /**
     * Maps items to their slots in the LRU queue, the keys in here is what we use as the source of truth
     * for what are the most recent items.
     */
    private final ConcurrentHashMap<Type, Slot> index = new ConcurrentHashMap<>();

    /** Most recently seen item */
    // guarded by synchronized(this)
    private Slot head;

    /** Least recently used item */
    // guarded by synchronized(this)
    private Slot tail;

    /**
     * @param maxItems is the size of the item set to track
     */
    public RecentK( int maxItems )
    {
        this.maxItems = maxItems;
    }

    /**
     * @param item a new item to the tracked set.
     */
    public synchronized void add( Type item )
    {
        Slot slot = index.get( item );
        if(slot != null)
        {
            // Known item, move to head of queue
            markAsMostRecent( slot );
        }
        else if( index.size() >= maxItems )
        {
            // New item, queue full
            removeLeastRecent();
            addNewItem( item );
        }
        else
        {
            // New item, not yet reached max size
            addNewItem( item );
        }
    }

    private void removeLeastRecent()
    {
        // assumes synchronized(this)
        index.remove( tail.item );

        if( head == tail ) // can happen if maxSize = 1
        {
            head = tail = null;
        }
        else
        {
            tail = tail.prev;
            tail.next = null;
        }
    }

    private void addNewItem( Type item )
    {
        // assumes synchronized(this)

        // Create a new slot
        Slot slot = new Slot( item );
        index.put( item, slot );

        // Add it as head
        if( head != null)
        {
            slot.next = head;
            head.prev = slot;
            head = slot;
        }
        else
        {
            head = slot;
            tail = slot;
        }
    }

    private void markAsMostRecent( Slot slot )
    {
        // assumes synchronized(this)
        if(slot == head)
        {
            return;
        }

        // Mend the hole we're about to make in the chain
        if(slot == tail)
        {
            tail = slot.prev;
            slot.prev.next = slot.next;
        }
        else
        {
            slot.prev.next = slot.next;
            slot.next.prev = slot.prev;
        }

        // Make head entry
        slot.prev = null;
        slot.next = head;
        head = slot;
    }

    public Set<Type> recentItems()
    {
        return index.keySet();
    }

    @Override
    public Iterator<Type> iterator()
    {
        return recentItems().iterator();
    }

    class Slot
    {
        private final Type item;

        private Slot prev;
        private Slot next;

        Slot( Type item )
        {
            this.item = item;
        }
    }
}
