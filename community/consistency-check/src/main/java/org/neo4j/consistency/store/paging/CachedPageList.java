/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.consistency.store.paging;

class CachedPageList
{
    Page head, tail;
    int size = 0;

    public int size()
    {
        return size;
    }

    public Page removeHead()
    {
        Page removedPage = head;
        head.moveToTailOf( null );
        return removedPage;
    }

    public void incrementSize()
    {
        size++;
    }

    public void decrementSize()
    {
        size--;
    }

    @Override
    public String toString()
    {
        return String.format( "CachedPageList{head=%s, tail=%s, size=%d}", head, tail, size );
    }
}
