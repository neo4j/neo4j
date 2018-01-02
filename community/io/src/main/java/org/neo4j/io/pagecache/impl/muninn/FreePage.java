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
package org.neo4j.io.pagecache.impl.muninn;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A free page in the MuninnPageCache.freelist.
 *
 * The next pointers are always other FreePage instances.
 */
final class FreePage
{
    final long pageRef;
    int count;
    Object next;

    FreePage( long pageRef )
    {
        this.pageRef = pageRef;
    }

    void setNext( Object next )
    {
        this.next = next;
        if ( next == null )
        {
            count = 1;
        }
        else if ( next.getClass() == AtomicInteger.class )
        {
            count = 1 + ((AtomicInteger) next).get();
        }
        else
        {
            this.count = 1 + ((FreePage) next).count;
        }
    }
}
