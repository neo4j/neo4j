/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.api;

/**
 * Serves as a reusable utility for building a chain of {@link TransactionToApply} instances,
 * where the instances themselves form the linked list. This utility is just for easily being able
 * to append to the end and then at regular intervals batch through the whole queue.
 */
public class TransactionQueue
{
    @FunctionalInterface
    public interface Applier
    {
        void apply( TransactionToApply first, TransactionToApply last ) throws Exception;
    }

    private final int maxSize;
    private final Applier applier;
    private TransactionToApply first;
    private TransactionToApply last;
    private int size;

    public TransactionQueue( int maxSize, Applier applier )
    {
        this.maxSize = maxSize;
        this.applier = applier;
    }

    public void queue( TransactionToApply transaction ) throws Exception
    {
        if ( isEmpty() )
        {
            first = last = transaction;
        }
        else
        {
            last.next( transaction );
            last = transaction;
        }
        if ( ++size == maxSize )
        {
            empty();
        }
    }

    public void empty() throws Exception
    {
        if ( size > 0 )
        {
            applier.apply( first, last );
            first = last = null;
            size = 0;
        }
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    public TransactionToApply first()
    {
        if ( isEmpty() )
        {
            throw new IllegalStateException( "Nothing in queue" );
        }
        return first;
    }

    public TransactionToApply last()
    {
        if ( isEmpty() )
        {
            throw new IllegalStateException( "Nothing in queue" );
        }
        return last;
    }
}
