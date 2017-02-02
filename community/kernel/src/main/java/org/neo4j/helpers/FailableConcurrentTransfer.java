/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.helpers;

import java.util.concurrent.CountDownLatch;

import org.neo4j.function.FailableConsumer;
import org.neo4j.function.ThrowingSupplier;

/**
 * Abstracts a meeting point between two threads, where a reference can change hands. It is essentially
 * a latch where a reference to a value can be set while a thread waits on it.
 */
public class FailableConcurrentTransfer<TYPE> implements FailableConsumer<TYPE>, ThrowingSupplier<TYPE, Exception>
{
    private final CountDownLatch latch = new CountDownLatch( 1 );
    private TYPE value;
    private Exception failure;

    @Override
    public void accept( TYPE value )
    {
        this.value = value;
        latch.countDown();
    }

    @Override
    public void fail( Exception failure )
    {
        this.failure = failure;
        latch.countDown();
    }

    @Override
    public TYPE get() throws Exception
    {
        try
        {
            latch.await();
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( "Thread interrupted", e );
        }
        if ( failure != null )
        {
            throw failure;
        }
        return value;
    }

    @Override
    public String toString()
    {
        return String.format( "ConcurrentTransfer{%s}", latch.getCount() == 1 ? "<waiting>" : value );
    }
}
