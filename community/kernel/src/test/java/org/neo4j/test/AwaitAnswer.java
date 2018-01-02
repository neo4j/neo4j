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
package org.neo4j.test;

import java.util.concurrent.CountDownLatch;

import org.mockito.internal.stubbing.answers.Returns;
import org.mockito.internal.stubbing.answers.ThrowsException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class AwaitAnswer<T> implements Answer<T>
{
    public static AwaitAnswer<Void> afterAwaiting( CountDownLatch latch )
    {
        return new AwaitAnswer<Void>( latch, null );
    }

    private final CountDownLatch latch;
    private final Answer<T> result;

    public AwaitAnswer( CountDownLatch latch, Answer<T> result )
    {
        this.latch = latch;
        this.result = result;
    }

    @Override
    public T answer( InvocationOnMock invocation ) throws Throwable
    {
        latch.await();
        return result == null ? null : result.answer( invocation );
    }

    public <R> Answer<R> then( Answer<R> result )
    {
        return new AwaitAnswer<R>( latch, result );
    }

    @SuppressWarnings("unchecked")
    public <R> Answer<R> thenReturn( R result )
    {
        return then( (Answer<R>) new Returns( result ) );
    }

    @SuppressWarnings("unchecked")
    public <R> Answer<R> thenThrow( Throwable exception )
    {
        return then( (Answer<R>) new ThrowsException( exception ) );
    }
}
