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
package org.neo4j.consistency.checking.full;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link ThreadLocal} which additionally assigns a zero-based id to each thread-local value created in
 * {@link #initialValue(int)}.
 */
public abstract class IdAssigningThreadLocal<T> extends ThreadLocal<T>
{
    private final AtomicInteger id = new AtomicInteger();

    @Override
    protected final T initialValue()
    {
        return initialValue( id.getAndIncrement() );
    }

    protected abstract T initialValue( int id );

    /**
     * Resets the id counter so that the next call to {@link #initialValue(int)} will get {@code 0}.
     */
    public void resetId()
    {
        id.set( 0 );
    }
}
