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
package org.neo4j.kernel.impl.util;

import org.neo4j.function.Supplier;

/**
 * Manages a lazy initialized single reference that can be {@link #invalidate() invalidated}.
 * Concurrent {@link #get() access} is supported and only a single instance will be {@link #create() created}.
 */
public abstract class LazySingleReference<T> implements Supplier<T>
{
    private volatile T reference;
    
    /**
     * @return whether or not the managed reference has been initialized, i.e {@link #get() evaluated}
     * for the first time, or after {@link #invalidate() invalidated}.
     */
    public boolean isCreated()
    {
        return reference != null;
    }
    
    /**
     * Returns the reference, initializing it if need be.
     */
    @Override
    public T get()
    {
        T result;
        if ( (result = reference) == null )
        {
            synchronized ( this )
            {
                if ( (result = reference) == null )
                {
                    result = reference = create();
                }
            }
        }
        return result;
    }
    
    /**
     * Invalidates any initialized reference. A future call to {@link #get()} will have it initialized again.
     */
    public synchronized void invalidate()
    {
        reference = null;
    }
    
    /**
     * Provides a reference to manage.
     */
    protected abstract T create();
}
