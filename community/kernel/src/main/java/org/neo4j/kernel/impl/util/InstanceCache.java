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
package org.neo4j.kernel.impl.util;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.function.Disposable;

/**
 * Caches single instances. This is meant to be used within a single thread, where
 * the usage pattern is such that it is likely that at any given time only one T is needed, but at times
 * more than one T is used. This cache will for the majority of cases cache that single instance.
 *
 * @param <T>
 */
public abstract class InstanceCache<T extends Disposable> implements Supplier<T>, Consumer<T>, AutoCloseable
{
    private T instance;

    @Override
    public T get()
    {
        if ( instance == null )
        {
            return create();
        }

        try
        {
            return instance;
        }
        finally
        {
            instance = null;
        }
    }

    protected abstract T create();

    @Override
    public void accept( T instance )
    {
        if ( this.instance == null )
        {
            this.instance = instance;
        }
        else
        {
            instance.dispose();
        }
    }

    @Override
    public void close()
    {
        if ( instance != null )
        {
            instance.dispose();
        }
    }
}
