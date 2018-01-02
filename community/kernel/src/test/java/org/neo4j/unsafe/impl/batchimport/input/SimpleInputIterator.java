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
package org.neo4j.unsafe.impl.batchimport.input;

import org.neo4j.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.unsafe.impl.batchimport.InputIterator;

/**
 * Crude implementation of an {@link InputIterator}.
 */
public abstract class SimpleInputIterator<T> extends PrefetchingResourceIterator<T> implements InputIterator<T>
{
    protected final String sourceDescription;
    protected int itemNumber;

    public SimpleInputIterator( String sourceDescription )
    {
        this.sourceDescription = sourceDescription;
    }

    @Override
    public void close()
    {   // Nothing to close
    }

    @Override
    public T next()
    {
        T result = super.next();
        itemNumber++;
        return result;
    }

    @Override
    public String sourceDescription()
    {
        return sourceDescription;
    }

    @Override
    public long lineNumber()
    {
        return itemNumber;
    }

    @Override
    public long position()
    {
        return itemNumber;
    }
}
