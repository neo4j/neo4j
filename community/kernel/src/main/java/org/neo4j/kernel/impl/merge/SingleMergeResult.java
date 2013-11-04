/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.merge;

import java.util.NoSuchElementException;

import org.neo4j.graphdb.MergeResult;
import org.neo4j.kernel.api.Statement;

/**
 * Result of getting a unique entity using a {@link org.neo4j.graphdb.Merger}.
 *
 * @param <T> the type of entity returned
 */
public class SingleMergeResult<T> implements MergeResult<T>
{
    private Statement statement;
    private boolean hasNext;

    private final T entity;
    private final boolean wasCreated;

    public SingleMergeResult( Statement statement, T entity, boolean wasCreated )
    {
        this.statement = statement;
        this.hasNext = true;
        this.entity = entity;
        this.wasCreated = wasCreated;
    }

    /**
     * @return the unique entity
     */
    @Override
    public T single()
    {
        statement.assertOpen();
        T result = next();
        assert ! hasNext();
        return result;
    }

    /**
     * @return true if the returned unique entity was newly created (false if it already existed)
     */
    @Override
    public boolean wasCreated()
    {
        return wasCreated;
    }

    @Override
    public boolean hasNext()
    {
        return hasNext;
    }

    @Override
    public T next()
    {
        statement.assertOpen();
        if ( hasNext )
        {
            hasNext = false;
            return entity;
        }
        else
        {
            throw new NoSuchElementException( "Iterator exhausted" );
        }
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
        statement.assertOpen();

        if ( null == statement )
        {
            throw new IllegalStateException( "Cannot close merge result twice" );
        }

        statement.close();
        statement = null;
    }
}
