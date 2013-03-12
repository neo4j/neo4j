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
package org.neo4j.kernel.impl.api.index;

import static org.neo4j.helpers.FutureAdapter.VOID;

import java.util.concurrent.Future;

import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.NodePropertyUpdate;

public abstract class AbstractSwallowingIndexProxy implements IndexProxy
{
    private final IndexDescriptor descriptor;
    private final Throwable cause;

    public AbstractSwallowingIndexProxy( IndexDescriptor descriptor, Throwable cause )
    {
        this.descriptor = descriptor;
        this.cause = cause;
    }

    @Override
    public void create()
    {
        String message = "Unable to create index, it is in a " + getState().name() + " state.";
        throw new UnsupportedOperationException( message, cause );
    }

    @Override
    public void update( Iterable<NodePropertyUpdate> updates )
    {
        // intentionally swallow updates, we're failed and nothing but re-population or dropIndex will solve this
    }

    @Override
    public void force()
    {
    }

    @Override
    public IndexDescriptor getDescriptor()
    {
        return descriptor;
    }

    @Override
    public Future<Void> close()
    {
        return VOID;
    }
    
    @Override
    public IndexReader newReader()
    {
        throw new UnsupportedOperationException();
    }
}
