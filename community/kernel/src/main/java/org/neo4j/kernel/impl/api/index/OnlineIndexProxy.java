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

import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;

public class OnlineIndexProxy implements IndexProxy
{
    private final IndexDescriptor descriptor;
    private final IndexAccessor accessor;

    public OnlineIndexProxy( IndexDescriptor descriptor, IndexAccessor accessor )
    {
        this.descriptor = descriptor;
        this.accessor = accessor;
    }
    
    @Override
    public void create()
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public void update( Iterable<NodePropertyUpdate> updates )
    {
        accessor.updateAndCommit( updates );
    }

    @Override
    public Future<Void> drop()
    {
        accessor.drop();
        return VOID;
    }

    @Override
    public IndexDescriptor getDescriptor()
    {
        return descriptor;
    }

    @Override
    public InternalIndexState getState()
    {
        return InternalIndexState.ONLINE;
    }
    
    @Override
    public void force()
    {
        accessor.force();
    }

    @Override
    public Future<Void> close()
    {
        accessor.close();
        return VOID;
    }
    
    @Override
    public IndexReader newReader()
    {
        return accessor.newReader();
    }
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{" +
                "accessor=" + accessor +
                ", descriptor=" + descriptor +
                '}';
    }
}
