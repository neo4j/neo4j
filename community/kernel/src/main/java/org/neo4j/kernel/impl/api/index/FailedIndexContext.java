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

import org.neo4j.kernel.api.InternalIndexState;

public class FailedIndexContext implements IndexContext
{
    private final IndexPopulator populator;
    private final Throwable cause;

    public FailedIndexContext( IndexPopulator populator )
    {
        this( populator, null );
    }

    public FailedIndexContext( IndexPopulator populator, Throwable cause )
    {
        this.populator = populator;
        this.cause = cause;
    }

    @Override
    public void create()
    {
        throw new UnsupportedOperationException( "Unable to create index, it is in a failed state.", cause );
    }

    @Override
    public void update( Iterable<NodePropertyUpdate> updates )
    {
        // intentionally swallow updates, we're failed and nothing but repopulation or dropIndex will solve this
    }

    @Override
    public void drop()
    {
        populator.dropIndex();
    }

    @Override
    public InternalIndexState getState()
    {
        return InternalIndexState.FAILED;
    }

    @Override
    public void force()
    {
    }
}
