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

import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;

public class FailedIndexProxy extends AbstractSwallowingIndexProxy
{
    protected final IndexPopulator populator;

    public FailedIndexProxy( IndexDescriptor descriptor, IndexPopulator populator )
    {
       this( descriptor, populator, null );
    }

    public FailedIndexProxy( IndexDescriptor descriptor, IndexPopulator populator, Throwable cause )
    {
        super( descriptor, cause );
        this.populator = populator;
    }

    @Override
    public Future<Void> drop()
    {
        populator.drop();
        return VOID;
    }

    @Override
    public InternalIndexState getState()
    {
        return InternalIndexState.FAILED;
    }
}
