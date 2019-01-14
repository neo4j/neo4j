/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.io.File;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.values.storable.Value;

public class RecoveringIndexProxy extends AbstractSwallowingIndexProxy
{
    RecoveringIndexProxy( IndexMeta indexMeta )
    {
        super( indexMeta, null );
    }

    @Override
    public InternalIndexState getState()
    {
        return InternalIndexState.POPULATING;
    }

    @Override
    public boolean awaitStoreScanCompleted()
    {
        throw unsupportedOperation( "Cannot await population on a recovering index." );
    }

    @Override
    public void activate()
    {
        throw unsupportedOperation( "Cannot activate recovering index." );
    }

    @Override
    public void validate()
    {
        throw unsupportedOperation( "Cannot validate recovering index." );
    }

    @Override
    public void validateBeforeCommit( Value[] tuple )
    {
        throw unsupportedOperation( "Unexpected call for validating value while recovering." );
    }

    @Override
    public ResourceIterator<File> snapshotFiles()
    {
        throw unsupportedOperation( "Cannot snapshot a recovering index." );
    }

    @Override
    public void drop()
    {
    }

    @Override
    public IndexPopulationFailure getPopulationFailure() throws IllegalStateException
    {
        throw new IllegalStateException( this + " is recovering" );
    }

    @Override
    public PopulationProgress getIndexPopulationProgress()
    {
        throw new IllegalStateException( this + " is recovering" );
    }

    private UnsupportedOperationException unsupportedOperation( String message )
    {
        return new UnsupportedOperationException( message + " Recovering Index" + getDescriptor() );
    }
}
