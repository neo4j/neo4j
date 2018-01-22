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
package org.neo4j.kernel.impl.util.diffsets;

import org.neo4j.collection.primitive.CombinePrimitiveLongSet;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.storageengine.api.txstate.PrimitiveLongReadableDiffSets;

public class CombinedPrimitiveLongDiffSets implements PrimitiveLongReadableDiffSets
{
    private final PrimitiveLongReadableDiffSets masterState;
    private final PrimitiveLongReadableDiffSets additionalState;

    CombinedPrimitiveLongDiffSets( PrimitiveLongReadableDiffSets masterState,
            PrimitiveLongReadableDiffSets additionalState )
    {
        this.masterState = masterState;
        this.additionalState = additionalState;
    }

    @Override
    public boolean isAdded( long element )
    {
        return masterState.isAdded( element ) || additionalState.isAdded( element );
    }

    @Override
    public boolean isRemoved( long element )
    {
        return masterState.isRemoved( element ) || additionalState.isRemoved( element );
    }

    @Override
    public PrimitiveLongSet getAdded()
    {
        return new CombinePrimitiveLongSet( masterState.getAdded(), additionalState.getAdded() );
    }

    @Override
    public PrimitiveLongSet getAddedSnapshot()
    {
        throw new UnsupportedOperationException( "AddedSnapshot is not supported in " + getClass() );
    }

    @Override
    public PrimitiveLongSet getRemoved()
    {
        return new CombinePrimitiveLongSet( masterState.getRemoved(), additionalState.getRemoved() );
    }

    @Override
    public boolean isEmpty()
    {
        return masterState.isEmpty() && additionalState.isEmpty();
    }

    @Override
    public int delta()
    {
        return masterState.delta() + additionalState.delta();
    }

    @Override
    public PrimitiveLongIterator augment( PrimitiveLongIterator elements )
    {
        throw new UnsupportedOperationException( "Augment is not supported in " + getClass() );
    }

    @Override
    public PrimitiveLongReadableDiffSets combine( PrimitiveLongReadableDiffSets diffSets )
    {
        throw new UnsupportedOperationException( "Combine is not supported in " + getClass() );
    }
}
