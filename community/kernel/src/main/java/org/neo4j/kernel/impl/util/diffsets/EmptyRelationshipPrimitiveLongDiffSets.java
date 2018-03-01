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

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.storageengine.api.txstate.PrimitiveLongDiffSetsVisitor;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptySet;

public class EmptyRelationshipPrimitiveLongDiffSets extends PrimitiveRelationshipDiffSets
{
    public static final PrimitiveRelationshipDiffSets INSTANCE = new EmptyRelationshipPrimitiveLongDiffSets();

    private EmptyRelationshipPrimitiveLongDiffSets()
    {
        super( null );
    }

    @Override
    public boolean isAdded( long element )
    {
        return false;
    }

    @Override
    public boolean isRemoved( long element )
    {
        return false;
    }

    @Override
    public void add( long element )
    {
    }

    @Override
    public boolean remove( long element )
    {
        return false;
    }

    @Override
    public void visit( PrimitiveLongDiffSetsVisitor visitor )
    {
    }

    @Override
    public int delta()
    {
        return 0;
    }

    @Override
    public PrimitiveLongSet getAdded()
    {
        return emptySet();
    }

    @Override
    public PrimitiveLongSet getRemoved()
    {
        return emptySet();
    }

    @Override
    public PrimitiveLongSet getAddedSnapshot()
    {
        return emptySet();
    }

    @Override
    public boolean isEmpty()
    {
        return true;
    }
}
