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

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.collection.primitive.versioned.VersionedPrimitiveLongSet;

public class VersionedPrimitiveLongDiffSets<I extends PrimitiveLongIterator, O extends PrimitiveLongResourceIterator>
{
    private final VersionedPrimitiveLongSet added = new VersionedPrimitiveLongSet();
    private final VersionedPrimitiveLongSet removed = new VersionedPrimitiveLongSet();
    private final PrimitiveLongDiffSets<I, O> currentView = new PrimitiveLongDiffSets<>( added.currentView(), removed.currentView() );
    private final PrimitiveLongDiffSets<I, O> stableView = new PrimitiveLongDiffSets<>( added.currentView(), removed.currentView() );

    public PrimitiveLongDiffSets<I, O> currentView()
    {
        return currentView;
    }

    public PrimitiveLongDiffSets<I, O> stableView()
    {
        return stableView;
    }

    public void markStable()
    {
        added.markStable();
        removed.markStable();
    }
}
