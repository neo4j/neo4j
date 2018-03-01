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
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.RelationshipVisitor.Home;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptySet;

/**
 * Given a sequence of add and removal operations, instances of DiffSets track
 * which elements need to actually be added and removed at minimum from some
 * hypothetical target collection such that the result is equivalent to just
 * executing the sequence of additions and removals in order
 *
 */
public class PrimitiveRelationshipDiffSets extends PrimitiveLongDiffSets<RelationshipIterator, RelationshipIterator>
{
    private Home txStateRelationshipHome;

    public PrimitiveRelationshipDiffSets( RelationshipVisitor.Home txStateRelationshipHome )
    {
        this( txStateRelationshipHome, emptySet(), emptySet() );
    }

    public PrimitiveRelationshipDiffSets( RelationshipVisitor.Home txStateRelationshipHome,
            PrimitiveLongSet addedElements, PrimitiveLongSet removedElements )
    {
        super( addedElements, removedElements );
        this.txStateRelationshipHome = txStateRelationshipHome;
    }

    @Override
    public RelationshipIterator augment( final RelationshipIterator source )
    {
        return new DiffApplyingRelationshipIterator( source, getAdded(), getRemoved(), txStateRelationshipHome );
    }
}
