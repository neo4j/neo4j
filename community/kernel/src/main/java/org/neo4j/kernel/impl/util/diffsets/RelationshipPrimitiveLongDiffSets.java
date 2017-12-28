/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

/**
 * Given a sequence of relationships add and removal operations, tracks
 * which elements need to actually be added and removed at minimum from some
 * target collection such that the result is equivalent to just
 * executing the sequence of additions and removals in order
 */
public class RelationshipPrimitiveLongDiffSets extends PrimitiveLongDiffSets<RelationshipIterator>
{
    private final RelationshipVisitor.Home visitor;

    public RelationshipPrimitiveLongDiffSets( RelationshipVisitor.Home visitor )
    {
        this.visitor = visitor;
    }

    @Override
    public RelationshipIterator augment( RelationshipIterator source )
    {
        return new DiffApplyingRelationshipIterator( source, getAdded(), getRemoved(), visitor );
    }
}
