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
package org.neo4j.kernel.impl.util.diffsets;

import java.util.Set;

import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

/**
 * Applies a diffset to the given source {@link RelationshipIterator}.
 * If the given source is a {@link Resource}, then so is this {@link DiffApplyingRelationshipIterator}.
 */
class DiffApplyingRelationshipIterator extends DiffApplyingLongIterator implements RelationshipIterator
{
    private final RelationshipVisitor.Home sourceHome;
    private final RelationshipVisitor.Home addedHome;

    DiffApplyingRelationshipIterator( RelationshipIterator source, Set<?> addedElements, Set<?> removedElements, RelationshipVisitor.Home addedHome )
    {
        super( source, addedElements, removedElements, null );
        this.sourceHome = source;
        this.addedHome = addedHome;
    }

    @Override
    public <EXCEPTION extends Exception> boolean relationshipVisit( long relId,
            RelationshipVisitor<EXCEPTION> visitor ) throws EXCEPTION
    {
        assert relId == next;
        switch ( phase )
        {
        case FILTERED_SOURCE: return sourceHome.relationshipVisit( next, visitor );
        case ADDED_ELEMENTS: return addedHome.relationshipVisit( next, visitor );
        default: throw new IllegalStateException( "Shouldn't have come here in phase " + phase );
        }
    }
}
