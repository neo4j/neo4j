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
package org.neo4j.legacy.consistency.repair;

import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static java.lang.String.format;

public enum RelationshipChainDirection
{
    NEXT( RelationshipChainField.FIRST_NEXT, RelationshipChainField.SECOND_NEXT ),
    PREV( RelationshipChainField.FIRST_PREV, RelationshipChainField.SECOND_PREV );

    private final RelationshipChainField first;
    private final RelationshipChainField second;

    RelationshipChainDirection( RelationshipChainField first, RelationshipChainField second )
    {
        this.first = first;
        this.second = second;
    }

    public RelationshipChainField fieldFor( long nodeId, RelationshipRecord rel )
    {
        if (rel.getFirstNode() == nodeId)
        {
            return first;
        }
        else if (rel.getSecondNode() == nodeId)
        {
            return second;
        }
        throw new IllegalArgumentException( format( "%s does not reference node %d", rel, nodeId ) );
    }
}
