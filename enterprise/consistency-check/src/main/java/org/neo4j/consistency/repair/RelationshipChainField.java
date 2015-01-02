/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.repair;

import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

@SuppressWarnings( "boxing" )
public enum RelationshipChainField
{
    FIRST_NEXT( Record.NO_NEXT_RELATIONSHIP )
    {
        @Override
        public long relOf( RelationshipRecord rel )
        {
            return rel.getFirstNextRel();
        }

    },
    FIRST_PREV( Record.NO_PREV_RELATIONSHIP )
    {
        @Override
        public long relOf( RelationshipRecord rel )
        {
            return rel.getFirstPrevRel();
        }

    },
    SECOND_NEXT( Record.NO_NEXT_RELATIONSHIP )
    {
        @Override
        public long relOf( RelationshipRecord rel )
        {
            return rel.getSecondNextRel();
        }

    },
    SECOND_PREV( Record.NO_PREV_RELATIONSHIP )
    {
        @Override
        public long relOf( RelationshipRecord rel )
        {
            return rel.getSecondPrevRel();
        }

    };

    public final long none;

    RelationshipChainField( Record none )
    {
        this.none = none.intValue();
    }

    public abstract long relOf( RelationshipRecord rel );
}
