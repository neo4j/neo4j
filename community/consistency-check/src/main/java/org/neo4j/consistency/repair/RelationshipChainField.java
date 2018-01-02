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
package org.neo4j.consistency.repair;

import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

@SuppressWarnings( "boxing" )
public enum RelationshipChainField
{
    FIRST_NEXT
    {
        @Override
        public long relOf( RelationshipRecord rel )
        {
            return rel.getFirstNextRel();
        }

        @Override
        public boolean endOfChain( RelationshipRecord rel )
        {
            return rel.getFirstNextRel() == Record.NO_NEXT_RELATIONSHIP.intValue();
        }
    },
    FIRST_PREV
    {
        @Override
        public long relOf( RelationshipRecord rel )
        {
            return rel.getFirstPrevRel();
        }

        @Override
        public boolean endOfChain( RelationshipRecord rel )
        {
            return rel.isFirstInFirstChain();
        }
    },
    SECOND_NEXT
    {
        @Override
        public long relOf( RelationshipRecord rel )
        {
            return rel.getSecondNextRel();
        }

        @Override
        public boolean endOfChain( RelationshipRecord rel )
        {
            return rel.getSecondNextRel() == Record.NO_NEXT_RELATIONSHIP.intValue();
        }
    },
    SECOND_PREV
    {
        @Override
        public long relOf( RelationshipRecord rel )
        {
            return rel.getSecondPrevRel();
        }

        @Override
        public boolean endOfChain( RelationshipRecord rel )
        {
            return rel.isFirstInSecondChain();
        }
    };

    public abstract long relOf( RelationshipRecord rel );

    public abstract boolean endOfChain( RelationshipRecord rel );
}
