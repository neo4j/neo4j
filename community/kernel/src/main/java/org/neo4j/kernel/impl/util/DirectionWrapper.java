/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

public enum DirectionWrapper
{
    OUTGOING( Direction.OUTGOING )
    {
        @Override
        public long getNextRel( RelationshipGroupRecord group )
        {
            return group.getFirstOut();
        }

        @Override
        public void setNextRel( RelationshipGroupRecord group, long firstNextRel )
        {
            group.setFirstOut( firstNextRel );
        }
    },
    INCOMING( Direction.INCOMING )
    {
        @Override
        public long getNextRel( RelationshipGroupRecord group )
        {
            return group.getFirstIn();
        }

        @Override
        public void setNextRel( RelationshipGroupRecord group, long firstNextRel )
        {
            group.setFirstIn( firstNextRel );
        }
    },
    BOTH( Direction.BOTH )
    {
        @Override
        public long getNextRel( RelationshipGroupRecord group )
        {
            return group.getFirstLoop();
        }

        @Override
        public void setNextRel( RelationshipGroupRecord group, long firstNextRel )
        {
            group.setFirstLoop( firstNextRel );
        }
    };

    private final Direction direction;

    private DirectionWrapper( Direction direction )
    {
        this.direction = direction;
    }

    public Direction direction()
    {
        return this.direction;
    }

    public abstract long getNextRel( RelationshipGroupRecord group );

    public abstract void setNextRel( RelationshipGroupRecord group, long firstNextRel );

    public static DirectionWrapper wrap( Direction direction )
    {
        switch ( direction )
        {
            case OUTGOING: return DirectionWrapper.OUTGOING;
            case INCOMING: return DirectionWrapper.INCOMING;
            case BOTH: return DirectionWrapper.BOTH;
            default: throw new IllegalArgumentException( "" + direction );
        }
    }
}
