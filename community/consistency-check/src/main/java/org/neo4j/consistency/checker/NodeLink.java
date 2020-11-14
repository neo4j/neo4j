/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.consistency.checker;

import org.neo4j.kernel.impl.store.record.RelationshipRecord;

/**
 * Means of parameterizing selection of {@link RelationshipRecord} pointers.
 */
public enum NodeLink
{
    SOURCE
    {
        @Override
        public void setPrevRel( RelationshipRecord record, long other )
        {
            record.setFirstPrevRel( other );
        }

        @Override
        public void setNextRel( RelationshipRecord record, long other )
        {
            record.setFirstNextRel( other );
        }

        @Override
        public long getPrevRel( RelationshipRecord relationship )
        {
            return relationship.getFirstPrevRel();
        }

        @Override
        public long getNextRel( RelationshipRecord relationship )
        {
            return relationship.getFirstNextRel();
        }

        @Override
        public void setNode( RelationshipRecord relationship, long nodeId )
        {
            relationship.setFirstNode( nodeId );
        }
    },
    TARGET
    {
        @Override
        public void setPrevRel( RelationshipRecord record, long other )
        {
            record.setSecondPrevRel( other );
        }

        @Override
        public void setNextRel( RelationshipRecord record, long other )
        {
            record.setSecondNextRel( other );
        }

        @Override
        public long getPrevRel( RelationshipRecord relationship )
        {
            return relationship.getSecondPrevRel();
        }

        @Override
        public long getNextRel( RelationshipRecord relationship )
        {
            return relationship.getSecondNextRel();
        }

        @Override
        public void setNode( RelationshipRecord relationship, long nodeId )
        {
            relationship.setSecondNode( nodeId );
        }
    };

    public static NodeLink select( RelationshipRecord record, long node )
    {
        if ( record.getFirstNode() == node )
        {
            return SOURCE;
        }
        if ( record.getSecondNode() == node )
        {
            return TARGET;
        }
        return null;
    }

    public abstract void setPrevRel( RelationshipRecord record, long other );

    public abstract void setNextRel( RelationshipRecord record, long other );

    public abstract long getPrevRel( RelationshipRecord relationship );

    public abstract long getNextRel( RelationshipRecord relationship );

    public abstract void setNode( RelationshipRecord relationship, long nodeId );
}
