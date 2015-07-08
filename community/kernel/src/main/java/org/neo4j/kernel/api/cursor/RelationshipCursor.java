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
package org.neo4j.kernel.api.cursor;

/**
 * Cursor for iterating over a set of relationships.
 */
public interface RelationshipCursor
        extends EntityCursor
{
    RelationshipCursor EMPTY = new RelationshipCursor()
    {
        @Override
        public long getId()
        {
            throw new IllegalStateException();
        }

        @Override
        public int getType()
        {
            throw new IllegalStateException();
        }

        @Override
        public long getStartNode()
        {
            throw new IllegalStateException();
        }

        @Override
        public long getEndNode()
        {
            throw new IllegalStateException();
        }

        @Override
        public long getOtherNode( long nodeId )
        {
            throw new IllegalStateException(  );
        }

        @Override
        public PropertyCursor properties()
        {
            throw new IllegalStateException();
        }

        @Override
        public boolean next()
        {
            return false;
        }

        @Override
        public void close()
        {

        }
    };

    /**
     * @return relationship type for current relationship
     */
    int getType();

    long getStartNode();

    long getEndNode();

    long getOtherNode( long nodeId );
}
