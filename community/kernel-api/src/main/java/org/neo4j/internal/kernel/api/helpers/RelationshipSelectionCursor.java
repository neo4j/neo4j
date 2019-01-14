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
package org.neo4j.internal.kernel.api.helpers;

/**
 * Helper cursor for traversing specific types and directions.
 */
public interface RelationshipSelectionCursor extends AutoCloseable
{
    boolean next();

    @Override
    void close();

    long relationshipReference();

    int type();

    long otherNodeReference();

    long sourceNodeReference();

    long targetNodeReference();

    long propertiesReference();

    final class EMPTY implements RelationshipSelectionCursor
    {
        @Override
        public boolean next()
        {
            return false;
        }

        @Override
        public void close()
        {

        }

        @Override
        public long relationshipReference()
        {
            return -1;
        }

        @Override
        public int type()
        {
            return -1;
        }

        @Override
        public long otherNodeReference()
        {
            return -1;
        }

        @Override
        public long sourceNodeReference()
        {
            return -1;
        }

        @Override
        public long targetNodeReference()
        {
            return -1;
        }

        @Override
        public long propertiesReference()
        {
            return -1;
        }
    }

    RelationshipSelectionCursor EMPTY = new EMPTY();
}
