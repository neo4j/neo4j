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

import org.neo4j.cursor.Cursor;
import org.neo4j.function.ToIntFunction;

/**
 * Cursor for iterating over all the labels on a node.
 */
public interface LabelCursor
        extends Cursor
{
    ToIntFunction<LabelCursor> GET_LABEL = new ToIntFunction<LabelCursor>()
    {
        @Override
        public int apply( LabelCursor cursor )
        {
            return cursor.getLabel();
        }
    };

    LabelCursor EMPTY = new LabelCursor()
    {
        @Override
        public boolean seek( int labelId )
        {
            return false;
        }

        @Override
        public int getLabel()
        {
            throw new IllegalStateException(  );
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
     * Move the cursor to a particular label.
     *
     * @param labelId to search for
     * @return true if found
     */
    boolean seek( int labelId );

    /**
     * @return id of current label
     */
    int getLabel();
}
