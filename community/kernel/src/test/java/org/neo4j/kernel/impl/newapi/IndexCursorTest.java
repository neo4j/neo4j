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
package org.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.api.index.IndexProgressor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexCursorTest
{
    @Test
    void shouldClosePreviousBeforeReinitialize()
    {
        // given
        StubIndexCursor cursor = new StubIndexCursor();
        StubProgressor progressor = new StubProgressor();
        cursor.initialize( progressor );
        assertFalse( progressor.isClosed, "open before re-initialize" );

        // when
        StubProgressor otherProgressor = new StubProgressor();
        cursor.initialize( otherProgressor );

        // then
        assertTrue( progressor.isClosed, "closed after re-initialize" );
        assertFalse( otherProgressor.isClosed, "new still open" );
    }

    private static class StubIndexCursor extends IndexCursor<StubProgressor>
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
        public boolean isClosed()
        {
            return false;
        }
    }

    private static class StubProgressor implements IndexProgressor
    {
        boolean isClosed;

        StubProgressor()
        {
            isClosed = false;
        }

        @Override
        public boolean next()
        {
            return false;
        }

        @Override
        public void close()
        {
            isClosed = true;
        }
    }
}
