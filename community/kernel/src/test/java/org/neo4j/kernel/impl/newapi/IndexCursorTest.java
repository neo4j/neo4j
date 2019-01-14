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

import org.junit.Test;

import org.neo4j.storageengine.api.schema.IndexProgressor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexCursorTest
{
    @Test
    public void shouldClosePreviousBeforeReinitialize()
    {
        // given
        StubIndexCursor cursor = new StubIndexCursor();
        StubProgressor progressor = new StubProgressor();
        cursor.initialize( progressor );
        assertFalse( "open before re-initialize", progressor.isClosed );

        // when
        StubProgressor otherProgressor = new StubProgressor();
        cursor.initialize( otherProgressor );

        // then
        assertTrue( "closed after re-initialize", progressor.isClosed );
        assertFalse( "new still open", otherProgressor.isClosed );
    }

    private static class StubIndexCursor extends IndexCursor<StubProgressor>
    {
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
