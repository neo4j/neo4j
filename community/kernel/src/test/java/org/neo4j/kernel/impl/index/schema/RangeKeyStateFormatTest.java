/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.io.pagecache.PageCursor;

class RangeKeyStateFormatTest extends GenericKeyStateFormatTest<RangeKey>
{
    @Override
    protected String zipName()
    {
        return "current-range-key-state-format.zip";
    }

    @Override
    protected String storeFileName()
    {
        return "range-key-state-store";
    }

    @Override
    Layout<RangeKey> getLayout()
    {
        RangeLayout rangeLayout = new RangeLayout( NUMBER_OF_SLOTS );
        return new Layout<>()
        {
            @Override
            public RangeKey newKey()
            {
                return rangeLayout.newKey();
            }

            @Override
            public void readKey( PageCursor cursor, RangeKey into, int keySize )
            {
                rangeLayout.readKey( cursor, into, keySize );
            }

            @Override
            public void writeKey( PageCursor cursor, RangeKey key )
            {
                rangeLayout.writeKey( cursor, key );
            }

            @Override
            public int compare( RangeKey k1, RangeKey k2 )
            {
                return rangeLayout.compare( k1, k2 );
            }

            @Override
            public int majorVersion()
            {
                return rangeLayout.majorVersion();
            }

            @Override
            public int minorVersion()
            {
                return rangeLayout.minorVersion();
            }
        };
    }
}
