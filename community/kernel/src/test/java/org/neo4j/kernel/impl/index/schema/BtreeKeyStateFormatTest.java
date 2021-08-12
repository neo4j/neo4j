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

import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;

class BtreeKeyStateFormatTest extends GenericKeyStateFormatTest<BtreeKey>
{
    @Override
    protected String zipName()
    {
        return "current-btree-key-state-format.zip";
    }

    @Override
    protected String storeFileName()
    {
        return "btree-key-state-store";
    }

    @Override
    Layout<BtreeKey> getLayout()
    {
        GenericLayout genericLayout = new GenericLayout( NUMBER_OF_SLOTS, IndexSpecificSpaceFillingCurveSettings.fromConfig( Config.defaults() ) );
        return new Layout<>()
        {
            @Override
            public BtreeKey newKey()
            {
                return genericLayout.newKey();
            }

            @Override
            public void readKey( PageCursor cursor, BtreeKey into, int keySize )
            {
                genericLayout.readKey( cursor, into, keySize );
            }

            @Override
            public void writeKey( PageCursor cursor, BtreeKey key )
            {
                genericLayout.writeKey( cursor, key );
            }

            @Override
            public int compare( BtreeKey k1, BtreeKey k2 )
            {
                return genericLayout.compare( k1, k2 );
            }

            @Override
            public int majorVersion()
            {
                return genericLayout.majorVersion();
            }

            @Override
            public int minorVersion()
            {
                return genericLayout.minorVersion();
            }
        };
    }
}
