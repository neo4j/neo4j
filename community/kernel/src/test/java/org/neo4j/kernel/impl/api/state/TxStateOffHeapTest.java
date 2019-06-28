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

package org.neo4j.kernel.impl.api.state;

import org.junit.jupiter.api.AfterAll;

import org.neo4j.kernel.impl.util.collection.CachingOffHeapBlockAllocator;
import org.neo4j.kernel.impl.util.collection.CollectionsFactory;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.impl.util.collection.OffHeapCollectionsFactory;

class TxStateOffHeapTest extends TxStateTest
{
    private static final CachingOffHeapBlockAllocator BLOCK_ALLOCATOR = new CachingOffHeapBlockAllocator();

    TxStateOffHeapTest()
    {
        super( new CollectionsFactorySupplier()
        {
            @Override
            public CollectionsFactory create()
            {
                return new OffHeapCollectionsFactory( BLOCK_ALLOCATOR );
            }

            @Override
            public String toString()
            {
                return "Off heap";
            }
        } );
    }

    @AfterAll
    static void afterAll()
    {
        BLOCK_ALLOCATOR.release();
    }
}
