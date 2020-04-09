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
package org.neo4j.memory;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.memory.MemoryGroup.QUERY_CACHE;
import static org.neo4j.memory.MemoryGroup.TRANSACTION;

class MemoryPoolsTest
{

    @Test
    void createdPoolRegisteredInListOfPools()
    {
        var pools = new MemoryPools();
        var pool1 = pools.pool( QUERY_CACHE, "test", 2, true );
        var pool2 = pools.pool( TRANSACTION, "test", 2, true );
        assertThat( pools.getPools() ).contains( pool1, pool2 );
    }

    @Test
    void poolIsDeregisteredOnClose()
    {
        var pools = new MemoryPools();
        var pool = pools.pool( TRANSACTION, "test", 2, true );

        assertThat( pools.getPools() ).contains( pool );
        pool.close();
        assertThat( pools.getPools() ).isEmpty();
    }
}
