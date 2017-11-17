/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl.muninn;

import org.junit.Test;

import org.neo4j.io.ByteUnit;
import org.neo4j.unsafe.impl.internal.dragons.MemoryManager;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class LargePageListTest
{
    private static final long ALIGNMENT = 8;

    @Test
    public void veryLargePageListsMustBeFullyAccessible() throws Exception
    {
        long pageCacheSize = ByteUnit.tebiBytes( 1 );
        int pageSize = (int) ByteUnit.kibiBytes( 8 );
        int pages = Math.toIntExact( pageCacheSize / pageSize );

        MemoryManager mman = new MemoryManager( ByteUnit.mebiBytes( 1 ), ALIGNMENT );
        SwapperSet swappers = new SwapperSet();
        long victimPage = VictimPageReference.getVictimPage( pageSize );

        PageList pageList = new PageList( pages, pageSize, mman, swappers, victimPage );
        assertThat( pageList.getPageCount(), is( pages ) );
        long ref = pageList.deref( pages - 1 );
        pageList.incrementUsage( ref );
        pageList.incrementUsage( ref );
        assertFalse( pageList.decrementUsage( ref ) );
        assertTrue( pageList.decrementUsage( ref ) );
        System.out.println( "mman.sumUsedMemory() = " + mman.sumUsedMemory() );
    }
}
