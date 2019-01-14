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
package org.neo4j.kernel.impl.locking;

import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LockGroupTest
{
    @Test
    public void shouldReleaseAllLocksWhenExitingTheLockGroupRegion()
    {
        // given
        Lock lock1 = mock( Lock.class );
        Lock lock2 = mock( Lock.class );
        Lock lock3 = mock( Lock.class );

        // when
        try ( LockGroup locks = new LockGroup() )
        {
            locks.add( lock1 );
            locks.add( lock2 );
            locks.add( lock3 );
        }

        // then
        verify( lock1, times( 1 ) ).release();
        verify( lock2, times( 1 ) ).release();
        verify( lock3, times( 1 ) ).release();
    }
}
