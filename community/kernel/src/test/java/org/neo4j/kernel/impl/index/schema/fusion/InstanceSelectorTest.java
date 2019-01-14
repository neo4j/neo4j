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
package org.neo4j.kernel.impl.index.schema.fusion;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class InstanceSelectorTest
{
    @Test
    public void shouldSelect()
    {
        // given
        InstanceSelector<String> selector = selector( "0", "1" );

        // when
        String select0 = selector.select( 0 );
        // then
        assertEquals( "0", select0 );

        // when
        String select1 = selector.select( 1 );
        // then
        assertEquals( "1", select1 );
    }

    @Test
    public void shouldThrowOnNonInstantiatedSelect()
    {
        // given
        InstanceSelector<String> selector = selector( "0", null );

        try
        {
            // when
            selector.select( 1 );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // then
            // good
        }
    }

    @Test
    public void shouldThrowOnNonInstantiatedInstancesAs()
    {
        // given
        InstanceSelector<String> selector = selector( "0", null );

        // when
        try
        {
            selector.instancesAs( new Number[2], Integer::parseInt );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // then
            // good
        }
    }

    @Test
    public void shouldInstancesAs()
    {
        // given
        InstanceSelector<String> selector = selector( "0", "1" );

        // when
        Number[] numbers = selector.instancesAs( new Number[2], Integer::parseInt );

        // then
        assertEquals( 0, numbers[0] );
        assertEquals( 1, numbers[1] );
    }

    @Test
    public void shouldThrowOnNonInstantiatedForAll()
    {
        // given
        InstanceSelector<String> selector = selector( "0", null );

        // when
        try
        {
            selector.forAll( Integer::parseInt );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // then
            // good
        }
    }

    @Test
    public void shouldForAll()
    {
        // given
        InstanceSelector<String> selector = selector( "0", "1" );

        // when
        MutableInt count = new MutableInt();
        selector.forAll( s -> count.increment() );

        // then
        assertEquals( 2, count.intValue() );
    }

    @Test
    public void shouldNotThrowOnNonInstantiatedForAll()
    {
        // given
        InstanceSelector<String> selector = selector( (String) null );

        // when
        selector.close( Integer::parseInt );

        // then
        // good
    }

    @Test
    public void shouldCloseAll()
    {
        // given
        InstanceSelector<String> selector = selector( "0", "1" );

        // when
        MutableInt count = new MutableInt();
        selector.close( s -> count.increment() );

        // then
        assertEquals( 2, count.intValue() );
    }

    private InstanceSelector<String> selector( String... strings )
    {
        return new InstanceSelector<>( strings );
    }
}
