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

import org.junit.Test;

import java.util.function.IntFunction;

import org.neo4j.function.ThrowingConsumer;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.INSTANCE_COUNT;

public class LazyInstanceSelectorTest
{
    @Test
    public void shouldInstantiateLazilyOnFirstSelect()
    {
        // given
        IntFunction<String> factory = mock( IntFunction.class );
        when( factory.apply( anyInt() ) ).then( invocationOnMock -> String.valueOf( (Integer) invocationOnMock.getArgument( 0 ) ) );
        LazyInstanceSelector<String> selector = new LazyInstanceSelector<>( new String[INSTANCE_COUNT], factory );

        // when
        for ( int slot = 0; slot < INSTANCE_COUNT; slot++ )
        {
            for ( int candidate = 0; candidate < INSTANCE_COUNT; candidate++ )
            {
                // then
                if ( candidate < slot )
                {
                    verify( factory, times( 1 ) ).apply( candidate );
                    selector.select( candidate );
                    verify( factory, times( 1 ) ).apply( candidate );
                }
                else if ( candidate == slot )
                {
                    verify( factory, times( 0 ) ).apply( candidate );
                    selector.select( candidate );
                    verify( factory, times( 1 ) ).apply( candidate );
                }
                else
                {
                    assertNull( selector.getIfInstantiated( candidate ) );
                }
            }
        }
    }

    @Test
    public void shouldPerformActionOnAll()
    {
        // given
        IntFunction<String> factory = mock( IntFunction.class );
        when( factory.apply( anyInt() ) ).then( invocationOnMock -> String.valueOf( (Integer) invocationOnMock.getArgument( 0 ) ) );
        LazyInstanceSelector<String> selector = new LazyInstanceSelector<>( new String[INSTANCE_COUNT], factory );
        selector.select( 1 );

        // when
        ThrowingConsumer<String,RuntimeException> consumer = mock( ThrowingConsumer.class );
        selector.forAll( consumer );

        // then
        for ( int slot = 0; slot < INSTANCE_COUNT; slot++ )
        {
            verify( consumer, times( 1 ) ).accept( String.valueOf( slot ) );
        }
        verifyNoMoreInteractions( consumer );
    }

    @Test
    public void shouldCloseAllInstantiated()
    {
        // given
        IntFunction<String> factory = mock( IntFunction.class );
        when( factory.apply( anyInt() ) ).then( invocationOnMock -> String.valueOf( (Integer) invocationOnMock.getArgument( 0 ) ) );
        LazyInstanceSelector<String> selector = new LazyInstanceSelector<>( new String[INSTANCE_COUNT], factory );
        selector.select( 1 );
        selector.select( 3 );

        // when
        ThrowingConsumer<String,RuntimeException> consumer = mock( ThrowingConsumer.class );
        selector.close( consumer );

        // then
        verify( consumer, times( 1 ) ).accept( "1" );
        verify( consumer, times( 1 ) ).accept( "3" );
        verifyNoMoreInteractions( consumer );
    }

    @Test
    public void shouldPreventInstantiationAfterClose()
    {
        // given
        IntFunction<String> factory = mock( IntFunction.class );
        when( factory.apply( anyInt() ) ).then( invocationOnMock -> String.valueOf( (Integer) invocationOnMock.getArgument( 0 ) ) );
        LazyInstanceSelector<String> selector = new LazyInstanceSelector<>( new String[INSTANCE_COUNT], factory );
        selector.select( 1 );
        selector.select( 3 );

        // when
        selector.close( mock( ThrowingConsumer.class ) );

        // then
        try
        {
            selector.select( 0 );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // then good
        }
    }
}
