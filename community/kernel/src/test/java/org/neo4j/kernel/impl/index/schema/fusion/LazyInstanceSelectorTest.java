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

import org.junit.jupiter.api.Test;

import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.GENERIC;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.LUCENE;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.values;

class LazyInstanceSelectorTest
{
    @Test
    void shouldInstantiateLazilyOnFirstSelect()
    {
        // given
        Function<IndexSlot,String> factory = slotToStringFunction();
        LazyInstanceSelector<String> selector = new LazyInstanceSelector<>( factory );

        // when
        for ( IndexSlot slot : values() )
        {
            for ( IndexSlot candidate : values() )
            {
                // then
                if ( candidate.ordinal() < slot.ordinal() )
                {
                    verify( factory ).apply( candidate );
                    selector.select( candidate );
                    verify( factory ).apply( candidate );
                }
                else if ( candidate == slot )
                {
                    verify( factory, times( 0 ) ).apply( candidate );
                    selector.select( candidate );
                    verify( factory ).apply( candidate );
                }
                else
                {
                    assertNull( selector.getIfInstantiated( candidate ) );
                }
            }
        }
    }

    @Test
    void shouldPerformActionOnAll()
    {
        // given
        Function<IndexSlot,String> factory = slotToStringFunction();
        LazyInstanceSelector<String> selector = new LazyInstanceSelector<>( factory );
        selector.select( GENERIC );

        // when
        Consumer<String> consumer = mock( Consumer.class );
        selector.forAll( consumer );

        // then
        for ( IndexSlot slot : IndexSlot.values() )
        {
            verify( consumer ).accept( String.valueOf( slot ) );
        }
        verifyNoMoreInteractions( consumer );
    }

    @Test
    void shouldCloseAllInstantiated()
    {
        // given
        Function<IndexSlot,String> factory = slotToStringFunction();
        LazyInstanceSelector<String> selector = new LazyInstanceSelector<>( factory );
        selector.select( LUCENE );
        selector.select( GENERIC );

        // when
        Consumer<String> consumer = mock( Consumer.class );
        selector.close( consumer );

        // then
        verify( consumer ).accept( String.valueOf( LUCENE ) );
        verify( consumer ).accept( String.valueOf( GENERIC ) );
        verifyNoMoreInteractions( consumer );
    }

    @Test
    void shouldPreventInstantiationAfterClose()
    {
        // given
        Function<IndexSlot,String> factory = slotToStringFunction();
        LazyInstanceSelector<String> selector = new LazyInstanceSelector<>( factory );
        selector.select( LUCENE );

        // when
        selector.close( mock( Consumer.class ) );

        // then
        try
        {
            selector.select( GENERIC );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // then good
        }
    }

    private static Function<IndexSlot,String> slotToStringFunction()
    {
        Function<IndexSlot,String> factory = mock( Function.class );
        when( factory.apply( any( IndexSlot.class ) ) ).then( invocationOnMock -> String.valueOf( (IndexSlot) invocationOnMock.getArgument( 0 ) ) );
        return factory;
    }
}
