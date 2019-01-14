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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.Test;

import org.neo4j.kernel.api.index.IndexProvider;

import static java.util.Collections.singletonList;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultIndexProviderMapTest
{
    @Test
    public void shouldNotSupportMultipleProvidersWithSameDescriptor()
    {
        // given
        IndexProvider.Descriptor descriptor = new IndexProvider.Descriptor( "provider", "1.2" );
        IndexProvider provider1 = mock( IndexProvider.class );
        when( provider1.getProviderDescriptor() ).thenReturn( descriptor );
        IndexProvider provider2 = mock( IndexProvider.class );
        when( provider2.getProviderDescriptor() ).thenReturn( descriptor );

        // when
        try
        {
            new DefaultIndexProviderMap( provider1, singletonList( provider2 ) );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // then good
        }
    }

    @Test
    public void shouldThrowOnLookupOnUnknownProvider()
    {
        // given
        IndexProvider provider = mock( IndexProvider.class );
        when( provider.getProviderDescriptor() ).thenReturn( new IndexProvider.Descriptor( "provider", "1.2" ) );

        // when
        try
        {
            new DefaultIndexProviderMap( provider ).lookup( new IndexProvider.Descriptor( "provider2", "1.2" ) );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // then good
        }
    }
}
