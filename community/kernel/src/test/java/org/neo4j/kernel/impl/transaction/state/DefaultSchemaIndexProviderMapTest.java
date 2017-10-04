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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.Test;

import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static java.util.Arrays.asList;

public class DefaultSchemaIndexProviderMapTest
{
    @Test
    public void shouldNotSupportMultipleProvidersWithSameDescriptor() throws Exception
    {
        // given
        SchemaIndexProvider.Descriptor descriptor = new SchemaIndexProvider.Descriptor( "provider", "1.2" );
        SchemaIndexProvider provider1 = mock( SchemaIndexProvider.class );
        when( provider1.getProviderDescriptor() ).thenReturn( descriptor );
        SchemaIndexProvider provider2 = mock( SchemaIndexProvider.class );
        when( provider2.getProviderDescriptor() ).thenReturn( descriptor );

        // when
        try
        {
            new DefaultSchemaIndexProviderMap( provider1, asList( provider2 ) );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // then good
        }
    }

    @Test
    public void shouldThrowOnLookupOnUnknownProvider() throws Exception
    {
        // given
        SchemaIndexProvider provider = mock( SchemaIndexProvider.class );
        when( provider.getProviderDescriptor() ).thenReturn( new SchemaIndexProvider.Descriptor( "provider", "1.2" ) );

        // when
        SchemaIndexProviderMap map = new DefaultSchemaIndexProviderMap( provider );
        try
        {
            new DefaultSchemaIndexProviderMap( provider ).apply( new SchemaIndexProvider.Descriptor( "provider2", "1.2" ) );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // then good
        }
    }
}
