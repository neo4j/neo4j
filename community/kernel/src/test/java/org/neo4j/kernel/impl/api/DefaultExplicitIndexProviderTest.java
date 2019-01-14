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
package org.neo4j.kernel.impl.api;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.spi.explicitindex.IndexImplementation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class DefaultExplicitIndexProviderTest
{
    private final DefaultExplicitIndexProvider provider = new DefaultExplicitIndexProvider();

    @Test
    void registerAndAccessIndexProvider()
    {
        IndexImplementation index = mock( IndexImplementation.class );
        String testProviderName = "a";
        provider.registerIndexProvider( testProviderName, index );
        assertSame( index, provider.getProviderByName( testProviderName ) );
    }

    @Test
    void throwOnAttemptToRegisterProviderWithSameName()
    {
        IndexImplementation index = mock( IndexImplementation.class );
        String testProviderName = "a";
        provider.registerIndexProvider( testProviderName, index );
        assertThrows( IllegalArgumentException.class, () -> provider.registerIndexProvider( testProviderName, index ) );
    }

    @Test
    void unregisterIndexProvider()
    {
        IndexImplementation index = mock( IndexImplementation.class );
        String testProviderName = "b";
        provider.registerIndexProvider( testProviderName, index );
        assertTrue( provider.unregisterIndexProvider( testProviderName ) );
    }

    @Test
    void removeNotExistentProvider()
    {
        assertFalse( provider.unregisterIndexProvider( "c" ) );
    }

    @Test
    void throwOnAttemptToGetNonRegisteredProviderByName()
    {
        String testProviderName = "d";
        assertThrows( IllegalArgumentException.class, () -> provider.getProviderByName( testProviderName ) );
    }

    @Test
    void accessAllRegisteredIndexProviders()
    {
        IndexImplementation index1 = mock( IndexImplementation.class );
        IndexImplementation index2 = mock( IndexImplementation.class );
        String testProviderName1 = "e";
        String testProviderName2 = "f";
        provider.registerIndexProvider( testProviderName1, index1 );
        provider.registerIndexProvider( testProviderName2, index2 );

        assertThat( provider.allIndexProviders(), contains( index1, index2 ) );
    }
}
