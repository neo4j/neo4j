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
package org.neo4j.kernel.extension.dependency;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings.LabelIndex;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.Iterables.iterable;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class NamedLabelScanStoreSelectionStrategyTest
{
    @Test
    public void shouldSelectSpecificallyConfiguredProviderWhenSingle() throws Exception
    {
        // GIVEN
        NamedLabelScanStoreSelectionStrategy strategy = strategy( LabelIndex.NATIVE );
        LabelScanStoreProvider single = provider( LabelIndex.NATIVE, store( false ) );

        // WHEN
        LabelScanStoreProvider selected = select( strategy, single );

        // THEN
        assertSame( single, selected );
    }

    @Test
    public void shouldSelectSpecificallyConfiguredProviderAmongMultiple() throws Exception
    {
        // GIVEN
        NamedLabelScanStoreSelectionStrategy strategy = strategy( LabelIndex.LUCENE );
        LabelScanStoreProvider nativeProvider = provider( LabelIndex.NATIVE, store( false ) );
        LabelScanStoreProvider luceneProvider = provider( LabelIndex.LUCENE, store( false ) );

        // WHEN
        LabelScanStoreProvider selected = select( strategy, nativeProvider, luceneProvider );

        // THEN
        assertSame( luceneProvider, selected );
    }

    @Test
    public void shouldFailOnMissingSpecificallyConfiguredProvider() throws Exception
    {
        // GIVEN
        NamedLabelScanStoreSelectionStrategy strategy = strategy( LabelIndex.LUCENE );
        LabelScanStoreProvider nativeProvider = provider( LabelIndex.NATIVE, store( false ) );

        // WHEN
        try
        {
            select( strategy, nativeProvider );
            fail( "Should've failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN good
        }
    }

    @Test
    public void shouldAutoSelectSingleProviderWithPresentStore() throws Exception
    {
        // GIVEN
        NamedLabelScanStoreSelectionStrategy strategy = strategy( LabelIndex.AUTO );
        LabelScanStoreProvider nativeProvider = provider( LabelIndex.NATIVE, store( true ) );

        // WHEN
        LabelScanStoreProvider selected = select( strategy, nativeProvider );

        // THEN
        assertSame( nativeProvider, selected );
    }

    @Test
    public void shouldAutoSelectSingleProviderWithPresentStoreAmongMultipleProviders() throws Exception
    {
        // GIVEN
        NamedLabelScanStoreSelectionStrategy strategy = strategy( LabelIndex.AUTO );
        LabelScanStoreProvider nativeProvider = provider( LabelIndex.NATIVE, store( false ) );
        LabelScanStoreProvider luceneProvider = provider( LabelIndex.LUCENE, store( true ) );

        // WHEN
        LabelScanStoreProvider selected = select( strategy, nativeProvider, luceneProvider );

        // THEN
        assertSame( luceneProvider, selected );
    }

    @Test
    public void shouldAutoSelectDefaultProviderIfNoProviderWithPresentStore() throws Exception
    {
        // GIVEN
        NamedLabelScanStoreSelectionStrategy strategy = strategy( LabelIndex.AUTO );
        LabelScanStoreProvider nativeProvider = provider( LabelIndex.NATIVE, store( false ) );
        LabelScanStoreProvider luceneProvider = provider( LabelIndex.LUCENE, store( false ) );

        // WHEN
        LabelScanStoreProvider selected = select( strategy, nativeProvider, luceneProvider );

        // THEN
        assertEquals( GraphDatabaseSettings.label_index.getDefaultValue(), selected.getName() );
    }

    @Test
    public void shouldFailAutoSelectIfMultipleProvidersWithPresentStore() throws Exception
    {
        // GIVEN
        NamedLabelScanStoreSelectionStrategy strategy = strategy( LabelIndex.AUTO );
        LabelScanStoreProvider nativeProvider = provider( LabelIndex.NATIVE, store( true ) );
        LabelScanStoreProvider luceneProvider = provider( LabelIndex.LUCENE, store( true ) );

        // WHEN
        try
        {
            select( strategy, nativeProvider, luceneProvider );
            fail( "Should've failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN good
        }
    }

    @Test
    public void shouldFailAutoSelectIfNoProviderWithPresentStoreAndSomeReportedIOException() throws Exception
    {
        // GIVEN
        NamedLabelScanStoreSelectionStrategy strategy = strategy( LabelIndex.AUTO );
        LabelScanStoreProvider nativeProvider = provider( LabelIndex.NATIVE, store( false ) );
        LabelScanStoreProvider luceneProvider = provider( LabelIndex.LUCENE, store( null ) );

        // WHEN
        try
        {
            select( strategy, nativeProvider, luceneProvider );
            fail( "Should've failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN good
            assertTrue( e.getCause() instanceof IOException );
        }
    }

    @Test
    public void shouldAutoSelectProviderWithPresentStoreEvenIfSomeOtherReportedIOException() throws Exception
    {
        // GIVEN
        NamedLabelScanStoreSelectionStrategy strategy = strategy( LabelIndex.AUTO );
        LabelScanStoreProvider nativeProvider = provider( LabelIndex.NATIVE, store( true ) );
        LabelScanStoreProvider luceneProvider = provider( LabelIndex.LUCENE, store( null ) );

        // WHEN
        LabelScanStoreProvider selected = select( strategy, nativeProvider, luceneProvider );

        // THEN
        assertSame( nativeProvider, selected );
    }

    private NamedLabelScanStoreSelectionStrategy strategy( LabelIndex index )
    {
        return new NamedLabelScanStoreSelectionStrategy( Config.defaults().augment( stringMap(
                GraphDatabaseSettings.label_index.name(), index.name() ) ) );
    }

    private LabelScanStoreProvider provider( LabelIndex index, LabelScanStore store )
    {
        return new LabelScanStoreProvider( index.name(), store );
    }

    private LabelScanStore store( Boolean present ) throws IOException
    {
        LabelScanStore store = mock( LabelScanStore.class );
        if ( present == null )
        {
            when( store.hasStore() ).thenThrow( IOException.class );
        }
        else
        {
            when( store.hasStore() ).thenReturn( present );
        }
        return store;
    }

    private LabelScanStoreProvider select( NamedLabelScanStoreSelectionStrategy strategy,
            LabelScanStoreProvider... among )
    {
        return strategy.select( LabelScanStoreProvider.class, iterable( among ) );
    }
}
