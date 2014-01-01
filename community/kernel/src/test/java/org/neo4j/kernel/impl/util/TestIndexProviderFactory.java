/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.io.IOException;

import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static org.powermock.api.mockito.PowerMockito.mock;

public class TestIndexProviderFactory extends KernelExtensionFactory<TestIndexProviderFactory.Dependencies>
{
    public static final String KEY = "test";

    public static final SchemaIndexProvider.Descriptor PROVIDER_DESCRIPTOR =
            new SchemaIndexProvider.Descriptor( KEY, "1.0" );

    private final SchemaIndexProvider indexProvider;

    public static class TestIndexProvider extends SchemaIndexProvider
    {

        public TestIndexProvider()
        {
            super( PROVIDER_DESCRIPTOR, 10 );
        }

        @Override
        public IndexPopulator getPopulator( long indexId, IndexConfiguration config )
        {
            return mock(IndexPopulator.class);
        }

        @Override
        public IndexAccessor getOnlineAccessor( long indexId, IndexConfiguration config ) throws IOException
        {
            return mock(IndexAccessor.class);
        }

        @Override
        public String getPopulationFailure( long indexId ) throws IllegalStateException
        {
            return null;
        }

        @Override
        public InternalIndexState getInitialState( long indexId )
        {
            return InternalIndexState.FAILED;
        }
    }

    public interface Dependencies
    {
    }

    public TestIndexProviderFactory( SchemaIndexProvider singleProvider )
    {
        super( KEY );
        if ( singleProvider == null )
        {
            throw new IllegalArgumentException( "Null provider" );
        }
        this.indexProvider = singleProvider;
    }

    @Override
    public Lifecycle newKernelExtension( Dependencies dependencies ) throws Throwable
    {
        return indexProvider;
    }
}
