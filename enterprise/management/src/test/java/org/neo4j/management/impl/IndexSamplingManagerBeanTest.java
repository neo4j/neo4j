/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.management.impl;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexSamplingManagerBeanTest
{
    private static final String EXISTING_LABEL = "label";
    private static final String NON_EXISTING_LABEL = "bogusLabel";
    private static final int LABEL_ID = 42;
    private static final String EXISTING_PROPERTY = "prop";
    private static final String NON_EXISTING_PROPERTY = "bogusProp";
    private static final int PROPERTY_ID = 43;

    private NeoStoreDataSource dataSource;
    private IndexingService indexingService;

    @Before
    public void setup()
    {
        dataSource = mock( NeoStoreDataSource.class );
        StorageEngine storageEngine = mock( StorageEngine.class );
        StorageReader storageReader = mock( StorageReader.class );
        when( storageEngine.newReader() ).thenReturn( storageReader );
        indexingService = mock( IndexingService.class );
        when( storageReader.labelGetForName( EXISTING_LABEL ) ).thenReturn( LABEL_ID );
        when( storageReader.propertyKeyGetForName( EXISTING_PROPERTY ) ).thenReturn( PROPERTY_ID );
        when( storageReader.propertyKeyGetForName( NON_EXISTING_PROPERTY ) ).thenReturn( -1 );
        when( storageReader.labelGetForName( NON_EXISTING_LABEL ) ).thenReturn( -1 );
        DependencyResolver resolver = mock( DependencyResolver.class );
        when( resolver.resolveDependency( IndexingService.class ) ).thenReturn( indexingService );
        when( resolver.resolveDependency( StorageEngine.class ) ).thenReturn( storageEngine );
        when( dataSource.getDependencyResolver() ).thenReturn( resolver );
    }

    @Test
    public void samplingTriggeredWhenIdsArePresent() throws IndexNotFoundKernelException
    {
        // Given
        IndexSamplingManagerBean.StoreAccess storeAccess = new IndexSamplingManagerBean.StoreAccess();
        storeAccess.registered( dataSource );

        // When
        storeAccess.triggerIndexSampling( EXISTING_LABEL, EXISTING_PROPERTY, false );

        // Then
        verify( indexingService, times( 1 ) ).triggerIndexSampling(
                SchemaDescriptorFactory.forLabel( LABEL_ID, PROPERTY_ID ) ,
                IndexSamplingMode.TRIGGER_REBUILD_UPDATED);
    }

    @Test
    public void forceSamplingTriggeredWhenIdsArePresent() throws IndexNotFoundKernelException
    {
        // Given
        IndexSamplingManagerBean.StoreAccess storeAccess = new IndexSamplingManagerBean.StoreAccess();
        storeAccess.registered( dataSource );

        // When
        storeAccess.triggerIndexSampling( EXISTING_LABEL, EXISTING_PROPERTY, true );

        // Then
        verify( indexingService, times( 1 ) ).triggerIndexSampling(
                SchemaDescriptorFactory.forLabel( LABEL_ID, PROPERTY_ID ) ,
                IndexSamplingMode.TRIGGER_REBUILD_ALL);
    }

    @Test( expected = IllegalArgumentException.class )
    public void exceptionThrownWhenMissingLabel()
    {
        // Given
        IndexSamplingManagerBean.StoreAccess storeAccess = new IndexSamplingManagerBean.StoreAccess();
        storeAccess.registered( dataSource );

        // When
        storeAccess.triggerIndexSampling( NON_EXISTING_LABEL, EXISTING_PROPERTY, false );
    }

    @Test( expected = IllegalArgumentException.class )
    public void exceptionThrownWhenMissingProperty()
    {
        // Given
        IndexSamplingManagerBean.StoreAccess storeAccess = new IndexSamplingManagerBean.StoreAccess();
        storeAccess.registered( dataSource );

        // When
        storeAccess.triggerIndexSampling( EXISTING_LABEL, NON_EXISTING_PROPERTY, false );
    }

    @Test( expected = IllegalArgumentException.class )
    public void exceptionThrownWhenNotRegistered()
    {
        // Given
        IndexSamplingManagerBean.StoreAccess storeAccess = new IndexSamplingManagerBean.StoreAccess();

        // When
        storeAccess.triggerIndexSampling( EXISTING_LABEL, EXISTING_PROPERTY, false );
    }
}
