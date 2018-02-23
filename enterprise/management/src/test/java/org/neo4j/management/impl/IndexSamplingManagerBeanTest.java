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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.storageengine.api.StoreReadLayer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode.TRIGGER_REBUILD_ALL;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode.TRIGGER_REBUILD_UPDATED;
import static org.neo4j.management.impl.IndexSamplingManagerBean.StoreAccess;

class IndexSamplingManagerBeanTest
{

    private static final String EXISTING_LABEL = "label";
    private static final String NON_EXISTING_LABEL = "bogusLabel";
    private static final int LABEL_ID = 42;
    private static final String EXISTING_PROPERTY = "prop";
    private static final String NON_EXISTING_PROPERTY = "bogusProp";
    private static final int PROPERTY_ID = 43;
    private NeoStoreDataSource dataSource;
    private StoreReadLayer storeReadLayer;
    private IndexingService indexingService;

    @BeforeEach
    void setup()
    {
        dataSource = mock( NeoStoreDataSource.class );
        storeReadLayer = mock( StoreReadLayer.class );
        indexingService = mock( IndexingService.class );
        when( dataSource.getStoreLayer() ).thenReturn( storeReadLayer );
        when( storeReadLayer.labelGetForName( EXISTING_LABEL ) ).thenReturn( LABEL_ID );
        when( storeReadLayer.propertyKeyGetForName( EXISTING_PROPERTY ) ).thenReturn( PROPERTY_ID );
        when( storeReadLayer.propertyKeyGetForName( NON_EXISTING_PROPERTY ) ).thenReturn( -1 );
        when( storeReadLayer.labelGetForName( NON_EXISTING_LABEL ) ).thenReturn( -1 );
        DependencyResolver resolver = mock( DependencyResolver.class );
        when( resolver.resolveDependency( IndexingService.class ) ).thenReturn( indexingService );
        when( dataSource.getDependencyResolver() ).thenReturn( resolver );
    }

    @Test
    void samplingTriggeredWhenIdsArePresent() throws IndexNotFoundKernelException
    {
        // Given
        StoreAccess storeAccess = new StoreAccess();
        storeAccess.registered( dataSource );

        // When
        storeAccess.triggerIndexSampling( EXISTING_LABEL, EXISTING_PROPERTY, false );

        // Then
        verify( indexingService, times( 1 ) )
                .triggerIndexSampling( forLabel( LABEL_ID, PROPERTY_ID ), TRIGGER_REBUILD_UPDATED );
    }

    @Test
    void forceSamplingTriggeredWhenIdsArePresent() throws IndexNotFoundKernelException
    {
        // Given
        StoreAccess storeAccess = new StoreAccess();
        storeAccess.registered( dataSource );

        // When
        storeAccess.triggerIndexSampling( EXISTING_LABEL, EXISTING_PROPERTY, true );

        // Then
        verify( indexingService, times( 1 ) )
                .triggerIndexSampling( forLabel( LABEL_ID, PROPERTY_ID ), TRIGGER_REBUILD_ALL );
    }

    @Test
    void exceptionThrownWhenMissingLabel()
    {
        assertThrows( IllegalArgumentException.class, () -> {
            // Given
            StoreAccess storeAccess = new StoreAccess();
            storeAccess.registered( dataSource );

            // When
            storeAccess.triggerIndexSampling( NON_EXISTING_LABEL, EXISTING_PROPERTY, false );
        } );
    }

    @Test
    void exceptionThrownWhenMissingProperty()
    {
        assertThrows( IllegalArgumentException.class, () -> {
            // Given
            StoreAccess storeAccess = new StoreAccess();
            storeAccess.registered( dataSource );

            // When
            storeAccess.triggerIndexSampling( EXISTING_LABEL, NON_EXISTING_PROPERTY, false );
        } );
    }

    @Test
    void exceptionThrownWhenNotRegistered()
    {
        assertThrows( IllegalArgumentException.class, () -> {
            // Given
            StoreAccess storeAccess = new StoreAccess();

            // When
            storeAccess.triggerIndexSampling( EXISTING_LABEL, EXISTING_PROPERTY, false );
        } );
    }
}
