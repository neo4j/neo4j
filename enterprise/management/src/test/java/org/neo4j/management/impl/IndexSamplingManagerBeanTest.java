/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
import org.neo4j.storageengine.api.StoreReadLayer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexSamplingManagerBeanTest
{

    public static final String EXISTING_LABEL = "label";
    public static final String NON_EXISTING_LABEL = "bogusLabel";
    public static final int LABEL_ID = 42;
    public static final String EXISTING_PROPERTY = "prop";
    public static final String NON_EXISTING_PROPERTY = "bogusProp";
    public static final int PROPERTY_ID = 43;
    private NeoStoreDataSource dataSource;
    private StoreReadLayer storeReadLayer;
    private IndexingService indexingService;

    @Before
    public void setup()
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
