/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.enterprise;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.RelExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.storageengine.api.StoreReadLayer;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class PropertyExistenceEnforcerTest
{

    @Test
    public void constraintPropertyIdsNotUpdatedByConstraintEnforcer()
    {
        UniquenessConstraintDescriptor uniquenessConstraint = ConstraintDescriptorFactory.uniqueForLabel( 1, 1, 70, 8 );
        NodeKeyConstraintDescriptor nodeKeyConstraint = ConstraintDescriptorFactory.nodeKeyForLabel( 2, 12, 7, 13 );
        RelExistenceConstraintDescriptor relTypeConstraint =
                ConstraintDescriptorFactory.existsForRelType( 3, 5, 13, 8 );
        List<ConstraintDescriptor> descriptors =
                Arrays.asList( uniquenessConstraint, nodeKeyConstraint, relTypeConstraint );

        StoreReadLayer storeReadLayer = prepareStoreReadLayerMock( descriptors );

        PropertyExistenceEnforcer.getOrCreatePropertyExistenceEnforcerFrom( storeReadLayer );

        assertArrayEquals( "Property ids should remain untouched.", new int[]{1, 70, 8},
                uniquenessConstraint.schema().getPropertyIds() );
        assertArrayEquals( "Property ids should remain untouched.", new int[]{12, 7, 13},
                nodeKeyConstraint.schema().getPropertyIds() );
        assertArrayEquals( "Property ids should remain untouched.", new int[]{5, 13, 8},
                relTypeConstraint.schema().getPropertyIds() );
    }

    @SuppressWarnings( "unchecked" )
    private StoreReadLayer prepareStoreReadLayerMock( List<ConstraintDescriptor> descriptors )
    {
        StoreReadLayer storeReadLayer = Mockito.mock( StoreReadLayer.class );
        when( storeReadLayer.constraintsGetAll() ).thenReturn( descriptors.iterator() );
        when( storeReadLayer.getOrCreateSchemaDependantState( eq( PropertyExistenceEnforcer.class ),
                any( Function.class) ) ).thenAnswer( invocation ->
        {
            Function<StoreReadLayer,PropertyExistenceEnforcer> function = invocation.getArgument( 1 );
            return function.apply( storeReadLayer );
        } );
        return storeReadLayer;
    }
}
