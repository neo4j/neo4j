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

import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IntegrityValidatorTest
{
    @Test
    public void shouldValidateUniquenessIndexes() throws Exception
    {
        // Given
        NeoStores store = mock( NeoStores.class );
        IndexingService indexes = mock(IndexingService.class);
        IntegrityValidator validator = new IntegrityValidator( store, indexes );
        UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForLabel( 1, 1 );

        doThrow( new UniquePropertyValueValidationException( constraint,
                ConstraintValidationException.Phase.VERIFICATION, new RuntimeException() ) )
                .when( indexes ).validateIndex( 2L );

        ConstraintRule record = ConstraintRule.constraintRule( 1L, constraint, 2L );

        // When
        try
        {
            validator.validateSchemaRule( record );
            fail("Should have thrown integrity error.");
        }
        catch ( Exception e )
        {
            // good
        }
    }

    @Test
    public void deletingNodeWithRelationshipsIsNotAllowed()
    {
        // Given
        NeoStores store = mock( NeoStores.class );
        IndexingService indexes = mock(IndexingService.class);
        IntegrityValidator validator = new IntegrityValidator( store, indexes );

        NodeRecord record = new NodeRecord( 1L, false, 1L, -1L );
        record.setInUse( false );

        // When
        try
        {
            validator.validateNodeRecord( record );
            fail( "Should have thrown integrity error." );
        }
        catch ( Exception e )
        {
            // good
        }
    }

    @Test
    public void transactionsStartedBeforeAConstraintWasCreatedAreDisallowed()
    {
        // Given
        NeoStores store = mock( NeoStores.class );
        MetaDataStore metaDataStore = mock( MetaDataStore.class );
        when( store.getMetaDataStore() ).thenReturn( metaDataStore );
        IndexingService indexes = mock( IndexingService.class );
        when( metaDataStore.getLatestConstraintIntroducingTx() ).thenReturn( 10L );
        IntegrityValidator validator = new IntegrityValidator( store, indexes );

        // When
        try
        {
            validator.validateTransactionStartKnowledge( 1 );
            fail( "Should have thrown integrity error." );
        }
        catch ( Exception e )
        {
            // good
        }
    }
}
