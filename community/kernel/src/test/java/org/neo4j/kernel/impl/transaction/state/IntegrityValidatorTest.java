/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.kernel.api.exceptions.schema.UniquenessConstraintVerificationFailedKernelException;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule.uniquenessConstraintRule;
import static org.powermock.api.mockito.PowerMockito.mock;

public class IntegrityValidatorTest
{
    @Test
    public void shouldValidateUniquenessIndexes() throws Exception
    {
        // Given
        NeoStores store = mock( NeoStores.class );
        IndexingService indexes = mock(IndexingService.class);
        IntegrityValidator validator = new IntegrityValidator(store, indexes);

        doThrow( new UniquenessConstraintVerificationFailedKernelException( null, new RuntimeException() ) )
                .when( indexes ).validateIndex( 2l );

        UniquePropertyConstraintRule record = uniquenessConstraintRule( 1l, 1, 1, 2l );

        // When
        try
        {
            validator.validateSchemaRule( record );
            fail("Should have thrown integrity error.");
        }
        catch(Exception e)
        {
            // good 
        }
    }

    @Test
    public void deletingNodeWithRelationshipsIsNotAllowed() throws Exception
    {
        // Given
        NeoStores store = mock( NeoStores.class );
        IndexingService indexes = mock(IndexingService.class);
        IntegrityValidator validator = new IntegrityValidator(store, indexes );

        NodeRecord record = new NodeRecord( 1l, false, 1l, -1l );
        record.setInUse( false );

        // When
        try
        {
            validator.validateNodeRecord( record );
            fail("Should have thrown integrity error.");
        }
        catch(Exception e)
        {
            // good
        }
    }

    @Test
    public void transactionsStartedBeforeAConstraintWasCreatedAreDisallowed() throws Exception
    {
        // Given
        NeoStores store = mock( NeoStores.class );
        MetaDataStore metaDataStore = mock( MetaDataStore.class );
        when( store.getMetaDataStore() ).thenReturn( metaDataStore );
        IndexingService indexes = mock( IndexingService.class );
        when( metaDataStore.getLatestConstraintIntroducingTx() ).thenReturn( 10l );
        IntegrityValidator validator = new IntegrityValidator( store, indexes );

        // When
        try
        {
            validator.validateTransactionStartKnowledge( 1 );
            fail("Should have thrown integrity error.");
        }
        catch(Exception e)
        {
            // good
        }
    }
}
