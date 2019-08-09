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
package org.neo4j.internal.recordstorage;

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.storageengine.api.IndexUpdateListener;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IntegrityValidatorTest
{
    @Test
    void shouldValidateUniquenessIndexes() throws Exception
    {
        // Given
        NeoStores store = mock( NeoStores.class );
        IndexUpdateListener indexes = mock( IndexUpdateListener.class );
        IntegrityValidator validator = new IntegrityValidator( store );
        validator.setIndexValidator( indexes );
        UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForLabel( 1, 1 );

        doThrow( new ConstraintViolationException( "error", new RuntimeException() ) ).when( indexes ).validateIndex( 2L );

        ConstraintDescriptor record = constraint.withId( 1 ).withOwnedIndexId( 2 );

        // When
        assertThrows( Exception.class, () -> validator.validateSchemaRule( record ) );
    }

    @Test
    void shouldOnlySupportOneIndexValidator()
    {
        // given
        NeoStores store = mock( NeoStores.class );
        IntegrityValidator validator = new IntegrityValidator( store );
        validator.setIndexValidator( mock( IndexUpdateListener.class ) );

        // when/then
        assertThrows( IllegalStateException.class, () -> validator.setIndexValidator( mock( IndexUpdateListener.class ) ) );
    }

    @Test
    void deletingNodeWithRelationshipsIsNotAllowed()
    {
        // Given
        NeoStores store = mock( NeoStores.class );
        IntegrityValidator validator = new IntegrityValidator( store );

        NodeRecord record = new NodeRecord( 1L, false, 1L, -1L );
        record.setInUse( false );

        // When
        assertThrows( Exception.class, () -> validator.validateNodeRecord( record ) );
    }

    @Test
    void transactionsStartedBeforeAConstraintWasCreatedAreDisallowed()
    {
        // Given
        NeoStores store = mock( NeoStores.class );
        MetaDataStore metaDataStore = mock( MetaDataStore.class );
        when( store.getMetaDataStore() ).thenReturn( metaDataStore );
        when( metaDataStore.getLatestConstraintIntroducingTx() ).thenReturn( 10L );
        IntegrityValidator validator = new IntegrityValidator( store );

        // When
        assertThrows( Exception.class, () -> validator.validateTransactionStartKnowledge( 1 ) );
    }
}
