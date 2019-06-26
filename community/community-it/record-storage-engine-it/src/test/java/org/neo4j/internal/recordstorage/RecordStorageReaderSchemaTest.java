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

import org.junit.Test;

import java.util.Set;

import org.neo4j.graphdb.Label;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.impl.index.schema.IndexDescriptorFactory;
import org.neo4j.storageengine.api.StorageIndexReference;
import org.neo4j.storageengine.api.StorageSchemaReader;

import static org.junit.Assert.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

public class RecordStorageReaderSchemaTest extends RecordStorageReaderTestBase
{
    @Test
    public void shouldListAllIndexes() throws Exception
    {
        // Given
        createIndex( label1, propertyKey );
        createIndex( label2, propertyKey );

        // When
        Set<StorageIndexReference> indexes = asSet( storageReader.indexesGetAll() );

        // Then
        Set<?> expectedIndexes = asSet(
                indexDescriptor( label1, propertyKey ),
                indexDescriptor( label2, propertyKey ) );

        assertEquals( expectedIndexes, indexes );
    }

    @Test
    public void shouldListAllIndexesAtTimeOfSnapshot() throws Exception
    {
        // Given
        createIndex( label1, propertyKey );

        // When
        StorageSchemaReader snapshot = storageReader.schemaSnapshot();
        createIndex( label2, propertyKey );
        Set<StorageIndexReference> indexes = asSet( snapshot.indexesGetAll() );

        // Then
        Set<?> expectedIndexes = asSet(
                indexDescriptor( label1, propertyKey ) );

        assertEquals( expectedIndexes, indexes );
    }

    @Test
    public void shouldListAllConstraints() throws Exception
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );
        createUniquenessConstraint( label2, propertyKey );

        // When
        Set<ConstraintDescriptor> constraints = asSet( storageReader.constraintsGetAll() );

        // Then
        Set<?> expectedConstraints = asSet(
                uniqueConstraintDescriptor( label1, propertyKey ),
                uniqueConstraintDescriptor( label2, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    public void shouldListAllConstraintsAtTimeOfSnapshot() throws Exception
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );

        // When
        StorageSchemaReader snapshot = storageReader.schemaSnapshot();
        createUniquenessConstraint( label2, propertyKey );
        Set<ConstraintDescriptor> constraints = asSet( snapshot.constraintsGetAll() );

        // Then
        Set<?> expectedConstraints = asSet(
                uniqueConstraintDescriptor( label1, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    public void shouldListAllConstraintsForLabel() throws Exception
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );
        createUniquenessConstraint( label2, propertyKey );

        // When
        Set<ConstraintDescriptor> constraints = asSet( storageReader.constraintsGetForLabel( labelId( label1 ) ) );

        // Then
        Set<?> expectedConstraints = asSet( uniqueConstraintDescriptor( label1, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    public void shouldListAllConstraintsForLabelAtTimeOfSnapshot() throws Exception
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );
        createUniquenessConstraint( label2, propertyKey );

        // When
        StorageSchemaReader snapshot = storageReader.schemaSnapshot();
        createUniquenessConstraint( label1, otherPropertyKey );
        Set<ConstraintDescriptor> constraints = asSet( snapshot.constraintsGetForLabel( labelId( label1 ) ) );

        // Then
        Set<?> expectedConstraints = asSet( uniqueConstraintDescriptor( label1, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    public void shouldListAllConstraintsForLabelAndProperty() throws Exception
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );
        createUniquenessConstraint( label1, otherPropertyKey );

        // When
        Set<ConstraintDescriptor> constraints = asSet(
                storageReader.constraintsGetForSchema( uniqueConstraintDescriptor( label1, propertyKey ).schema() ) );

        // Then
        Set<?> expectedConstraints = asSet(
                uniqueConstraintDescriptor( label1, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    private IndexDescriptor indexDescriptor( Label label, String propertyKey )
    {
        int labelId = labelId( label );
        int propKeyId = propertyKeyId( propertyKey );
        return IndexDescriptorFactory.forSchema( SchemaDescriptor.forLabel( labelId, propKeyId ) );
    }

    private ConstraintDescriptor uniqueConstraintDescriptor( Label label, String propertyKey )
    {
        int labelId = labelId( label );
        int propKeyId = propertyKeyId( propertyKey );
        return ConstraintDescriptorFactory.uniqueForLabel( labelId, propKeyId );
    }
}
