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

import java.util.Set;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.storageengine.api.StorageSchemaReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

class RecordStorageReaderSchemaTest extends RecordStorageReaderTestBase
{
    @Test
    void shouldListAllIndexes() throws Exception
    {
        // Given
        createIndex( label1, propertyKey );
        createIndex( label2, propertyKey );

        // When
        Set<IndexDescriptor> indexes = asSet( storageReader.indexesGetAll() );

        // Then
        Set<IndexDescriptor> expectedIndexes = asSet(
                indexDescriptor( label1, propertyKey ),
                indexDescriptor( label2, propertyKey ) );

        assertEquals( expectedIndexes, indexes );
    }

    @Test
    void shouldListAllIndexesAtTimeOfSnapshot() throws Exception
    {
        // Given
        createIndex( label1, propertyKey );

        // When
        StorageSchemaReader snapshot = storageReader.schemaSnapshot();
        createIndex( label2, propertyKey );
        Set<IndexDescriptor> indexes = asSet( snapshot.indexesGetAll() );

        // Then
        Set<IndexDescriptor> expectedIndexes = asSet(
                indexDescriptor( label1, propertyKey ) );

        assertEquals( expectedIndexes, indexes );
    }

    @Test
    void shouldListAllConstraints() throws Exception
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );
        createUniquenessConstraint( label2, propertyKey );

        // When
        Set<ConstraintDescriptor> constraints = asSet( storageReader.constraintsGetAll() );

        // Then
        Set<ConstraintDescriptor> expectedConstraints = asSet(
                uniqueConstraintDescriptor( label1, propertyKey ),
                uniqueConstraintDescriptor( label2, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    void shouldListAllConstraintsAtTimeOfSnapshot() throws Exception
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );

        // When
        StorageSchemaReader snapshot = storageReader.schemaSnapshot();
        createUniquenessConstraint( label2, propertyKey );
        Set<ConstraintDescriptor> constraints = asSet( snapshot.constraintsGetAll() );

        // Then
        Set<ConstraintDescriptor> expectedConstraints = asSet(
                uniqueConstraintDescriptor( label1, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    void shouldListAllIndexesForLabel() throws Exception
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );
        createIndex( label1, otherPropertyKey );
        createUniquenessConstraint( label2, propertyKey );
        createIndex( label2, otherPropertyKey );

        // When
        Set<IndexDescriptor> indexes = asSet( storageReader.indexesGetForLabel( labelId( label1 ) ) );

        // Then
        Set<IndexDescriptor> expectedIndexes = asSet(
                uniqueIndexDescriptor( label1, propertyKey ),
                indexDescriptor( label1, otherPropertyKey ) );

        assertEquals( expectedIndexes, indexes );
    }

    @Test
    void shouldListAllIndexesForLabelAtTimeOfSnapshot() throws Exception
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );
        createIndex( label1, otherPropertyKey );
        createUniquenessConstraint( label2, propertyKey );
        createIndex( label2, otherPropertyKey );

        // When
        StorageSchemaReader snapshot = storageReader.schemaSnapshot();
        Set<IndexDescriptor> indexes = asSet( snapshot.indexesGetForLabel( labelId( label1 ) ) );

        // Then
        Set<IndexDescriptor> expectedIndexes = asSet(
                uniqueIndexDescriptor( label1, propertyKey ),
                indexDescriptor( label1, otherPropertyKey ) );

        assertEquals( expectedIndexes, indexes );
    }

    @Test
    void shouldListAllIndexesForRelationshipType() throws Exception
    {
        // Given
        createIndex( relType1, propertyKey );
        createIndex( relType2, propertyKey );

        // When
        Set<IndexDescriptor> indexes = asSet( storageReader.indexesGetForRelationshipType( relationshipTypeId( relType1 ) ) );

        // Then
        Set<IndexDescriptor> expectedIndexes = asSet(
                indexDescriptor( relType1, propertyKey ) );

        assertEquals( expectedIndexes, indexes );
    }

    @Test
    void shouldListAllIndexesForRelationshipTypeAtTimeOfSnapshot() throws Exception
    {
        // Given
        createIndex( relType1, propertyKey );
        createIndex( relType2, propertyKey );

        // When
        StorageSchemaReader snapshot = storageReader.schemaSnapshot();
        Set<IndexDescriptor> indexes = asSet( snapshot.indexesGetForRelationshipType( relationshipTypeId( relType1 ) ) );

        // Then
        Set<IndexDescriptor> expectedIndexes = asSet(
                indexDescriptor( relType1, propertyKey ) );

        assertEquals( expectedIndexes, indexes );
    }

    @Test
    void shouldListAllConstraintsForLabel() throws Exception
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );
        createUniquenessConstraint( label2, propertyKey );

        // When
        Set<ConstraintDescriptor> constraints = asSet( storageReader.constraintsGetForLabel( labelId( label1 ) ) );

        // Then
        Set<ConstraintDescriptor> expectedConstraints = asSet( uniqueConstraintDescriptor( label1, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    void shouldListAllConstraintsForLabelAtTimeOfSnapshot() throws Exception
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );
        createUniquenessConstraint( label2, propertyKey );

        // When
        StorageSchemaReader snapshot = storageReader.schemaSnapshot();
        createUniquenessConstraint( label1, otherPropertyKey );
        Set<ConstraintDescriptor> constraints = asSet( snapshot.constraintsGetForLabel( labelId( label1 ) ) );

        // Then
        Set<ConstraintDescriptor> expectedConstraints = asSet( uniqueConstraintDescriptor( label1, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    void shouldListAllConstraintsForLabelAndProperty() throws Exception
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );
        createUniquenessConstraint( label1, otherPropertyKey );

        // When
        Set<ConstraintDescriptor> constraints = asSet(
                storageReader.constraintsGetForSchema( uniqueConstraintDescriptor( label1, propertyKey ).schema() ) );

        // Then
        Set<ConstraintDescriptor> expectedConstraints = asSet(
                uniqueConstraintDescriptor( label1, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    private IndexDescriptor indexDescriptor( Label label, String propertyKey )
    {
        int labelId = labelId( label );
        int propKeyId = propertyKeyId( propertyKey );
        return IndexPrototype.forSchema( SchemaDescriptor.forLabel( labelId, propKeyId ) ).withName( "index" ).materialise( 0 );
    }

    private IndexDescriptor indexDescriptor( RelationshipType relType, String propertyKey )
    {
        int relTypeId = relationshipTypeId( relType );
        int propKeyId = propertyKeyId( propertyKey );
        return IndexPrototype.forSchema( SchemaDescriptor.forRelType( relTypeId, propKeyId ) ).withName( "index" ).materialise( 0 );
    }

    private IndexDescriptor uniqueIndexDescriptor( Label label, String propertyKey )
    {
        int labelId = labelId( label );
        int propKeyId = propertyKeyId( propertyKey );
        return IndexPrototype.uniqueForSchema( SchemaDescriptor.forLabel( labelId, propKeyId ) ).withName( "constraint" ).materialise( 0 );
    }

    private ConstraintDescriptor uniqueConstraintDescriptor( Label label, String propertyKey )
    {
        int labelId = labelId( label );
        int propKeyId = propertyKeyId( propertyKey );
        return ConstraintDescriptorFactory.uniqueForLabel( labelId, propKeyId );
    }
}
