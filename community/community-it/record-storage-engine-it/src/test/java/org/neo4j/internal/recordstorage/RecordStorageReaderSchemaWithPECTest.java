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
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor;
import org.neo4j.test.rule.RecordStorageEngineRule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.existsForLabel;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.nodeKeyForLabel;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.uniqueForLabel;

class RecordStorageReaderSchemaWithPECTest extends RecordStorageReaderTestBase
{
    @Override
    RecordStorageEngineRule.Builder modify( RecordStorageEngineRule.Builder builder )
    {
        // Basically temporarily allow PEC and node key constraints here, which is usually is only allowed in enterprise edition
        return builder.constraintSemantics( new StandardConstraintRuleAccessor() );
    }

    @Test
    void shouldListAllConstraints() throws Exception
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );
        createUniquenessConstraint( label2, propertyKey );
        createNodeKeyConstraint( label1, otherPropertyKey );
        createNodeKeyConstraint( label2, otherPropertyKey );

        createNodePropertyExistenceConstraint( label2, propertyKey );
        createRelPropertyExistenceConstraint( relType1, propertyKey );

        // When
        Set<ConstraintDescriptor> constraints = asSet( storageReader.constraintsGetAll() );

        // Then
        int labelId1 = labelId( label1 );
        int labelId2 = labelId( label2 );
        int relTypeId = relationshipTypeId( relType1 );
        int propKeyId = propertyKeyId( propertyKey );
        int propKeyId2 = propertyKeyId( otherPropertyKey );

        assertThat( constraints ).contains( uniqueForLabel( labelId1, propKeyId ), uniqueForLabel( labelId2, propKeyId ),
                nodeKeyForLabel( labelId1, propKeyId2 ), nodeKeyForLabel( labelId2, propKeyId2 ), existsForLabel( labelId2, propKeyId ),
                ConstraintDescriptorFactory.existsForRelType( relTypeId, propKeyId ) );
    }

    @Test
    void shouldListAllConstraintsForLabel() throws Exception
    {
        // Given
        createNodePropertyExistenceConstraint( label1, propertyKey );
        createNodePropertyExistenceConstraint( label2, propertyKey );

        createUniquenessConstraint( label1, propertyKey );
        createNodeKeyConstraint( label1, otherPropertyKey );
        createNodeKeyConstraint( label2, otherPropertyKey );

        // When
        Set<ConstraintDescriptor> constraints = asSet( storageReader.constraintsGetForLabel( labelId( label1 ) ) );

        // Then
        Set<ConstraintDescriptor> expectedConstraints = asSet(
                uniqueConstraintDescriptor( label1, propertyKey ),
                nodeKeyConstraintDescriptor( label1, otherPropertyKey ),
                nodePropertyExistenceDescriptor( label1, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    void shouldListAllConstraintsForLabelAndProperty() throws Exception
    {
        // Given
        createUniquenessConstraint( label2, propertyKey );
        createUniquenessConstraint( label1, otherPropertyKey );
        createNodeKeyConstraint( label1, propertyKey );
        createNodeKeyConstraint( label2, otherPropertyKey );

        createNodePropertyExistenceConstraint( label1, propertyKey );
        createNodePropertyExistenceConstraint( label2, propertyKey );

        // When
        Set<ConstraintDescriptor> constraints = asSet( storageReader.constraintsGetForSchema(
                SchemaDescriptor.forLabel( labelId( label1 ), propertyKeyId( propertyKey ) ) ) );

        // Then
        Set<ConstraintDescriptor> expected = asSet(
                nodeKeyConstraintDescriptor( label1, propertyKey ),
                nodePropertyExistenceDescriptor( label1, propertyKey ) );

        assertEquals( expected, constraints );
    }

    @Test
    void shouldListAllConstraintsForRelationshipType() throws Exception
    {
        // Given
        createRelPropertyExistenceConstraint( relType1, propertyKey );
        createRelPropertyExistenceConstraint( relType2, propertyKey );
        createRelPropertyExistenceConstraint( relType2, otherPropertyKey );

        // When
        Set<ConstraintDescriptor> constraints = asSet(
                storageReader.constraintsGetForRelationshipType( relationshipTypeId( relType2 ) ) );

        // Then
        Set<ConstraintDescriptor> expectedConstraints = Iterators.asSet(
                relationshipPropertyExistenceDescriptor( relType2, propertyKey ),
                relationshipPropertyExistenceDescriptor( relType2, otherPropertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    void shouldListAllConstraintsForRelationshipTypeAndProperty() throws Exception
    {
        // Given
        createRelPropertyExistenceConstraint( relType1, propertyKey );
        createRelPropertyExistenceConstraint( relType1, otherPropertyKey );

        createRelPropertyExistenceConstraint( relType2, propertyKey );
        createRelPropertyExistenceConstraint( relType2, otherPropertyKey );

        // When
        int relTypeId = relationshipTypeId( relType1 );
        int propKeyId = propertyKeyId( propertyKey );
        Set<ConstraintDescriptor> constraints = asSet(
                storageReader.constraintsGetForSchema( SchemaDescriptor.forRelType( relTypeId, propKeyId ) ) );

        // Then
        Set<ConstraintDescriptor> expectedConstraints = Iterators.asSet(
                relationshipPropertyExistenceDescriptor( relType1, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    private ConstraintDescriptor uniqueConstraintDescriptor( Label label, String propertyKey )
    {
        int labelId = labelId( label );
        int propKeyId = propertyKeyId( propertyKey );
        return uniqueForLabel( labelId, propKeyId );
    }

    private ConstraintDescriptor nodeKeyConstraintDescriptor( Label label, String propertyKey )
    {
        int labelId = labelId( label );
        int propKeyId = propertyKeyId( propertyKey );
        return nodeKeyForLabel( labelId, propKeyId );
    }

    private ConstraintDescriptor nodePropertyExistenceDescriptor( Label label, String propertyKey )
    {
        int labelId = labelId( label );
        int propKeyId = propertyKeyId( propertyKey );
        return existsForLabel( labelId, propKeyId );
    }

    private ConstraintDescriptor relationshipPropertyExistenceDescriptor( RelationshipType relType, String propertyKey )
    {
        int relTypeId = relationshipTypeId( relType );
        int propKeyId = propertyKeyId( propertyKey );
        return ConstraintDescriptorFactory.existsForRelType( relTypeId, propKeyId );
    }
}
