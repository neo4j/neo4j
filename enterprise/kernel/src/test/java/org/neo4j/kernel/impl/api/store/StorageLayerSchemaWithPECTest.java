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
package org.neo4j.kernel.impl.api.store;

import org.junit.Test;

import java.util.Set;

import org.neo4j.SchemaHelper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class StorageLayerSchemaWithPECTest extends StorageLayerTest
{
    @Override
    protected GraphDatabaseService createGraphDatabase()
    {
        return new TestEnterpriseGraphDatabaseFactory().newImpermanentDatabase();
    }

    @Test
    public void shouldListAllConstraints()
    {
        // Given
        SchemaHelper.createUniquenessConstraint( db, label1, propertyKey );
        SchemaHelper.createUniquenessConstraint( db, label2, propertyKey );
        SchemaHelper.createNodeKeyConstraint( db, label1, otherPropertyKey );
        SchemaHelper.createNodeKeyConstraint( db, label2, otherPropertyKey );

        SchemaHelper.createNodePropertyExistenceConstraint( db, label2, propertyKey );
        SchemaHelper.createRelPropertyExistenceConstraint( db, relType1, propertyKey );

        SchemaHelper.awaitIndexes( db );

        // When
        Set<ConstraintDescriptor> constraints = asSet( disk.constraintsGetAll() );

        // Then
        int labelId1 = labelId( label1 );
        int labelId2 = labelId( label2 );
        int relTypeId = relationshipTypeId( relType1 );
        int propKeyId = propertyKeyId( propertyKey );
        int propKeyId2 = propertyKeyId( otherPropertyKey );

        assertThat( constraints, containsInAnyOrder(
                ConstraintDescriptorFactory.uniqueForLabel( labelId1, propKeyId ),
                ConstraintDescriptorFactory.uniqueForLabel( labelId2, propKeyId ),
                ConstraintDescriptorFactory.nodeKeyForLabel( labelId1, propKeyId2 ),
                ConstraintDescriptorFactory.nodeKeyForLabel( labelId2, propKeyId2 ),
                ConstraintDescriptorFactory.existsForLabel( labelId2, propKeyId ),
                ConstraintDescriptorFactory.existsForRelType( relTypeId, propKeyId )
            ) );
    }

    @Test
    public void shouldListAllConstraintsForLabel()
    {
        // Given
        SchemaHelper.createNodePropertyExistenceConstraint( db, label1, propertyKey );
        SchemaHelper.createNodePropertyExistenceConstraint( db, label2, propertyKey );

        SchemaHelper.createUniquenessConstraint( db, label1, propertyKey );
        SchemaHelper.createNodeKeyConstraint( db, label1, otherPropertyKey );
        SchemaHelper.createNodeKeyConstraint( db, label2, otherPropertyKey );

        SchemaHelper.awaitIndexes( db );

        // When
        Set<ConstraintDescriptor> constraints = asSet( disk.constraintsGetForLabel( labelId( label1 ) ) );

        // Then
        Set<ConstraintDescriptor> expectedConstraints = asSet(
                uniqueConstraintDescriptor( label1, propertyKey ),
                nodeKeyConstraintDescriptor( label1, otherPropertyKey ),
                nodePropertyExistenceDescriptor( label1, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    public void shouldListAllConstraintsForLabelAndProperty()
    {
        // Given
        SchemaHelper.createUniquenessConstraint( db, label2, propertyKey );
        SchemaHelper.createUniquenessConstraint( db, label1, otherPropertyKey );
        SchemaHelper.createNodeKeyConstraint( db, label1, propertyKey );
        SchemaHelper.createNodeKeyConstraint( db, label2, otherPropertyKey );

        SchemaHelper.createNodePropertyExistenceConstraint( db, label1, propertyKey );
        SchemaHelper.createNodePropertyExistenceConstraint( db, label2, propertyKey );

        SchemaHelper.awaitIndexes( db );

        // When
        Set<ConstraintDescriptor> constraints = asSet( disk.constraintsGetForSchema(
                SchemaDescriptorFactory.forLabel( labelId( label1 ), propertyKeyId( propertyKey ) ) ) );

        // Then
        Set<ConstraintDescriptor> expected = asSet(
                nodeKeyConstraintDescriptor( label1, propertyKey ),
                nodePropertyExistenceDescriptor( label1, propertyKey ) );

        assertEquals( expected, constraints );
    }

    @Test
    public void shouldListAllConstraintsForRelationshipType()
    {
        // Given
        SchemaHelper.createRelPropertyExistenceConstraint( db, relType1, propertyKey );
        SchemaHelper.createRelPropertyExistenceConstraint( db, relType2, propertyKey );
        SchemaHelper.createRelPropertyExistenceConstraint( db, relType2, otherPropertyKey );

        // When
        Set<ConstraintDescriptor> constraints = asSet(
                disk.constraintsGetForRelationshipType( relationshipTypeId( relType2 ) ) );

        // Then
        Set<ConstraintDescriptor> expectedConstraints = Iterators.asSet(
                relationshipPropertyExistenceDescriptor( relType2, propertyKey ),
                relationshipPropertyExistenceDescriptor( relType2, otherPropertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    public void shouldListAllConstraintsForRelationshipTypeAndProperty()
    {
        // Given
        SchemaHelper.createRelPropertyExistenceConstraint( db, relType1, propertyKey );
        SchemaHelper.createRelPropertyExistenceConstraint( db, relType1, otherPropertyKey );

        SchemaHelper.createRelPropertyExistenceConstraint( db, relType2, propertyKey );
        SchemaHelper.createRelPropertyExistenceConstraint( db, relType2, otherPropertyKey );

        // When
        int relTypeId = relationshipTypeId( relType1 );
        int propKeyId = propertyKeyId( propertyKey );
        Set<ConstraintDescriptor> constraints = asSet(
                disk.constraintsGetForSchema( SchemaDescriptorFactory.forRelType( relTypeId, propKeyId ) ) );

        // Then
        Set<ConstraintDescriptor> expectedConstraints = Iterators.asSet(
                relationshipPropertyExistenceDescriptor( relType1, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    private ConstraintDescriptor uniqueConstraintDescriptor( Label label, String propertyKey )
    {
        int labelId = labelId( label );
        int propKeyId = propertyKeyId( propertyKey );
        return ConstraintDescriptorFactory.uniqueForLabel( labelId, propKeyId );
    }

    private ConstraintDescriptor nodeKeyConstraintDescriptor( Label label, String propertyKey )
    {
        int labelId = labelId( label );
        int propKeyId = propertyKeyId( propertyKey );
        return ConstraintDescriptorFactory.nodeKeyForLabel( labelId, propKeyId );
    }

    private ConstraintDescriptor nodePropertyExistenceDescriptor( Label label, String propertyKey )
    {
        int labelId = labelId( label );
        int propKeyId = propertyKeyId( propertyKey );
        return ConstraintDescriptorFactory.existsForLabel( labelId, propKeyId );
    }

    private ConstraintDescriptor relationshipPropertyExistenceDescriptor( RelationshipType relType, String propertyKey )
    {
        int relTypeId = relationshipTypeId( relType );
        int propKeyId = propertyKeyId( propertyKey );
        return ConstraintDescriptorFactory.existsForRelType( relTypeId, propKeyId );
    }
}
