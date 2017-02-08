/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.store;

import org.junit.Test;

import java.util.Set;

import org.neo4j.SchemaHelper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.schema.NodePropertyDescriptor;
import org.neo4j.kernel.api.schema.RelationshipPropertyDescriptor;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.schema_new.SchemaBoundary;
import org.neo4j.kernel.api.schema_new.SchemaDescriptorFactory;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;

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

        SchemaHelper.createNodePropertyExistenceConstraint( db, label2, propertyKey );
        SchemaHelper.createRelPropertyExistenceConstraint( db, relType1, propertyKey );

        SchemaHelper.awaitIndexes( db );

        // When
        Set<PropertyConstraint> constraints = asSet( disk.constraintsGetAll() );

        // Then
        int labelId1 = labelId( label1 );
        int labelId2 = labelId( label2 );
        int propKeyId = propertyKeyId( propertyKey );
        NodePropertyDescriptor descriptor1 = new NodePropertyDescriptor( labelId1, propKeyId );
        NodePropertyDescriptor descriptor2 = new NodePropertyDescriptor( labelId2, propKeyId );

        Set<PropertyConstraint> expectedConstraints = asSet(
                new UniquenessConstraint( descriptor1 ),
                new UniquenessConstraint( descriptor2 ),
                new NodePropertyExistenceConstraint( descriptor2 ),
                new RelationshipPropertyExistenceConstraint(
                        new RelationshipPropertyDescriptor( relationshipTypeId( relType1 ), propKeyId ) ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    public void shouldListAllConstraintsForLabel()
    {
        // Given
        SchemaHelper.createNodePropertyExistenceConstraint( db, label1, propertyKey );
        SchemaHelper.createNodePropertyExistenceConstraint( db, label2, propertyKey );

        SchemaHelper.createUniquenessConstraint( db, label1, propertyKey );

        SchemaHelper.awaitIndexes( db );

        // When
        Set<NodePropertyConstraint> constraints = asSet( disk.constraintsGetForLabel( labelId( label1 ) ) );

        // Then
        Set<NodePropertyConstraint> expectedConstraints = asSet(
                new UniquenessConstraint(
                        new NodePropertyDescriptor( labelId( label1 ), propertyKeyId( propertyKey ) ) ),
                new NodePropertyExistenceConstraint(
                        new NodePropertyDescriptor( labelId( label1 ), propertyKeyId( propertyKey ) ) ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    public void shouldListAllConstraintsForLabelAndProperty()
    {
        // Given
        SchemaHelper.createUniquenessConstraint( db, label1, propertyKey );
        SchemaHelper.createUniquenessConstraint( db, label1, otherPropertyKey );

        SchemaHelper.createNodePropertyExistenceConstraint( db, label1, propertyKey );
        SchemaHelper.createNodePropertyExistenceConstraint( db, label2, propertyKey );

        SchemaHelper.awaitIndexes( db );

        // When
        NodePropertyDescriptor descriptor =
                new NodePropertyDescriptor( labelId( label1 ), propertyKeyId( propertyKey ) );
        Set<NodePropertyConstraint> constraints = asSet( disk.constraintsGetForLabelAndPropertyKey(
                SchemaBoundary.map( descriptor ) ) );

        // Then
        Set<NodePropertyConstraint> expectedConstraints = asSet(
                new UniquenessConstraint( descriptor ),
                new NodePropertyExistenceConstraint( descriptor ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    public void shouldListAllConstraintsForRelationshipType()
    {
        // Given
        SchemaHelper.createRelPropertyExistenceConstraint( db, relType1, propertyKey );
        SchemaHelper.createRelPropertyExistenceConstraint( db, relType2, propertyKey );
        SchemaHelper.createRelPropertyExistenceConstraint( db, relType2, otherPropertyKey );

        // When
        int relTypeId = relationshipTypeId( relType2 );
        Set<RelationshipPropertyConstraint> constraints = asSet( disk.constraintsGetForRelationshipType( relTypeId ) );

        // Then
        Set<RelationshipPropertyConstraint> expectedConstraints = Iterators.asSet(
                new RelationshipPropertyExistenceConstraint(
                        new RelationshipPropertyDescriptor( relTypeId, propertyKeyId( propertyKey ) ) ),
                new RelationshipPropertyExistenceConstraint(
                        new RelationshipPropertyDescriptor( relTypeId, propertyKeyId( otherPropertyKey ) ) ) );

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
        Set<RelationshipPropertyConstraint> constraints = asSet(
                disk.constraintsGetForRelationshipTypeAndPropertyKey(
                        SchemaDescriptorFactory.forRelType( relTypeId, propKeyId ) ) );

        // Then
        Set<RelationshipPropertyConstraint> expectedConstraints = Iterators.asSet(
                new RelationshipPropertyExistenceConstraint(
                        new RelationshipPropertyDescriptor( relTypeId, propKeyId ) ) );

        assertEquals( expectedConstraints, constraints );
    }
}
