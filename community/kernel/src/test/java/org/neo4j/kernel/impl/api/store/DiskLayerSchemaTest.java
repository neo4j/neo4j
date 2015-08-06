/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.store;

import org.junit.Test;

import java.util.Set;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class DiskLayerSchemaTest extends DiskLayerTest
{
    @Test
    public void shouldListAllConstraints()
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );
        createUniquenessConstraint( label2, propertyKey );

        createNodePropertyExistenceConstraint( label2, propertyKey );
        createRelationshipPropertyExistenceConstraint( relType1, propertyKey );

        // When
        Set<PropertyConstraint> constraints = asSet( disk.constraintsGetAll() );

        // Then
        int propKeyId = propertyKeyId( propertyKey );

        Set<PropertyConstraint> expectedConstraints = asSet(
                new UniquenessConstraint( labelId( label1 ), propKeyId ),
                new UniquenessConstraint( labelId( label2 ), propKeyId ),
                new NodePropertyExistenceConstraint( labelId( label2 ), propKeyId ),
                new RelationshipPropertyExistenceConstraint( relationshipTypeId( relType1 ), propKeyId ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    public void shouldListAllConstraintsForLabel()
    {
        // Given
        createNodePropertyExistenceConstraint( label1, propertyKey );
        createNodePropertyExistenceConstraint( label2, propertyKey );

        createUniquenessConstraint( label1, propertyKey );

        // When
        Set<NodePropertyConstraint> constraints = asSet( disk.constraintsGetForLabel( labelId( label1 ) ) );

        // Then
        Set<NodePropertyConstraint> expectedConstraints = asSet(
                new UniquenessConstraint( labelId( label1 ), propertyKeyId( propertyKey ) ),
                new NodePropertyExistenceConstraint( labelId( label1 ), propertyKeyId( propertyKey ) ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    public void shouldListAllConstraintsForLabelAndProperty()
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );
        createUniquenessConstraint( label1, otherPropertyKey );

        createNodePropertyExistenceConstraint( label1, propertyKey );
        createNodePropertyExistenceConstraint( label2, propertyKey );

        // When
        Set<NodePropertyConstraint> constraints = asSet(
                disk.constraintsGetForLabelAndPropertyKey( labelId( label1 ), propertyKeyId( propertyKey ) ) );

        // Then
        Set<NodePropertyConstraint> expectedConstraints = asSet(
                new UniquenessConstraint( labelId( label1 ), propertyKeyId( propertyKey ) ),
                new NodePropertyExistenceConstraint( labelId( label1 ), propertyKeyId( propertyKey ) ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    public void shouldListAllConstraintsForRelationshipType()
    {
        // Given
        createRelationshipPropertyExistenceConstraint( relType1, propertyKey );
        createRelationshipPropertyExistenceConstraint( relType2, propertyKey );
        createRelationshipPropertyExistenceConstraint( relType2, otherPropertyKey );

        // When
        int relTypeId = relationshipTypeId( relType2 );
        Set<RelationshipPropertyConstraint> constraints = asSet( disk.constraintsGetForRelationshipType( relTypeId ) );

        // Then
        Set<RelationshipPropertyConstraint> expectedConstraints = IteratorUtil.<RelationshipPropertyConstraint>asSet(
                new RelationshipPropertyExistenceConstraint( relTypeId, propertyKeyId( propertyKey ) ),
                new RelationshipPropertyExistenceConstraint( relTypeId, propertyKeyId( otherPropertyKey ) ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    public void shouldListAllConstraintsForRelationshipTypeAndProperty()
    {
        // Given
        createRelationshipPropertyExistenceConstraint( relType1, propertyKey );
        createRelationshipPropertyExistenceConstraint( relType1, otherPropertyKey );

        createRelationshipPropertyExistenceConstraint( relType2, propertyKey );
        createRelationshipPropertyExistenceConstraint( relType2, otherPropertyKey );

        // When
        int relTypeId = relationshipTypeId( relType1 );
        int propKeyId = propertyKeyId( propertyKey );
        Set<RelationshipPropertyConstraint> constraints = asSet(
                disk.constraintsGetForRelationshipTypeAndPropertyKey( relTypeId, propKeyId ) );

        // Then
        Set<RelationshipPropertyConstraint> expectedConstraints = IteratorUtil.<RelationshipPropertyConstraint>asSet(
                new RelationshipPropertyExistenceConstraint( relTypeId, propKeyId ) );

        assertEquals( expectedConstraints, constraints );
    }

    private void createUniquenessConstraint( Label label, String propertyKey )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).create();
            tx.success();
        }
    }

    private void createNodePropertyExistenceConstraint( Label label, String propertyKey )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyExists( propertyKey ).create();
            tx.success();
        }
    }

    private void createRelationshipPropertyExistenceConstraint( RelationshipType type, String propertyKey )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( type ).assertPropertyExists( propertyKey ).create();
            tx.success();
        }
    }

    private int labelId( Label label )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            return readOps().labelGetForName( label.name() );
        }
    }

    private int propertyKeyId( String propertyKey )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            return readOps().propertyKeyGetForName( propertyKey );
        }
    }

    private int relationshipTypeId( RelationshipType type )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            return readOps().relationshipTypeGetForName( type.name() );
        }
    }

    private ReadOperations readOps()
    {
        DependencyResolver dependencyResolver = db.getDependencyResolver();
        Statement statement = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class ).get();
        return statement.readOperations();
    }
}
