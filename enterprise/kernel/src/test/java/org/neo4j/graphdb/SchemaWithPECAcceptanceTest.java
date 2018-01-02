/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.graphdb;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.SchemaHelper;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.coreapi.schema.InternalSchemaActions;
import org.neo4j.kernel.impl.coreapi.schema.NodeKeyConstraintDefinition;
import org.neo4j.kernel.impl.coreapi.schema.NodePropertyExistenceConstraintDefinition;
import org.neo4j.kernel.impl.coreapi.schema.RelationshipPropertyExistenceConstraintDefinition;
import org.neo4j.kernel.impl.coreapi.schema.UniquenessConstraintDefinition;
import org.neo4j.test.rule.EnterpriseDatabaseRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.containsOnly;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.getConstraints;

public class SchemaWithPECAcceptanceTest
{
    @Rule
    public EnterpriseDatabaseRule dbRule = new EnterpriseDatabaseRule();

    private GraphDatabaseService db;
    private Label label = Labels.MY_LABEL;
    private Label label2 = Labels.MY_OTHER_LABEL;
    private String propertyKey = "my_property_key";
    private String propertyKey2 = "my_other_property";

    private enum Labels implements Label
    {
        MY_LABEL,
        MY_OTHER_LABEL
    }

    private enum Types implements RelationshipType
    {
        MY_TYPE,
        MY_OTHER_TYPE
    }

    @Before
    public void init()
    {
        db = dbRule.getGraphDatabaseAPI();
    }

    @Test
    public void shouldCreateNodePropertyExistenceConstraint()
    {
        // When
        ConstraintDefinition constraint = createNodePropertyExistenceConstraint( label, propertyKey );

        // Then
        assertThat( getConstraints( db ), containsOnly( constraint ) );
    }

    @Test
    public void shouldCreateRelationshipPropertyExistenceConstraint()
    {
        // When
        ConstraintDefinition constraint = createRelationshipPropertyExistenceConstraint( Types.MY_TYPE, propertyKey );

        // Then
        assertThat( getConstraints( db ), containsOnly( constraint ) );
    }

    @Test
    public void shouldListAddedConstraintsByLabel() throws Exception
    {
        // GIVEN
        ConstraintDefinition constraint1 = createUniquenessConstraint( label, propertyKey );
        ConstraintDefinition constraint2 = createNodePropertyExistenceConstraint( label, propertyKey );
        ConstraintDefinition constraint3 = createNodeKeyConstraint( label, propertyKey2 );
        createNodeKeyConstraint( label2, propertyKey2 );
        createNodePropertyExistenceConstraint( Labels.MY_OTHER_LABEL, propertyKey );

        // WHEN THEN
        assertThat( getConstraints( db, label ), containsOnly( constraint1, constraint2, constraint3 ) );
    }

    @Test
    public void shouldListAddedConstraintsByRelationshipType() throws Exception
    {
        // GIVEN
        ConstraintDefinition constraint1 = createRelationshipPropertyExistenceConstraint( Types.MY_TYPE, propertyKey );
        createRelationshipPropertyExistenceConstraint( Types.MY_OTHER_TYPE, propertyKey );

        // WHEN THEN
        assertThat( getConstraints( db, Types.MY_TYPE ), containsOnly( constraint1 ) );
    }

    @Test
    public void shouldListAddedConstraints() throws Exception
    {
        // GIVEN
        ConstraintDefinition constraint1 = createUniquenessConstraint( label, propertyKey );
        ConstraintDefinition constraint2 = createNodePropertyExistenceConstraint( label, propertyKey );
        ConstraintDefinition constraint3 = createRelationshipPropertyExistenceConstraint( Types.MY_TYPE, propertyKey );
        ConstraintDefinition constraint4 = createNodeKeyConstraint( label, propertyKey2 );

        // WHEN THEN
        assertThat( getConstraints( db ), containsOnly( constraint1, constraint2, constraint3, constraint4 ) );
    }

    private ConstraintDefinition createUniquenessConstraint( Label label, String propertyKey )
    {
        SchemaHelper.createUniquenessConstraint( db, label, propertyKey );
        SchemaHelper.awaitIndexes( db );
        InternalSchemaActions actions = mock( InternalSchemaActions.class );
        IndexDefinition index = new IndexDefinitionImpl( actions, label, new String[]{propertyKey}, true );
        return new UniquenessConstraintDefinition( actions, index );
    }

    private ConstraintDefinition createNodeKeyConstraint( Label label, String propertyKey )
    {
        SchemaHelper.createNodeKeyConstraint( db, label, propertyKey );
        SchemaHelper.awaitIndexes( db );
        InternalSchemaActions actions = mock( InternalSchemaActions.class );
        IndexDefinition index = new IndexDefinitionImpl( actions, label, new String[]{propertyKey}, true );
        return new NodeKeyConstraintDefinition( actions, index );
    }

    private ConstraintDefinition createNodePropertyExistenceConstraint( Label label, String propertyKey )
    {
        SchemaHelper.createNodePropertyExistenceConstraint( db, label, propertyKey );
        return new NodePropertyExistenceConstraintDefinition( mock( InternalSchemaActions.class ), label,
                new String[]{propertyKey} );
    }

    private ConstraintDefinition createRelationshipPropertyExistenceConstraint( Types type, String propertyKey )
    {
        SchemaHelper.createRelPropertyExistenceConstraint( db, type, propertyKey );
        return new RelationshipPropertyExistenceConstraintDefinition( mock( InternalSchemaActions.class ), type,
                propertyKey );
    }
}
