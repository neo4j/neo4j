/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.IllegalLabelNameException;
import org.neo4j.kernel.api.LabelNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.core.PropertyIndexManager;
import org.neo4j.test.ImpermanentDatabaseRule;

public class TestGDBackedStatementContext_LabelsTest
{

    public final @Rule ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    @Test
    public void shouldAllowCreatingLabels() throws Exception
    {
        // Given
        String labelName = "My Label";
        StatementContext statement = newStatementContext();

        // When
        long id = statement.getOrCreateLabelId( labelName );

        // Then
        assertThat(id, is(statement.getLabelId( labelName )));
    }

    @Test
    public void differentLabelsShouldGetDifferentIds() throws Exception
    {
        // Given
        String labelName = "My Label";
        String otherLabelName = "My Other Label";

        StatementContext statement = newStatementContext();

        // When
        long id = statement.getOrCreateLabelId( labelName );
        long otherId = statement.getOrCreateLabelId( otherLabelName );

        // Then
        assertThat(id, not(otherId));
    }

    @Test
    public void shouldAllowAddingLabelToNode() throws Exception
    {
        // Given
        String labelName = "My Label";
        StatementContext statement = newStatementContext();

        long nodeId = createANode();
        long labelId = statement.getOrCreateLabelId( labelName );

        // When
        statement.addLabelToNode( nodeId, labelId );

        // Then
        // No exception should have been thrown.
        // TODO: Once we implement reading labels on nodes, this test should be expanded to assert the label is present
    }

    @Test(expected = LabelNotFoundException.class)
    public void gettingANonExistantLabelIdShouldThrowException() throws Exception
    {
        // Given
        StatementContext statement = newStatementContext();

        // When
        statement.getLabelId( "FooBar" );
    }

    @Test(expected = IllegalLabelNameException.class)
    public void creatingALabelWithNoNameShouldThrowException() throws Exception
    {
        // Given
        GDBackedStatementContext statement = newStatementContext();

        // When
        statement.getOrCreateLabelId( "" );
    }

    private long createANode()
    {
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        Transaction tx = db.beginTx();
        long nodeId = db.createNode().getId();
        tx.success();
        tx.finish();
        return nodeId;
    }

    private GDBackedStatementContext newStatementContext()
    {
        PropertyIndexManager propertyIndexManager = dbRule.getGraphDatabaseAPI().getDependencyResolver().resolveDependency(
                PropertyIndexManager.class );
        return new GDBackedStatementContext( propertyIndexManager );
    }

}
