/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Test;

import java.util.Iterator;

import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.storageengine.api.Token;

import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterators.asCollection;

public class LabelIT extends KernelIntegrationTest
{
    @Test
    public void shouldListAllLabels() throws Exception
    {
        // given
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        int label1Id = statement.tokenWriteOperations().labelGetOrCreateForName( "label1" );
        int label2Id = statement.tokenWriteOperations().labelGetOrCreateForName( "label2" );

        // when
        Iterator<Token> labelIdsBeforeCommit = statement.readOperations().labelsGetAllTokens();

        // then
        assertThat( asCollection( labelIdsBeforeCommit ),
                hasItems( new Token( "label1", label1Id ), new Token( "label2", label2Id ) ) );

        // when
        commit();

        ReadOperations readOperations = readOperationsInNewTransaction();
        Iterator<Token> labelIdsAfterCommit = readOperations.labelsGetAllTokens();

        // then
        assertThat( asCollection( labelIdsAfterCommit ),
                hasItems( new Token( "label1", label1Id ), new Token( "label2", label2Id ) ) );
        commit();
    }

    @Test
    public void addingAndRemovingLabelInSameTxShouldHaveNoEffect() throws Exception
    {
        // Given a node with a label
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        int label = statement.tokenWriteOperations().labelGetOrCreateForName( "Label 1" );
        long node = statement.dataWriteOperations().nodeCreate();
        statement.dataWriteOperations().nodeAddLabel( node, label );
        commit();

        // When I add and remove that label in the same tx
        DataWriteOperations dataWriteOperations = dataWriteOperationsInNewTransaction();
        dataWriteOperations.nodeRemoveLabel( node, label );
        dataWriteOperations.nodeAddLabel( node, label );

        // Then commit should not throw exceptions
        commit();

        // And then the node should have the label
        assertTrue( readOperationsInNewTransaction().nodeHasLabel( node, label ) );
        commit();
    }
}
