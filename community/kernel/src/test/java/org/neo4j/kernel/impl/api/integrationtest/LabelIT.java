/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.Iterator;

import org.junit.Test;

import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.impl.core.Token;

import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertThat;

import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;

public class LabelIT extends KernelIntegrationTest
{
    @Test
    public void shouldListAllLabels() throws Exception
    {
        // given
        int label1Id;
        int label2Id;
        {
            TokenWriteOperations statement = tokenWriteOperationsInNewTransaction();
            label1Id = statement.labelGetOrCreateForName( "label1" );
            label2Id = statement.labelGetOrCreateForName( "label2" );

            // when
            Iterator<Token> labelIdsBeforeCommit = statement.labelsGetAllTokens();

            // then
            assertThat( asCollection( labelIdsBeforeCommit ),
                        hasItems( new Token( "label1", label1Id ), new Token( "label2", label2Id )) );

            // when
            commit();
        }
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            Iterator<Token> labelIdsAfterCommit = statement.labelsGetAllTokens();

            // then
            assertThat(asCollection( labelIdsAfterCommit ) ,
                    hasItems( new Token( "label1", label1Id ), new Token( "label2", label2Id ) ));
        }
    }

    @Test
    public void addingAndRemovingLabelInSameTxShouldHaveNoEffect() throws Exception
    {
        // Given a node with a label
        int label;
        long node;
        {
            DataWriteOperations stmt = dataWriteOperationsInNewTransaction();
            label = stmt.labelGetOrCreateForName( "Label 1" );
            node = stmt.nodeCreate();
            stmt.nodeAddLabel( node, label );
            commit();
        }

        // When I add and remove that label in the same tx
        {
            DataWriteOperations stmt = dataWriteOperationsInNewTransaction();
            stmt.nodeRemoveLabel( node, label );
            stmt.nodeAddLabel( node, label );
        }

        // Then commit should not throw exceptions
        commit();

        // And then the node should have the label
        assertTrue( readOperationsInNewTransaction().nodeHasLabel( node, label ) );
    }
}
