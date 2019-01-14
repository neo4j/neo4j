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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Test;

import java.util.Iterator;

import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.kernel.api.security.AnonymousContext;

import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.Iterators.asCollection;

public class LabelIT extends KernelIntegrationTest
{
    @Test
    public void shouldListAllLabels() throws Exception
    {
        // given
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        int label1Id = transaction.tokenWrite().labelGetOrCreateForName( "label1" );
        int label2Id = transaction.tokenWrite().labelGetOrCreateForName( "label2" );

        // when
        Iterator<NamedToken> labelIdsBeforeCommit = transaction.tokenRead().labelsGetAllTokens();

        // then
        assertThat( asCollection( labelIdsBeforeCommit ),
                hasItems( new NamedToken( "label1", label1Id ), new NamedToken( "label2", label2Id ) ) );

        // when
        commit();

        transaction = newTransaction();
        Iterator<NamedToken> labelIdsAfterCommit = transaction.tokenRead().labelsGetAllTokens();

        // then
        assertThat( asCollection( labelIdsAfterCommit ),
                hasItems( new NamedToken( "label1", label1Id ), new NamedToken( "label2", label2Id ) ) );
        commit();
    }
}
