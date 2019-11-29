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

import org.junit.jupiter.api.Test;

import java.util.Iterator;

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.token.api.NamedToken;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.collection.Iterators.asCollection;

class LabelIT extends KernelIntegrationTest
{
    @Test
    void shouldListAllLabels() throws Exception
    {
        // given
        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );
        int label1Id = transaction.tokenWrite().labelGetOrCreateForName( "label1" );
        int label2Id = transaction.tokenWrite().labelGetOrCreateForName( "label2" );

        // when
        Iterator<NamedToken> labelIdsBeforeCommit = transaction.tokenRead().labelsGetAllTokens();

        // then
        assertThat( asCollection( labelIdsBeforeCommit ) ).contains( new NamedToken( "label1", label1Id ), new NamedToken( "label2", label2Id ) );

        // when
        commit();

        transaction = newTransaction();
        Iterator<NamedToken> labelIdsAfterCommit = transaction.tokenRead().labelsGetAllTokens();

        // then
        assertThat( asCollection( labelIdsAfterCommit ) ).contains( new NamedToken( "label1", label1Id ), new NamedToken( "label2", label2Id ) );
        commit();
    }
}
