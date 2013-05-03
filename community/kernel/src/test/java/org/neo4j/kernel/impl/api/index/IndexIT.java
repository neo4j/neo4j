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
package org.neo4j.kernel.impl.api.index;

import java.util.Set;

import org.junit.Test;

import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class IndexIT extends KernelIntegrationTest
{
    long labelId = 5, propertyKey = 8;

    @Test
    public void createANewIndex() throws Exception
    {
        // GIVEN
        newTransaction();

        // WHEN
        IndexDescriptor rule = statement.addIndexRule( labelId, propertyKey, false );
        commit();

        // AND WHEN the index is created
        awaitIndexOnline( rule );

        // THEN
    }

    @Test
    public void addIndexRuleInATransaction() throws Exception
    {
        // GIVEN
        newTransaction();

        // WHEN
        IndexDescriptor expectedRule = statement.addIndexRule( labelId, propertyKey, false );
        commit();

        // THEN
        StatementContext roStatement = readOnlyContext();
        assertEquals( asSet( expectedRule ),
                asSet( roStatement.getIndexRules( labelId ) ) );
        assertEquals( expectedRule, roStatement.getIndexRule( labelId, propertyKey ) );
    }

    @Test
    public void committedAndTransactionalIndexRulesShouldBeMerged() throws Exception
    {
        // GIVEN
        newTransaction();
        IndexDescriptor existingRule = statement.addIndexRule( labelId, propertyKey, false );
        commit();

        // WHEN
        newTransaction();
        long propertyKey2 = 10;
        IndexDescriptor addedRule = statement.addIndexRule( labelId, propertyKey2, false );
        Set<IndexDescriptor> indexRulesInTx = asSet( statement.getIndexRules( labelId ) );
        commit();

        // THEN
        assertEquals( asSet( existingRule, addedRule ), indexRulesInTx );
    }

    @Test
    public void rollBackIndexRuleShouldNotBeCommitted() throws Exception
    {
        // GIVEN
        newTransaction();

        // WHEN
        statement.addIndexRule( labelId, propertyKey, false );
        statement.close();
        // don't mark as success
        rollback();

        // THEN
        assertEquals( asSet(), asSet( readOnlyContext().getIndexRules( labelId ) ) );
    }

    @Test
    public void shouldRemoveAConstraintIndexWithoutOwnerInRecovery() throws Exception
    {
        // given
        newTransaction();
        IndexDescriptor index = statement.addIndexRule( labelId, propertyKey, true );
        commit();
        awaitIndexOnline( index );

        // when
        restartDb();

        // then
        assertEquals( asSet(), asSet( readOnlyContext().getIndexRules( labelId ) ) );
    }

    private void awaitIndexOnline( IndexDescriptor indexRule ) throws IndexNotFoundKernelException
    {
        StatementContext ctx = readOnlyContext();
        long start = System.currentTimeMillis();
        while(true)
        {
           if(ctx.getIndexState(indexRule) == InternalIndexState.ONLINE)
           {
               break;
           }

           if(start + 1000 * 10 < System.currentTimeMillis())
           {
               throw new RuntimeException( "Index didn't come online within a reasonable time." );
           }
        }
    }

}
