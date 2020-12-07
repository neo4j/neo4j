/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.api.impl.fulltext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.Transaction;

/**
 * Tests testing affect of TX state on index query results.
 * <p>
 * Results of index queries must combine the content of the index
 * with relevant changes in the TX state and that is what is tested here.
 */
abstract class FulltextIndexTransactionStateTest extends FulltextProceduresTestSupport
{
    @BeforeEach
    void setUp()
    {
        createIndex();
    }

    @Test
    void queryResultFromTransactionStateMustSortTogetherWithResultFromBaseIndex()
    {
        long firstId;
        long secondId;
        long thirdId;

        try ( Transaction tx = db.beginTx() )
        {
            firstId = createEntityWithProperty( tx, "God of War" );
            thirdId = createEntityWithProperty( tx, "God Wars: Future Past" );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            secondId = createEntityWithProperty( tx, "God of War III Remastered" );
            assertQueryFindsIdsInOrder( tx, "god of war", firstId, secondId, thirdId );
            tx.commit();
        }
    }

    @Test
    void queryResultsMustIncludeEntitiesAddedInTheSameTransaction()
    {
        try ( Transaction tx = db.beginTx() )
        {
            long id = createEntityWithProperty( tx, "value" );
            assertQueryFindsIdsInOrder( tx, "value", id );
            tx.commit();
        }
    }

    @Test
    void queryResultsMustNotIncludeEntitiesDeletedInTheSameTransaction()
    {
        long entityIdA;
        long entityIdB;
        try ( Transaction tx = db.beginTx() )
        {
            entityIdA = createEntityWithProperty( tx, "value" );
            entityIdB = createEntityWithProperty( tx, "value" );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            assertQueryFindsIdsInOrder( tx, "value", entityIdA, entityIdB );

            deleteEntity( tx, entityIdA );
            assertQueryFindsIdsInOrder( tx, "value", entityIdB );

            deleteEntity( tx, entityIdB );
            assertQueryFindsIdsInOrder( tx, "value" );
            tx.commit();
        }
    }

    private void createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createIndex( tx );
            tx.commit();
        }
        awaitIndexesOnline();
    }

    abstract void createIndex( Transaction tx );

    abstract long createEntityWithProperty( Transaction tx, String propertyValue );

    abstract void assertQueryFindsIdsInOrder( Transaction tx, String query, long... ids );

    abstract void deleteEntity( Transaction tx, long entityId );
}
