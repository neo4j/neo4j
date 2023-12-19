/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.javacompat;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EnterpriseDatabaseRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.neo4j.helpers.collection.Iterators.asList;

public class AutoIndexAcceptanceTest
{
    @Rule
    public DatabaseRule db = new EnterpriseDatabaseRule();

    @Test
    public void shouldAutoIndexOnNodeSetEvenIfValueNotChanged() throws IOException
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "CREATE ({name:'test'}), ({name:'test2'})" );
            tx.success();
        }
        db.restartDatabase(
                GraphDatabaseSettings.node_auto_indexing.name(), "true",
                GraphDatabaseSettings.node_keys_indexable.name(), "name" );

        // When, set with same value as it had before
        assertThat( asList( db.execute( "MATCH (p) WITH p, p.name AS name SET p.name = name RETURN count(p)" ) ), hasSize(1));

        // Then, we should be able to find it in the index
        assertThat( asList( db.execute( "START i=node:node_auto_index('name:test') RETURN i" ) ), hasSize(1));
    }

    @Test
    public void shouldAutoIndexOnRelationshipSetEvenIfValueNotChanged() throws IOException
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "CREATE ()-[:R {name:'test'}]->(), ()-[:R {name:'test2'}]->()" );
            tx.success();
        }
        db.restartDatabase(
                GraphDatabaseSettings.relationship_auto_indexing.name(), "true",
                GraphDatabaseSettings.relationship_keys_indexable.name(), "name" );

        // When, set with same value as it had before
        assertThat( asList( db.execute( "MATCH ()-[r]->() WITH r, r.name AS name SET r.name = name RETURN count(r)" ) ), hasSize(1));

        // Then, we should be able to find it in the index
        assertThat( asList( db.execute( "START i=relationship:relationship_auto_index('name:test') RETURN i" ) ), hasSize(1));

    }
}
