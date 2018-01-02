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
package org.neo4j.kernel.impl.api.constraints;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProviderFactory;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class ConstraintCreationIT
{
    private static final Label LABEL = DynamicLabel.label( "label1" );
    @Rule
    public EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule( ConstraintCreationIT.class );

    @Test
    public void shouldNotLeaveLuceneIndexFilesHangingAroundIfConstraintCreationFails()
    {
        // given
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();

        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 2; i++ )
            {
                Node node1 = db.createNode( LABEL );
                node1.setProperty( "prop", true );
            }

            tx.success();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( LABEL ).assertPropertyIsUnique( "prop" ).create();
            fail("Should have failed with ConstraintViolationException");
            tx.success();
        }
        catch ( ConstraintViolationException ignored )  { }

        // then
        try(Transaction ignore = db.beginTx())
        {
            assertEquals(0, Iterables.count(db.schema().getIndexes() ));
        }

        File schemaStorePath = SchemaIndexProvider
                .getRootDirectory( new File( db.getStoreDir() ), LuceneSchemaIndexProviderFactory.KEY );

        String indexId = "1";
        File[] files = new File(schemaStorePath, indexId ).listFiles();
        assertNotNull( files );
        assertEquals(0, files.length);
    }
}
