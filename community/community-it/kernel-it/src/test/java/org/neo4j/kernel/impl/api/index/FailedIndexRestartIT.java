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
package org.neo4j.kernel.impl.api.index;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.Exceptions.rootCause;
import static org.neo4j.helpers.collection.Iterators.count;
import static org.neo4j.io.ByteUnit.mebiBytes;

public class FailedIndexRestartIT
{
    private static final String ROBOT = "Robot";
    private static final String GENDER = "gender";

    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule();

    @Test
    public void failedIndexUpdatesAfterRestart() throws IOException
    {
        Label robot = Label.label( ROBOT );
        String megaProperty = randomAlphanumeric( (int) mebiBytes( 16 ) );
        createNodeWithProperty( robot, megaProperty );

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( robot ).on( GENDER ).create();
            tx.success();
        }
        try
        {
            awaitIndexesOnline();
            fail( "Index population should fail on entry size limit" );
        }
        catch ( RuntimeException e )
        {
            assertThat( rootCause( e ).getMessage(), containsString( "Index key-value size it to large." ) );
        }

        // can add more nodes that do not satisfy failed index
        createNodeWithProperty( robot, megaProperty );
        try ( Transaction transaction = db.beginTx() )
        {
            assertEquals( 2, count( db.findNodes( robot ) ) );
        }

        db.restartDatabase();

        // can add more nodes that do not satisfy failed index after db and index restart
        createNodeWithProperty( robot, megaProperty );
        try ( Transaction transaction = db.beginTx() )
        {
            assertEquals( 3, count( db.findNodes( robot ) ) );
        }
    }

    private void createNodeWithProperty( Label label, String propertyValue )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Node node = db.createNode( label );
            node.setProperty( GENDER, propertyValue );
            transaction.success();
        }
    }

    private void awaitIndexesOnline()
    {
        try ( Transaction ignored = db.beginTx() )
        {
            db.schema().awaitIndexesOnline(3, TimeUnit.MINUTES );
        }
    }
}
