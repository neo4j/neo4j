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

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.helpers.collection.Iterators.count;
import static org.neo4j.io.ByteUnit.mebiBytes;

@DbmsExtension
public class FailedIndexRestartIT
{
    private static final String ROBOT = "Robot";
    private static final String GENDER = "gender";

    @Inject
    private GraphDatabaseService database;
    @Inject
    private DbmsController dbmsController;

    @Test
    void failedIndexUpdatesAfterRestart()
    {
        Label robot = Label.label( ROBOT );
        String megaProperty = randomAlphanumeric( (int) mebiBytes( 16 ) );
        createNodeWithProperty( database, robot, megaProperty );

        try ( Transaction tx = database.beginTx() )
        {
            tx.schema().indexFor( robot ).on( GENDER ).create();
            tx.commit();
        }
        var e = assertThrows( RuntimeException.class, () -> awaitIndexesOnline( database ) );
        assertThat( getRootCause( e ).getMessage(), containsString( "Property value is too large to index" ) );

        // can add more nodes that do not satisfy failed index
        createNodeWithProperty( database, robot, megaProperty );
        try ( Transaction transaction = database.beginTx() )
        {
            assertEquals( 2, count( transaction.findNodes( robot ) ) );
        }

        dbmsController.restartDbms();

        // can add more nodes that do not satisfy failed index after db and index restart
        createNodeWithProperty( database, robot, megaProperty );
        try ( Transaction transaction = database.beginTx() )
        {
            assertEquals( 3, count( transaction.findNodes( robot ) ) );
        }
    }

    private void createNodeWithProperty( GraphDatabaseService db, Label label, String propertyValue )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( label );
            node.setProperty( GENDER, propertyValue );
            tx.commit();
        }
    }

    private void awaitIndexesOnline( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline(3, TimeUnit.MINUTES );
        }
    }
}
