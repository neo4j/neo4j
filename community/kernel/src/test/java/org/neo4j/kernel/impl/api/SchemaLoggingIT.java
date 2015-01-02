/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.api.index.IndexPopulationJob;
import org.neo4j.kernel.impl.util.TestLogging;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.info;

public class SchemaLoggingIT
{

    private final TestLogging logging = new TestLogging();

    @Rule public ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule(logging);

    @Test
    public void shouldLogUserReadableLabelAndPropertyNames() throws Exception
    {
        //noinspection deprecation
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();

        String labelName = "User";
        String property = "name";

        // when
        createIndex( db, labelName, property );

        // then
        logging.getMessagesLog( IndexPopulationJob.class ).assertExactly(
                info( "Index population started: [:User(name) [provider: {key=in-memory, version=1.0}]]" ),
                info( "Index population completed. Index is now online: [:User(name) [provider: {key=in-memory, version=1.0}]]" )
        );
    }

    private void createIndex( @SuppressWarnings("deprecation") GraphDatabaseAPI db, String labelName, String property )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label( labelName ) ).on( property ).create();
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
    }
}
