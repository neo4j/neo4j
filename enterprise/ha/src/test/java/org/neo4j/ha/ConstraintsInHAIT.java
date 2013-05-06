/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.ha;

import static org.junit.Assert.fail;

import java.io.File;

import org.junit.After;
import org.junit.Test;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.ha.ConflictingServerIdIT;
import org.neo4j.test.TargetDirectory;

public class ConstraintsInHAIT
{
    private static final File path = TargetDirectory.forTest( ConflictingServerIdIT.class ).graphDbDir( true );
    private GraphDatabaseService db;

    @Test
    public void shouldFailNicelyWhenRunningInHA() throws Exception
    {
        GraphDatabaseBuilder masterBuilder = new HighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( path( 1 ) )
                .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001" )
                .setConfig( ClusterSettings.cluster_server, "127.0.0.1:5001" )
                .setConfig( ClusterSettings.server_id, "1" );

        db = masterBuilder.newGraphDatabase();
        db.beginTx();

        try
        {
            db.schema().constraintCreator( DynamicLabel.label( "LabelName" ) ).on( "PropertyName" ).unique().create();
            fail("Expected an exception to be thrown");
        }
        catch ( Exception e )
        { //Good
        }
    }

    @After
    public void cleanUp()
    {
        db.shutdown();
    }

    private static String path( int i )
    {
        return new File( path, "" + i ).getAbsolutePath();
    }
}
