/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.junit.Assert.assertEquals;

import static org.neo4j.test.ProcessUtil.executeSubProcess;
import static org.neo4j.test.TestLabels.LABEL_ONE;

public class TestIdJumping
{
    @Test
    public void shouldNotAllowHighIdsToJumpAfterCrash() throws Exception
    {
        // GIVEN an almost empty db that crashed
        executeSubProcess( getClass(), 10, SECONDS, dir.absolutePath() );

        // WHEN
        GraphDatabaseService db = cleanup.add( new GraphDatabaseFactory().newEmbeddedDatabase( dir.absolutePath() ) );
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
        }

        // THEN
        assertEquals( 1, nodeId );
    }

    public final @Rule CleanupRule cleanup = new CleanupRule();

    public static void main( String[] args )
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( args[0] );
        try ( Transaction tx = db.beginTx() )
        {
            // TODO create something in every store
            db.createNode( LABEL_ONE ).setProperty( "name", "something" );
            tx.success();
        }
        // Crash the database
        System.exit( 0 );
    }

    public final @Rule TestDirectory dir = TargetDirectory.cleanTestDirForTest( getClass() );
}
