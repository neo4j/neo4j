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
package org.neo4j.index;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static java.lang.Boolean.TRUE;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class AccessExplicitIndexReadOnlyIT
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule().startLazily();

    @Test
    public void shouldListAndReadExplicitIndexesForReadOnlyDb() throws Exception
    {
        // given a db with some nodes and populated explicit indexes
        db.ensureStarted();
        String key = "key";
        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> nodeIndex = db.index().forNodes( "NODE" );
            Index<Relationship> relationshipIndex = db.index().forRelationships( "RELATIONSHIP" );

            for ( int i = 0; i < 10; i++ )
            {
                Node node = db.createNode();
                Relationship relationship = node.createRelationshipTo( node, MyRelTypes.TEST );
                nodeIndex.add( node, key, String.valueOf( i ) );
                relationshipIndex.add( relationship, key, String.valueOf( i ) );
            }
            tx.success();
        }

        // when restarted as read-only
        db.restartDatabase( GraphDatabaseSettings.read_only.name(), TRUE.toString() );
        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> nodeIndex = db.index().forNodes( db.index().nodeIndexNames()[0] );
            Index<Relationship> relationshipIndex = db.index().forRelationships( db.index().relationshipIndexNames()[0] );

            // then try and read the indexes
            for ( int i = 0; i < 10; i++ )
            {
                assertNotNull( nodeIndex.get( key, String.valueOf( i ) ).getSingle() );
                assertNotNull( relationshipIndex.get( key, String.valueOf( i ) ).getSingle() );
            }
            tx.success();
        }
    }

    @Test
    public void shouldNotCreateIndexesForReadOnlyDb()
    {
        // given
        db.ensureStarted( GraphDatabaseSettings.read_only.name(), TRUE.toString() );

        // when
        try ( Transaction tx = db.beginTx() )
        {
            db.index().forNodes( "NODE" );
            fail( "Should've failed" );
        }
        catch ( WriteOperationsNotAllowedException e )
        {
            // then good
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.index().forRelationships( "RELATIONSHIP" );
            fail( "Should've failed" );
        }
        catch ( WriteOperationsNotAllowedException e )
        {
            // then good
        }
    }
}
