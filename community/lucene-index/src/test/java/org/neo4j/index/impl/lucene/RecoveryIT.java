/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.tooling.GlobalGraphOperations;

public class RecoveryIT
{
    @Test
    public void testHardCoreRecovery() throws Exception
    {
        String path = "target/hcdb";
        FileUtils.deleteRecursively( new File( path ) );
        Process process = Runtime.getRuntime().exec( new String[] {
                "java", "-cp", System.getProperty( "java.class.path" ),
                Inserter.class.getName(), path
        } );
        
        // Let it run for a while and then kill it, and wait for it to die
        Thread.sleep( 5000 );
        process.destroy();
        process.waitFor();
        
        GraphDatabaseService db = new EmbeddedGraphDatabase( path );
        assertTrue( db.index().existsForNodes( "myIndex" ) );
        Index<Node> index = db.index().forNodes( "myIndex" );
        for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
        {
            for ( String key : node.getPropertyKeys() )
            {
                String value = (String) node.getProperty( key );
                boolean found = false;
                for ( Node indexedNode : index.get( key, value ) )
                {
                    if ( indexedNode.equals( node ) )
                    {
                        found = true;
                        break;
                    }
                }
                if ( !found )
                {
                    throw new IllegalStateException( node + " has property '" + key + "'='" +
                            value + "', but not in index" );
                }
            }
        }
        db.shutdown();
    }
}
