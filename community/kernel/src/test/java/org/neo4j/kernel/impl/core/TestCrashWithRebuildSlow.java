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
package org.neo4j.kernel.impl.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.helpers.collection.IteratorUtil.count;

import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.TargetDirectory;
import org.neo4j.tooling.GlobalGraphOperations;

/**
 * Test for making sure that slow id generator rebuild is exercised and also a problem
 * @author Mattias Persson
 */
public class TestCrashWithRebuildSlow
{
    @Test
    public void crashAndRebuildSlowWithDynamicStringDeletions() throws Exception
    {
        // Produce the string store with holes in it
        String dir = TargetDirectory.forTest( getClass() ).directory( "holes", true ).getAbsolutePath();
        Process process = Runtime.getRuntime().exec( new String[]{
            "java", "-cp", System.getProperty( "java.class.path" ),
            ProduceNonCleanDefraggedStringStore.class.getName(), dir
        } );

        int processResult = new ProcessStreamHandler( process, true ).waitForResult();

        assertEquals( 0, processResult );
        
        // Recover with rebuild_idgenerators_fast=false
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( dir ).setConfig( GraphDatabaseSettings.rebuild_idgenerators_fast, GraphDatabaseSetting.FALSE ).newGraphDatabase();
        try
        {
            int nameCount = 0;
            int relCount = 0;
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                if ( node.equals( db.getReferenceNode() ) )
                    continue;
                nameCount++;
                assertNotNull( node.getProperty( "name" ) );
                relCount += count( node.getRelationships( Direction.OUTGOING ) );
            }
            
            assertEquals( 16, nameCount );
            assertEquals( 12, relCount );
        }
        finally
        {
            db.shutdown();
        }
    }
}
