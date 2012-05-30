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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

import java.io.File;
import java.net.URL;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.TargetDirectory;

/**
 * Tests upgrade from a neo4j 1.5 store (which had lucene version 3.1.0) to a
 * new neo4j version which has lucene version 3.5.0
 */
public class LuceneThreeFiveUpgradeIT
{
    public final @Rule TestName testName = new TestName();
    
    @Test
    public void upgradeShouldFailIfNotAllowed() throws Exception
    {
        File storeDir = copyResourceStore( "1.5-store" );
        assertDisallowedUpgradeFails( storeDir );
    }

    @Test
    public void upgradeShouldSucceedIfAllowed() throws Exception
    {
        File storeDir = copyResourceStore( "1.5-store" );
        assertIndexContainsNode( storeDir, stringMap( Config.ALLOW_STORE_UPGRADE, "true" ) );
        assertIndexContainsNode( storeDir, stringMap() );
    }
    
    @Test
    public void upgradeThenUncleanShutdownShouldHaveUpgraded() throws Exception
    {
        // Upgrade, but shutdown unclean
        File storeDir = copyResourceStore( "1.5-store" );
        Process process = Runtime.getRuntime().exec( new String[] { "java", "-cp", System.getProperty( "java.class.path" ),
                StartAndKill.class.getName(), storeDir.getAbsolutePath(), "-" + Config.ALLOW_STORE_UPGRADE + "=true"
        } );
        new ProcessStreamHandler( process, true ).launch();
        assertEquals( 1, process.waitFor() );
        
        // Start again w/o upgrade config set
        assertIndexContainsNode( storeDir, stringMap() );
    }
    
    @Test
    public void upgradeOfNonCleanStoreShouldAlsoWork() throws Exception
    {
        File storeDir = copyResourceStore( "unclean-1.5-store" );
        assertIndexContainsNode( storeDir, stringMap( Config.ALLOW_STORE_UPGRADE, "true" ) );
    }
    
    @Test
    public void upgradeOfNonCleanStoreWithoutAllowUpgradeShouldFail() throws Exception
    {
        File storeDir = copyResourceStore( "unclean-1.5-store" );
        assertDisallowedUpgradeFails( storeDir );
    }

    private void assertDisallowedUpgradeFails( File storeDir )
    {
        try
        {
            new EmbeddedGraphDatabase( storeDir.getAbsolutePath() );
            fail( "Shouldn't be able to start" );
        }
        catch ( Exception e )
        {   // Good
        }
    }
    
    private void assertIndexContainsNode( File storeDir, Map<String, String> config )
    {
        GraphDatabaseService db = new EmbeddedGraphDatabase( storeDir.getAbsolutePath(), config );
        try
        {
            Node node = db.getNodeById( 1 );
            assertEquals( node, db.index().forNodes( "persons" ).get( "name", node.getProperty( "name" ) ).getSingle() );
        }
        finally
        {
            db.shutdown();
        }
    }
    
    private File copyResourceStore( String resource ) throws Exception
    {
        URL uri = getClass().getResource( resource );
        File file = new File( uri.toURI() );
        File target = TargetDirectory.forTest( getClass() ).directory( testName.getMethodName(), true );
        FileUtils.copyRecursively( file, target );
        return target;
    }
}
