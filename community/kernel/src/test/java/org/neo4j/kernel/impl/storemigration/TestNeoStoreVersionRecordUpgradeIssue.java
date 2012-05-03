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
package org.neo4j.kernel.impl.storemigration;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.URL;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.ProduceUncleanStore;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.test.TargetDirectory;

public class TestNeoStoreVersionRecordUpgradeIssue
{
    public final @Rule TestName testName = new TestName();
    
    @Test
    public void upgradeOneFiveMilestoneTwoWhereStoreVersionRecordShouldBeAddedCorrectly() throws Exception
    {
        File storeDir = copyResourceStore( "1.5.M02-store" );
        startAndShutdown( storeDir );
        startAndKill( storeDir );
        startAndShutdown( storeDir );
    }

    private void startAndKill( File storeDir ) throws Exception
    {
        assertEquals( 0, Runtime.getRuntime().exec( new String[] { "java", "-cp", System.getProperty( "java.class.path" ),
                ProduceUncleanStore.class.getName(), storeDir.getAbsolutePath() } ).waitFor() );
    }

    private void startAndShutdown( File storeDir )
    {
        new EmbeddedGraphDatabase( storeDir.getAbsolutePath() ).shutdown();
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
