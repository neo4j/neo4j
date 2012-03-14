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
package org.neo4j.kernel.impl.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.nioneo.store.NeoStore.versionStringToLong;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.CommonFactories;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NotCurrentStoreVersionException;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;

public class TestIndexProviderStore
{
    private File file;
    private FileSystemAbstraction fileSystem;

    @Before
    public void createStore()
    {
        file = new File( "target/test-data/index-provider-store" );
        fileSystem = CommonFactories.defaultFileSystemAbstraction();
        file.mkdirs();
        file.delete();
    }
    
    @Test
    public void lastCommitedTxGetsStoredBetweenSessions() throws Exception
    {
        IndexProviderStore store = new IndexProviderStore( file, fileSystem, 0, false );
        store.setVersion( 5 );
        store.setLastCommittedTx( 12 );
        store.close();
        store = new IndexProviderStore( file, fileSystem, 0, false );
        assertEquals( 5, store.getVersion() );
        assertEquals( 12, store.getLastCommittedTx() );
        store.close();
    }
    
    @Test
    public void shouldFailUpgradeIfNotAllowed()
    {
        IndexProviderStore store = new IndexProviderStore( file, fileSystem, versionStringToLong( "3.1" ), true );
        store.close();
        store = new IndexProviderStore( file, fileSystem, versionStringToLong( "3.1" ), false );
        store.close();
        try
        {
            new IndexProviderStore( file, fileSystem, versionStringToLong( "3.5" ), false );
            fail( "Shouldn't be able to upgrade there" );
        }
        catch ( UpgradeNotAllowedByConfigurationException e )
        {   // Good
        }
        store = new IndexProviderStore( file, fileSystem, versionStringToLong( "3.5" ), true );
        assertEquals( "3.5", NeoStore.versionLongToString( store.getIndexVersion() ) );
        store.close();
    }
    
    @Test( expected = NotCurrentStoreVersionException.class )
    public void shouldFailToGoBackToOlderVersion() throws Exception
    {
        String newerVersion = "3.5";
        String olderVersion = "3.1";
        try
        {
            IndexProviderStore store = new IndexProviderStore( file, fileSystem, versionStringToLong( newerVersion ), true );
            store.close();
            store = new IndexProviderStore( file, fileSystem, versionStringToLong( olderVersion ), false );
        }
        catch ( NotCurrentStoreVersionException e )
        {
            assertTrue( e.getMessage().contains( newerVersion ) );
            assertTrue( e.getMessage().contains( olderVersion ) );
            throw e;
        }
    }

    @Test( expected = NotCurrentStoreVersionException.class )
    public void shouldFailToGoBackToOlderVersionEvenIfAllowUpgrade() throws Exception
    {
        String newerVersion = "3.5";
        String olderVersion = "3.1";
        try
        {
            IndexProviderStore store = new IndexProviderStore( file, fileSystem, versionStringToLong( newerVersion ), true );
            store.close();
            store = new IndexProviderStore( file, fileSystem, versionStringToLong( olderVersion ), true );
        }
        catch ( NotCurrentStoreVersionException e )
        {
            assertTrue( e.getMessage().contains( newerVersion ) );
            assertTrue( e.getMessage().contains( olderVersion ) );
            throw e;
        }
    }
}
