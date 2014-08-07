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
package org.neo4j.kernel.impl.store.impl;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileLock;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.format.TestHeaderlessStoreFormat;
import org.neo4j.kernel.impl.store.standard.IdGeneratorRebuilder;
import org.neo4j.kernel.impl.store.standard.StoreFormat;
import org.neo4j.kernel.impl.store.standard.StoreOpenCloseCycle;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.EphemeralFileSystemRule;

import static org.mockito.Mockito.*;

public class StoreOpenCloseCycleTest
{
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    @Test
    public void shouldLockAndUnlock() throws Exception
    {
        // Given
        StoreFormat<?,?> format = mock(StoreFormat.class);
        when(format.version()).thenReturn( "v1.0.0" );
        when(format.type()).thenReturn( "SomeFormat" );

        File dbFileName = new File( "/store" );
        StoreChannel channel = mock(StoreChannel.class);

        FileLock lock = mock(FileLock.class);

        FileSystemAbstraction fs = mock(FileSystemAbstraction.class);
        when(fs.tryLock( dbFileName, channel )).thenReturn( lock );

        StoreOpenCloseCycle logic = new StoreOpenCloseCycle( StringLogger.DEV_NULL,
                dbFileName, format, fs );

        // When
        logic.openStore(channel, mock(IdGeneratorRebuilder.class));

        // Then
        verify( fs ).tryLock(dbFileName, channel );

        // And when
        logic.closeStore( channel, 0 );

        // Then
        verify( lock ).release();
    }

    @Test
    public void shouldRebuildIdGeneratorIfStoreIsNotClean() throws Exception
    {
        // Given
        File storeFile = new File( "store" );
        IdGeneratorRebuilder idGenRebuilder = mock(IdGeneratorRebuilder.class);
        EphemeralFileSystemAbstraction fs = fsRule.get();
        TestHeaderlessStoreFormat format = new TestHeaderlessStoreFormat();

        StoreOpenCloseCycle cycle = new StoreOpenCloseCycle( StringLogger.DEV_NULL,
                storeFile, format, fs );

        // And given the file exists (but contains no headers, and should thus be considered unclean)
        fs.create( storeFile );
        StoreChannel channel = fs.open( storeFile, "rw" );

        // When
        cycle.openStore( channel, idGenRebuilder);

        // Then
        verify( idGenRebuilder ).rebuildIdGenerator();
        verifyNoMoreInteractions( idGenRebuilder );
    }

    @Test
    public void cleanlyShutdownStoreShouldNotHaveIdGeneratorRebuilt() throws Exception
    {
        // Given
        File storeFile = new File( "store" );
        EphemeralFileSystemAbstraction fs = fsRule.get();

        StoreChannel channel = newCleanStore( storeFile );

        IdGeneratorRebuilder idGenRebuilder = mock( IdGeneratorRebuilder.class );
        StoreOpenCloseCycle cycle = new StoreOpenCloseCycle( StringLogger.DEV_NULL,
                storeFile, new TestHeaderlessStoreFormat(), fs );

        // When
        cycle.openStore( channel, mock(IdGeneratorRebuilder.class));

        // Then
        verifyNoMoreInteractions( idGenRebuilder );
    }

    private StoreChannel newCleanStore( File storeFile ) throws IOException
    {
        EphemeralFileSystemAbstraction fs = fsRule.get();
        StoreOpenCloseCycle cycle = new StoreOpenCloseCycle( StringLogger.DEV_NULL,
                storeFile, new TestHeaderlessStoreFormat(), fs);

        // And given a cleanly shut down store
        fs.create( storeFile );
        StoreChannel channel = fs.open( storeFile, "rw" );
        cycle.openStore( channel, mock(IdGeneratorRebuilder.class));
        cycle.closeStore( channel, 1 );
        return channel;
    }
}
