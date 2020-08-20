/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.server;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.lang.management.MemoryUsage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.neo4j.configuration.ExternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.memory.MachineMemory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class NeoBootstrapperTest
{
    @Inject
    private TestDirectory homeDir;
    @Inject
    private SuppressOutput suppress;
    private NeoBootstrapper neoBootstrapper;

    @AfterEach
    void tearDown()
    {
        if ( neoBootstrapper != null )
        {
            neoBootstrapper.stop();
        }
    }

    @Test
    void shouldNotThrowNullPointerExceptionIfConfigurationValidationFails() throws Exception
    {
        // given
        neoBootstrapper = new CommunityBootstrapper();

        Path dir = Files.createTempDirectory( "test-server-bootstrapper" );

        // when
        neoBootstrapper.start( dir, MapUtil.stringMap() );

        // then no exceptions are thrown and
        assertThat( suppress.getOutputVoice().lines() ).isNotEmpty();
    }

    @Test
    void shouldFailToStartIfRequestedPageCacheMemoryExceedsAvailable() throws Exception
    {
        // given
        neoBootstrapper = new CommunityBootstrapper();
        Path dir = Files.createTempDirectory( "test-server-bootstrapper" );
        Map<String,String> config = MapUtil.stringMap();
        config.put( GraphDatabaseSettings.pagecache_memory.name(), "10B" );

        // Mock heap usage and free memory.
        MachineMemory mockedMemory = mock( MachineMemory.class );
        MemoryUsage heapMemory = new MemoryUsage( 0, 1, 1, 1 );
        when( mockedMemory.getFreePhysicalMemory() ).thenReturn( 8L );
        when( mockedMemory.getHeapMemoryUsage() ).thenReturn( heapMemory );
        neoBootstrapper.setMachineMemory( mockedMemory );

        assertThat( neoBootstrapper.start( dir, config ) ).isNotEqualTo( NeoBootstrapper.OK );
        assertThat( suppress.getOutputVoice().lines() )
                .anySatisfy( line -> assertThat( line ).containsSubsequence( "Invalid memory configuration - exceeds available physical memory." ) );
    }

    @Test
    void shouldFailToStartIfRequestedHeapMemoryExceedsAvailable() throws Exception
    {
        // given
        neoBootstrapper = new CommunityBootstrapper();
        Path dir = Files.createTempDirectory( "test-server-bootstrapper" );
        Map<String,String> config = MapUtil.stringMap();
        config.put( ExternalSettings.max_heap_size.name(), "10B" );
        config.put( GraphDatabaseSettings.pagecache_memory.name(), "1B" );

        // Mock heap usage and free memory.
        MachineMemory mockedMemory = mock( MachineMemory.class );
        MemoryUsage heapMemory = new MemoryUsage( 0, 1, 1, 10 );
        when( mockedMemory.getFreePhysicalMemory() ).thenReturn( 8L );
        when( mockedMemory.getHeapMemoryUsage() ).thenReturn( heapMemory );
        neoBootstrapper.setMachineMemory( mockedMemory );

        assertThat( neoBootstrapper.start( dir, config ) ).isNotEqualTo( NeoBootstrapper.OK );
        assertThat( suppress.getOutputVoice().lines() )
                .anySatisfy( line -> assertThat( line ).containsSubsequence( "Invalid memory configuration - exceeds available physical memory." ) );
    }

    @Test
    void shouldFailToStartIfRequestedHeapAndPageCacheMemoryExceedsAvailable() throws Exception
    {
        // given
        neoBootstrapper = new CommunityBootstrapper();
        Path dir = Files.createTempDirectory( "test-server-bootstrapper" );
        Map<String,String> config = MapUtil.stringMap();
        config.put( ExternalSettings.max_heap_size.name(), "10B" );
        config.put( GraphDatabaseSettings.pagecache_memory.name(), "10B" );

        // Mock heap usage and free memory.
        MachineMemory mockedMemory = mock( MachineMemory.class );
        MemoryUsage heapMemory = new MemoryUsage( 0, 1, 1, 10 );
        when( mockedMemory.getFreePhysicalMemory() ).thenReturn( 18L );
        when( mockedMemory.getHeapMemoryUsage() ).thenReturn( heapMemory );
        neoBootstrapper.setMachineMemory( mockedMemory );

        assertThat( neoBootstrapper.start( dir, config ) ).isNotEqualTo( NeoBootstrapper.OK );
        assertThat( suppress.getOutputVoice().lines() )
                .anySatisfy( line -> assertThat( line ).containsSubsequence( "Invalid memory configuration - exceeds available physical memory." ) );
    }

    @Test
    void shouldFailToStartIfCalculatedPageCacheSizeExceedsAvailableMemory() throws Exception
    {
        // given
        neoBootstrapper = new CommunityBootstrapper();
        Path dir = Files.createTempDirectory( "test-server-bootstrapper" );
        Map<String,String> config = MapUtil.stringMap();
        config.put( ExternalSettings.max_heap_size.name(), "10B" );

        // Mock heap usage and free memory.
        MachineMemory mockedMemory = mock( MachineMemory.class );
        MemoryUsage heapMemory = new MemoryUsage( 0, 1, 1, 10 );
        when( mockedMemory.getFreePhysicalMemory() ).thenReturn( 1000L );
        when( mockedMemory.getTotalPhysicalMemory() ).thenReturn( 2000L );
        when( mockedMemory.getHeapMemoryUsage() ).thenReturn( heapMemory );
        neoBootstrapper.setMachineMemory( mockedMemory );

        assertThat( neoBootstrapper.start( dir, config ) ).isNotEqualTo( NeoBootstrapper.OK );
        assertThat( suppress.getOutputVoice().lines() )
                .anySatisfy( line -> assertThat( line ).containsSubsequence( "Invalid memory configuration - exceeds available physical memory." ) );
    }
}
