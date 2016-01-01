/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Note by Tobias & Jacob 2013-04-17:
 *
 * This test does NOT verify that the algorithm produces sane values, just that it produces values in all cases.
 * We need to verify that the expected values in this test are sensible,
 * and change the algorithm accordingly if they aren't.
 */
public class AutoConfiguratorTest
{
    private static final long MiB = 1024 * 1024, GiB = 1024 * MiB;
    @Rule
    public final TestName testName = new TestName();
    private File storeDir;

    @Before
    public void given()
    {
        storeDir = TargetDirectory.forTest( getClass() ).cleanDirectory( testName.getMethodName() );
    }

    @Test
    public void shouldNotConfigureMemoryMappingWhenUnableToAccessPhysicalMemorySize() throws Exception
    {
        // given
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        ConsoleLogger logger = Mockito.mock( ConsoleLogger.class );
        mockFileSize( fs, "nodestore.db", 200 * GiB );
        mockFileSize( fs, "relationshipstore.db", 200 * GiB );
        mockFileSize( fs, "relationshipgroupstore.db", 200 * GiB );
        mockFileSize( fs, "propertystore.db", 200 * GiB );
        mockFileSize( fs, "propertystore.db.strings", 200 * GiB );
        mockFileSize( fs, "propertystore.db.arrays", 200 * GiB );
        long vmMemory = 512 * MiB;
        long physicalMemory = -1;

        // when
        AutoConfigurator autoConf = new AutoConfigurator( fs, storeDir, true, physicalMemory, vmMemory, logger );

        // then
        verify( logger ).warn( "Could not determine the size of the physical memory. Continuing but without memory mapped buffers." );
        verifyNoMoreInteractions( logger );
        assertEquals( Collections.<String, String>emptyMap(), autoConf.configure() );
    }

    @Test
    public void shouldProvideDefaultAssignmentsForHugeFiles() throws Exception
    {
        // given
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        mockFileSize( fs, "nodestore.db", 200 * GiB );
        mockFileSize( fs, "relationshipstore.db", 200 * GiB );
        mockFileSize( fs, "relationshipgroupstore.db", 200 * GiB );
        mockFileSize( fs, "propertystore.db", 200 * GiB );
        mockFileSize( fs, "propertystore.db.strings", 200 * GiB );
        mockFileSize( fs, "propertystore.db.arrays", 200 * GiB );

        long availableMem = 100000;
        // reverse the internal formula
        long physicalMemory = 2 * availableMem;
        long vmMemory = (long) Math.ceil( physicalMemory - (availableMem / 0.85) );

        AutoConfigurator autoConf = new AutoConfigurator( fs, storeDir, true, physicalMemory * MiB, vmMemory * MiB, Mockito.mock(ConsoleLogger.class) );

        // when
        Map<String, String> configuration = autoConf.configure();

        // then
        assertMappedMemory( configuration, "75000M", "relationshipstore.db" );
        assertMappedMemory( configuration, " 5000M", "nodestore.db" );
        assertMappedMemory( configuration, "    2M", "relationshipgroupstore.db" );
        assertMappedMemory( configuration, "14998M", "propertystore.db" );
        assertMappedMemory( configuration, " 3750M", "propertystore.db.strings" );
        assertMappedMemory( configuration, " 1250M", "propertystore.db.arrays" );
    }

    @Test
    public void shouldProvideDefaultAssignmentsForHugeFilesWhenMemoryMappingIsNotUsed() throws Exception
    {
        // given
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        mockFileSize( fs, "nodestore.db", 200 * GiB );
        mockFileSize( fs, "relationshipstore.db", 200 * GiB );
        mockFileSize( fs, "relationshipgroupstore.db", 200 * GiB );
        mockFileSize( fs, "propertystore.db", 200 * GiB );
        mockFileSize( fs, "propertystore.db.strings", 200 * GiB );
        mockFileSize( fs, "propertystore.db.arrays", 200 * GiB );

        long availableMem = 100000;

        AutoConfigurator autoConf = new AutoConfigurator( fs, storeDir, false, 10000 * GiB, 2 * availableMem * MiB, Mockito.mock(ConsoleLogger.class) );

        // when
        Map<String, String> configuration = autoConf.configure();

        // then
        assertMappedMemory( configuration, "75000M", "relationshipstore.db" );
        assertMappedMemory( configuration, " 5000M", "nodestore.db" );
        assertMappedMemory( configuration, "    2M", "relationshipgroupstore.db" );
        assertMappedMemory( configuration, "14998M", "propertystore.db" );
        assertMappedMemory( configuration, " 3750M", "propertystore.db.strings" );
        assertMappedMemory( configuration, " 1250M", "propertystore.db.arrays" );
    }

    @Test
    public void shouldProvideDefaultAssignmentsForEmptyFiles() throws Exception
    {
        // given
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        mockFileSize( fs, "nodestore.db", 0 );
        mockFileSize( fs, "relationshipstore.db", 0 );
        mockFileSize( fs, "relationshipgroupstore.db", 0 );
        mockFileSize( fs, "propertystore.db", 0 );
        mockFileSize( fs, "propertystore.db.strings", 0 );
        mockFileSize( fs, "propertystore.db.arrays", 0 );

        long availableMem = 100000;
        // reverse the internal formula
        long physicalMemory = 2 * availableMem;
        long vmMemory = (long) Math.ceil( physicalMemory - (availableMem / 0.85) );

        AutoConfigurator autoConf = new AutoConfigurator( fs, storeDir, true, physicalMemory * MiB, vmMemory * MiB, Mockito.mock(ConsoleLogger.class) );

        // when
        Map<String, String> configuration = autoConf.configure();

        // then
        assertMappedMemory( configuration, "12500M", "relationshipstore.db" );
        assertMappedMemory( configuration, " 2916M", "nodestore.db" );
        assertMappedMemory( configuration, "    1M", "relationshipgroupstore.db" );
        assertMappedMemory( configuration, "10572M", "propertystore.db" );
        assertMappedMemory( configuration, " 9251M", "propertystore.db.strings" );
        assertMappedMemory( configuration, "10793M", "propertystore.db.arrays" );
    }

    @Test
    public void shouldProvideDefaultAssignmentsForEmptyFilesWhenMemoryMappingIsNotUsed() throws Exception
    {
        // given
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        mockFileSize( fs, "nodestore.db", 0 );
        mockFileSize( fs, "relationshipstore.db", 0 );
        mockFileSize( fs, "relationshipgroupstore.db", 0 );
        mockFileSize( fs, "propertystore.db", 0 );
        mockFileSize( fs, "propertystore.db.strings", 0 );
        mockFileSize( fs, "propertystore.db.arrays", 0 );

        long availableMem = 100000;

        AutoConfigurator autoConf = new AutoConfigurator( fs, storeDir, false, 10000 * GiB, 2 * availableMem * MiB, Mockito.mock(ConsoleLogger.class) );

        // when
        Map<String, String> configuration = autoConf.configure();

        // then
        assertMappedMemory( configuration, "12500M", "relationshipstore.db" );
        assertMappedMemory( configuration, " 2916M", "nodestore.db" );
        assertMappedMemory( configuration, "    1M", "relationshipgroupstore.db" );
        assertMappedMemory( configuration, "10572M", "propertystore.db" );
        assertMappedMemory( configuration, " 9251M", "propertystore.db.strings" );
        assertMappedMemory( configuration, "10793M", "propertystore.db.arrays" );
    }

    @Test
    public void shouldProvideZeroMappedMemoryWhenPhysicalLessThanJVMMemory() throws Exception
    {
        // given
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        mockFileSize( fs, "nodestore.db", 0 );
        mockFileSize( fs, "relationshipstore.db", 0 );
        mockFileSize( fs, "relationshipgroupstore.db", 0 );
        mockFileSize( fs, "propertystore.db", 0 );
        mockFileSize( fs, "propertystore.db.strings", 0 );
        mockFileSize( fs, "propertystore.db.arrays", 0 );

        ConsoleLogger mock = Mockito.mock( ConsoleLogger.class );
        AutoConfigurator autoConf = new AutoConfigurator( fs, storeDir, true, 128 * MiB, 256 * MiB, mock );

        // when
        Map<String, String> configuration = autoConf.configure();

        // then
        verify( mock ).log( startsWith( "WARNING!" ) );
        assertMappedMemory( configuration, "0M", "relationshipstore.db" );
        assertMappedMemory( configuration, "0M", "relationshipgroupstore.db" );
        assertMappedMemory( configuration, "0M", "nodestore.db" );
        assertMappedMemory( configuration, "0M", "propertystore.db" );
        assertMappedMemory( configuration, "0M", "propertystore.db.strings" );
        assertMappedMemory( configuration, "0M", "propertystore.db.arrays" );
    }

    private void assertMappedMemory( Map<String, String> configuration, String expected, String store )
    {
        assertEquals( store, expected.trim(), configuration.get( "neostore." + store + ".mapped_memory" ) );
    }

    private void mockFileSize( FileSystemAbstraction fs, String file, long size )
    {
        when( fs.getFileSize( new File( storeDir, "neostore." + file ) ) ).thenReturn( size );
    }
}
