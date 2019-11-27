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
package org.neo4j.internal.index.label;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static java.lang.Math.abs;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@Neo4jLayoutExtension
class LabelScanWriteMonitorTest
{
    @Inject
    private DefaultFileSystemAbstraction fs;
    @Inject
    private DatabaseLayout databaseLayout;

    private String baseName;

    @BeforeEach
    void before()
    {
        baseName = LabelScanWriteMonitor.writeLogBaseFile( databaseLayout ).getName();
    }

    @Test
    void shouldRotateExistingFileOnOpen()
    {
        // given
        LabelScanWriteMonitor writeMonitor = new LabelScanWriteMonitor( fs, databaseLayout );
        writeMonitor.close();

        // when
        LabelScanWriteMonitor secondWriteMonitor = new LabelScanWriteMonitor( fs, databaseLayout );
        secondWriteMonitor.close();

        // then
        assertEquals( 2, requireNonNull( databaseLayout.databaseDirectory().listFiles( ( dir, name ) -> name.startsWith( baseName ) ) ).length );
    }

    @Test
    void shouldLogAndDumpData() throws IOException
    {
        // given
        LabelScanWriteMonitor writeMonitor = new LabelScanWriteMonitor( fs, databaseLayout );
        LabelScanValue value = new LabelScanValue();
        writeMonitor.range( 3, 0 );
        writeMonitor.prepareAdd( 123, 4 );
        writeMonitor.prepareAdd( 123, 5 );
        writeMonitor.mergeAdd( new LabelScanValue(), value.set( 4 ).set( 5 ) );
        writeMonitor.flushPendingUpdates();
        writeMonitor.prepareRemove( 124, 5 );
        writeMonitor.mergeRemove( value, new LabelScanValue().set( 5 ) );
        writeMonitor.writeSessionEnded();
        writeMonitor.range( 5, 1 );
        writeMonitor.prepareAdd( 125, 10 );
        writeMonitor.mergeAdd( new LabelScanValue().set( 9 ), new LabelScanValue().set( 10 ) );
        writeMonitor.flushPendingUpdates();
        writeMonitor.writeSessionEnded();
        writeMonitor.close();

        // when
        LabelScanWriteMonitor.Dumper dumper = mock( LabelScanWriteMonitor.Dumper.class );
        LabelScanWriteMonitor.dump( fs, databaseLayout, dumper, null );

        // then
        InOrder inOrder = Mockito.inOrder( dumper );
        inOrder.verify( dumper ).prepare( true, 0, 0, 123, 64 * 3 + 4, 0 );
        inOrder.verify( dumper ).prepare( true, 0, 0, 123, 64 * 3 + 5, 0 );
        inOrder.verify( dumper ).merge( true, 0, 0, 3, 0, 0,
                0b00000000_0000000_00000000_00000000__00000000_00000000_00000000_00110000 );
        inOrder.verify( dumper ).prepare( false, 0, 1, 124, 64 * 3 + 5, 0 );
        inOrder.verify( dumper ).merge( false, 0, 1, 3, 0,
                0b00000000_0000000_00000000_00000000__00000000_00000000_00000000_00110000,
                0b00000000_0000000_00000000_00000000__00000000_00000000_00000000_00100000 );
        inOrder.verify( dumper ).prepare( true, 1, 0, 125, 64 * 5 + 10, 1 );
        inOrder.verify( dumper ).merge( true, 1, 0, 5, 1,
                0b00000000_0000000_00000000_00000000__00000000_00000000_00000010_00000000,
                0b00000000_0000000_00000000_00000000__00000000_00000000_00000100_00000000 );
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldParseSimpleSingleTxFilter()
    {
        // given
        LabelScanWriteMonitor.TxFilter txFilter = LabelScanWriteMonitor.parseTxFilter( "123" );

        // when/then
        assertFalse( txFilter.contains( 122 ) );
        assertTrue( txFilter.contains( 123 ) );
        assertFalse( txFilter.contains( 124 ) );
    }

    @Test
    void shouldParseRangedSingleTxFilter()
    {
        // given
        LabelScanWriteMonitor.TxFilter txFilter = LabelScanWriteMonitor.parseTxFilter( "123-126" );

        // when/then
        assertFalse( txFilter.contains( 122 ) );
        assertTrue( txFilter.contains( 123 ) );
        assertTrue( txFilter.contains( 124 ) );
        assertTrue( txFilter.contains( 125 ) );
        assertTrue( txFilter.contains( 126 ) );
        assertFalse( txFilter.contains( 127 ) );
    }

    @Test
    void shouldParseSimpleMultipleTxFilters()
    {
        // given
        LabelScanWriteMonitor.TxFilter txFilter = LabelScanWriteMonitor.parseTxFilter( "123,146,123456" );

        // when/then
        assertFalse( txFilter.contains( 122 ) );
        assertTrue( txFilter.contains( 123 ) );
        assertTrue( txFilter.contains( 146 ) );
        assertTrue( txFilter.contains( 123456 ) );
        assertFalse( txFilter.contains( 147 ) );
    }

    @Test
    void shouldParseRangedMultipleTxFilters()
    {
        // given
        LabelScanWriteMonitor.TxFilter txFilter = LabelScanWriteMonitor.parseTxFilter( "123-125,345-567" );

        // when/then
        assertFalse( txFilter.contains( 122 ) );
        assertTrue( txFilter.contains( 123 ) );
        assertTrue( txFilter.contains( 124 ) );
        assertTrue( txFilter.contains( 125 ) );
        assertFalse( txFilter.contains( 201 ) );
        assertTrue( txFilter.contains( 345 ) );
        assertTrue( txFilter.contains( 405 ) );
        assertTrue( txFilter.contains( 567 ) );
        assertFalse( txFilter.contains( 568 ) );
    }

    @Test
    void shouldRotateAtConfiguredThreshold()
    {
        // given
        File storeDir = databaseLayout.databaseDirectory();
        int rotationThreshold = 1_000;
        LabelScanWriteMonitor writeMonitor = new LabelScanWriteMonitor( fs, databaseLayout, rotationThreshold, ByteUnit.Byte, 1, TimeUnit.DAYS );

        // when
        for ( int i = 0; requireNonNull( storeDir.listFiles() ).length < 5; i++ )
        {
            writeMonitor.range( i, 1 );
            writeMonitor.prepareAdd( i, 5 );
            writeMonitor.mergeAdd( new LabelScanValue(), new LabelScanValue().set( 5 ) );
            writeMonitor.writeSessionEnded();
        }

        // then
        writeMonitor.close();
        for ( File file : requireNonNull( storeDir.listFiles( ( dir, name ) -> !name.equals( baseName ) ) ) )
        {
            long sizeDiff = abs( rotationThreshold - fs.getFileSize( file ) );
            assertTrue( sizeDiff < rotationThreshold / 10D );
        }
    }

    @Test
    void shouldPruneAtConfiguredThreshold()
    {
        // given
        File storeDir = databaseLayout.databaseDirectory();
        long pruneThreshold = 200;
        LabelScanWriteMonitor writeMonitor =
                new LabelScanWriteMonitor( fs, databaseLayout, 1_000, ByteUnit.Byte, pruneThreshold, TimeUnit.MILLISECONDS );

        // when
        long startTime = currentTimeMillis();
        long endTime = startTime + TimeUnit.SECONDS.toMillis( 1 );
        for ( int i = 0; currentTimeMillis() < endTime; i++ )
        {
            writeMonitor.range( i, 1 );
            writeMonitor.prepareAdd( i, 5 );
            writeMonitor.mergeAdd( new LabelScanValue(), new LabelScanValue().set( 5 ) );
            writeMonitor.writeSessionEnded();
        }
        long loopEnded = currentTimeMillis();

        // then
        writeMonitor.close();
        for ( File file : requireNonNull( storeDir.listFiles( ( dir, name ) -> !name.equals( baseName ) ) ) )
        {
            long timestamp = LabelScanWriteMonitor.millisOf( file );
            long diff = endTime - timestamp;
            assertThat( diff ).isLessThan( (loopEnded - endTime) + pruneThreshold * 2 );
        }
    }
}
