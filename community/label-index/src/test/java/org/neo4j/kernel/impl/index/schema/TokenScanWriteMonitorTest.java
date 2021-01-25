/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.index.schema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.lang.Long.min;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;

@Neo4jLayoutExtension
@ExtendWith( RandomExtension.class )
class TokenScanWriteMonitorTest
{
    @Inject
    private DefaultFileSystemAbstraction fs;
    @Inject
    private DatabaseLayout databaseLayout;
    @Inject
    private RandomRule random;

    private String baseName;

    @BeforeEach
    void before()
    {
        baseName = TokenScanWriteMonitor.writeLogBaseFile( databaseLayout, NODE ).getFileName().toString();
    }

    @Test
    void shouldRotateExistingFileOnOpen() throws IOException
    {
        // given
        Config config = Config.defaults();
        TokenScanWriteMonitor writeMonitor = new TokenScanWriteMonitor( fs, databaseLayout, NODE, config );
        writeMonitor.close();

        // when
        TokenScanWriteMonitor secondWriteMonitor = new TokenScanWriteMonitor( fs, databaseLayout, NODE, config );
        secondWriteMonitor.close();

        // then
        long count = 0;
        try ( DirectoryStream<Path> paths = Files.newDirectoryStream( databaseLayout.databaseDirectory(), baseName + "*" ) )
        {
            for ( Path path : paths )
            {
                count++;
            }
        }
        assertEquals( 2, count );
    }

    @Test
    void shouldLogAndDumpData() throws IOException
    {
        // given
        TokenScanWriteMonitor writeMonitor = new TokenScanWriteMonitor( fs, databaseLayout, NODE, Config.defaults() );
        TokenScanValue value = new TokenScanValue();
        writeMonitor.range( 3, 0 );
        writeMonitor.prepareAdd( 123, 4 );
        writeMonitor.prepareAdd( 123, 5 );
        writeMonitor.mergeAdd( new TokenScanValue(), value.set( 4 ).set( 5 ) );
        writeMonitor.flushPendingUpdates();
        writeMonitor.prepareRemove( 124, 5 );
        writeMonitor.mergeRemove( value, new TokenScanValue().set( 5 ) );
        writeMonitor.writeSessionEnded();
        writeMonitor.range( 5, 1 );
        writeMonitor.prepareAdd( 125, 10 );
        writeMonitor.mergeAdd( new TokenScanValue().set( 9 ), new TokenScanValue().set( 10 ) );
        writeMonitor.flushPendingUpdates();
        writeMonitor.writeSessionEnded();
        writeMonitor.close();

        // when
        TokenScanWriteMonitor.Dumper dumper = mock( TokenScanWriteMonitor.Dumper.class );
        TokenScanWriteMonitor.dump( fs, databaseLayout, dumper, null, NODE );

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
        TokenScanWriteMonitor.TxFilter txFilter = TokenScanWriteMonitor.parseTxFilter( "123" );

        // when/then
        assertFalse( txFilter.contains( 122 ) );
        assertTrue( txFilter.contains( 123 ) );
        assertFalse( txFilter.contains( 124 ) );
    }

    @Test
    void shouldParseRangedSingleTxFilter()
    {
        // given
        TokenScanWriteMonitor.TxFilter txFilter = TokenScanWriteMonitor.parseTxFilter( "123-126" );

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
        TokenScanWriteMonitor.TxFilter txFilter = TokenScanWriteMonitor.parseTxFilter( "123,146,123456" );

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
        TokenScanWriteMonitor.TxFilter txFilter = TokenScanWriteMonitor.parseTxFilter( "123-125,345-567" );

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
    void shouldRotateAtConfiguredThreshold() throws IOException
    {
        // given
        Path storeDir = databaseLayout.databaseDirectory();
        int rotationThreshold = 1_000;
        RecordingMonitor monitor = new RecordingMonitor();
        TokenScanWriteMonitor writeMonitor = new TokenScanWriteMonitor( fs, databaseLayout, rotationThreshold, ByteUnit.Byte, 1, TimeUnit.DAYS,
                NODE, monitor, Clocks.nanoClock() );

        // when

        for ( int i = 0; numberOfFilesIn( storeDir ) < 5; i++ )
        {
            writeMonitor.range( i, 1 );
            writeMonitor.prepareAdd( i, 5 );
            writeMonitor.mergeAdd( new TokenScanValue(), new TokenScanValue().set( 5 ) );
            writeMonitor.writeSessionEnded();
        }

        // then
        writeMonitor.close();
        assertThat( monitor.rotations ).isNotEmpty();
        monitor.rotations.forEach( e -> assertThat( e.fileSize ).isGreaterThanOrEqualTo( rotationThreshold ) );
    }

    @Test
    void shouldPruneAtConfiguredThreshold()
    {
        // given
        long pruneThreshold = 200;
        RecordingMonitor monitor = new RecordingMonitor();
        FakeClock clock = Clocks.fakeClock();
        TokenScanWriteMonitor writeMonitor =
                new TokenScanWriteMonitor( fs, databaseLayout, 500, ByteUnit.Byte, pruneThreshold, TimeUnit.MILLISECONDS, NODE, monitor, clock );

        // when
        long startTime = clock.millis();
        long endTime = startTime + TimeUnit.SECONDS.toMillis( 1 );
        for ( int i = 0; clock.millis() < endTime; i++ )
        {
            long timeLeft = endTime - clock.millis();
            clock.forward( min( timeLeft, random.nextInt( 1, 10 ) ), TimeUnit.MILLISECONDS );
            writeMonitor.range( i, 1 );
            writeMonitor.prepareAdd( i, 5 );
            writeMonitor.mergeAdd( new TokenScanValue(), new TokenScanValue().set( 5 ) );
            writeMonitor.writeSessionEnded();
        }

        // then
        writeMonitor.close();
        List<Entry> remainingFiles =
                monitor.rotations.stream().filter( r -> monitor.prunes.stream().noneMatch( p -> r.timestamp == p.timestamp ) ).collect( Collectors.toList() );
        assertThat( remainingFiles ).isNotEmpty();
        assertThat( monitor.rotations.size() ).isGreaterThan( remainingFiles.size() );
        assertThat( monitor.prunes ).isNotEmpty();
        remainingFiles.forEach( e -> assertThat( endTime - e.timestamp ).isLessThan( pruneThreshold * 2 ) );
    }

    @Test
    void shouldUseTargetRelationshipTypeScanStoreIfEntityTypeRelationship()
    {
        // given
        assertThat( fs.listFiles( databaseLayout.databaseDirectory() ).length ).isEqualTo( 0 );
        TokenScanWriteMonitor writeMonitor = new TokenScanWriteMonitor( fs, databaseLayout, RELATIONSHIP, Config.defaults() );
        writeMonitor.close();
        List<Path> filesAfter = Arrays.asList( fs.listFiles( databaseLayout.databaseDirectory() ) );
        assertThat( filesAfter.size() ).isEqualTo( 1 );
        assertThat( filesAfter.get( 0 ).getFileName().toString() ).contains( databaseLayout.relationshipTypeScanStore().getFileName().toString() );
    }

    private long numberOfFilesIn( Path storeDir ) throws IOException
    {
        try ( Stream<Path> list = Files.list( storeDir ) )
        {
            return list.count();
        }
    }

    private static class RecordingMonitor implements TokenScanWriteMonitor.Monitor
    {
        private final List<Entry> rotations = new ArrayList<>();
        private final List<Entry> prunes = new ArrayList<>();

        @Override
        public void rotated( Path file, long timestamp, long fileSize )
        {
            rotations.add( new Entry( timestamp, fileSize ) );
        }

        @Override
        public void pruned( Path file, long timestamp )
        {
            prunes.add( new Entry( timestamp, 0 ) );
        }
    }

    private static class Entry
    {
        private final long timestamp;
        private final long fileSize;

        Entry( long timestamp, long fileSize )
        {
            this.timestamp = timestamp;
            this.fileSize = fileSize;
        }
    }
}
