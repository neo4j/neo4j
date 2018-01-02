/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.tools.txlog;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.CommandWriter;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PhysicalWritableLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.SuppressOutput;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CheckTxLogsTest
{
    @Rule
    public final SuppressOutput mute = SuppressOutput.suppressAll();
    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final File storeDirectory = new File( "db" );

    @Test
    public void shouldReportNoInconsistenciesFromValidLog() throws Exception
    {

        // Given
        File log = logFile( 1 );

        writeTxContent( log,
                new Command.NodeCommand().init(
                        new NodeRecord( 42, false, false, -1, -1, 1 ),
                        new NodeRecord( 42, true, false, 42, -1, 1 )
                ),
                new Command.PropertyCommand().init(
                        propertyRecord( 5, false, -1, -1 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.NodeCommand().init(
                        new NodeRecord( 1, true, true, 2, -1, 1 ),
                        new NodeRecord( 1, true, false, -1, -1, 1 )
                )
        );

        writeTxContent( log,
                new Command.NodeCommand().init(
                        new NodeRecord( 2, false, false, -1, -1, 1 ),
                        new NodeRecord( 2, true, false, -1, -1, 1 )
                ),
                new Command.NodeCommand().init(
                        new NodeRecord( 42, true, false, 42, -1, 1 ),
                        new NodeRecord( 42, true, false, 24, 5, 1 )
                )
        );
        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fsRule.get() );

        // When
        boolean success = checker.scan( storeDirectory, CheckType.NODE, handler );

        // Then
        assertTrue( success );

        assertEquals( 0, handler.inconsistencies.size() );
    }

    @Test
    public void shouldReportNodeInconsistenciesFromSingleLog() throws IOException
    {
        // Given
        File log = logFile( 1 );

        writeTxContent( log,
                new Command.NodeCommand().init(
                        new NodeRecord( 42, false, false, -1, -1, 1 ),
                        new NodeRecord( 42, true, false, 42, -1, 1 )
                ),
                new Command.PropertyCommand().init(
                        propertyRecord( 5, false, -1, -1 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.NodeCommand().init(
                        new NodeRecord( 1, true, true, 2, -1, 1 ),
                        new NodeRecord( 1, true, false, -1, -1, 1 )
                )
        );

        writeTxContent( log,
                new Command.NodeCommand().init(
                        new NodeRecord( 2, false, false, -1, -1, 1 ),
                        new NodeRecord( 2, true, false, -1, -1, 1 )
                ),
                new Command.NodeCommand().init(
                        new NodeRecord( 42, true, false, 24, -1, 1 ),
                        new NodeRecord( 42, true, false, 24, 5, 1 )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fsRule.get() );

        // When
        boolean success = checker.scan( storeDirectory, CheckType.NODE, handler );

        // Then
        assertFalse( success );

        assertEquals( 1, handler.inconsistencies.size() );

        NodeRecord seenRecord = (NodeRecord) handler.inconsistencies.get( 0 ).committed.record();
        NodeRecord currentRecord = (NodeRecord) handler.inconsistencies.get( 0 ).current.record();

        assertEquals( 42, seenRecord.getId() );
        assertEquals( 42, seenRecord.getNextRel() );
        assertEquals( 42, currentRecord.getId() );
        assertEquals( 24, currentRecord.getNextRel() );
    }

    @Test
    public void shouldReportNodeInconsistenciesFromDifferentLogs() throws IOException
    {
        // Given
        File log1 = logFile( 1 );
        File log2 = logFile( 2 );
        File log3 = logFile( 3 );

        writeTxContent( log1,
                new Command.NodeCommand().init(
                        new NodeRecord( 42, false, false, -1, -1, 1 ),
                        new NodeRecord( 42, true, false, 42, -1, 1 )
                ),
                new Command.PropertyCommand().init(
                        propertyRecord( 5, true, -1, -1, 777 ),
                        propertyRecord( 5, true, -1, -1, 777, 888 )
                ),
                new Command.NodeCommand().init(
                        new NodeRecord( 1, true, true, 2, -1, 1 ),
                        new NodeRecord( 1, true, false, -1, -1, 1 )
                )
        );

        writeTxContent( log2,
                new Command.NodeCommand().init(
                        new NodeRecord( 2, false, false, -1, -1, 1 ),
                        new NodeRecord( 2, true, false, -1, -1, 1 )
                )
        );

        writeTxContent( log3,
                new Command.NodeCommand().init(
                        new NodeRecord( 42, true, true, 42, -1, 1 ),
                        new NodeRecord( 42, true, true, 42, 10, 1 )
                ),
                new Command.NodeCommand().init(
                        new NodeRecord( 2, true, false, -1, -1, 5 ),
                        new NodeRecord( 2, false, false, -1, -1, 5 )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fsRule.get() );

        // When
        boolean success = checker.scan( storeDirectory, CheckType.NODE, handler );

        // Then
        assertFalse( success );

        assertEquals( 2, handler.inconsistencies.size() );

        NodeRecord seenRecord1 = (NodeRecord) handler.inconsistencies.get( 0 ).committed.record();
        NodeRecord currentRecord1 = (NodeRecord) handler.inconsistencies.get( 0 ).current.record();

        assertEquals( 42, seenRecord1.getId() );
        assertFalse( seenRecord1.isDense() );
        assertEquals( 42, currentRecord1.getId() );
        assertTrue( currentRecord1.isDense() );

        NodeRecord seenRecord2 = (NodeRecord) handler.inconsistencies.get( 1 ).committed.record();
        NodeRecord currentRecord2 = (NodeRecord) handler.inconsistencies.get( 1 ).current.record();

        assertEquals( 2, seenRecord2.getId() );
        assertEquals( 1, seenRecord2.getLabelField() );
        assertEquals( 2, currentRecord2.getId() );
        assertEquals( 5, currentRecord2.getLabelField() );
    }

    @Test
    public void shouldReportPropertyInconsistenciesFromSingleLog() throws IOException
    {
        // Given
        File log = logFile( 1 );

        writeTxContent( log,
                new Command.PropertyCommand().init(
                        propertyRecord( 42, false, -1, -1 ),
                        propertyRecord( 42, true, -1, -1, 10 )
                ),
                new Command.PropertyCommand().init(
                        propertyRecord( 42, true, -1, -1, 10 ),
                        propertyRecord( 42, true, 24, -1, 10 )
                )
        );

        writeTxContent( log,
                new Command.NodeCommand().init(
                        new NodeRecord( 2, false, false, -1, -1, 1 ),
                        new NodeRecord( 2, true, false, -1, -1, 1 )
                ),
                new Command.PropertyCommand().init(
                        propertyRecord( 42, true, -1, -1, 10 ),
                        propertyRecord( 42, true, -1, -1, 10, 20 )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fsRule.get() );

        // When
        boolean success = checker.scan( storeDirectory, CheckType.PROPERTY, handler );

        // Then
        assertFalse( success );

        assertEquals( 1, handler.inconsistencies.size() );

        PropertyRecord seenRecord = (PropertyRecord) handler.inconsistencies.get( 0 ).committed.record();
        PropertyRecord currentRecord = (PropertyRecord) handler.inconsistencies.get( 0 ).current.record();

        assertEquals( 42, seenRecord.getId() );
        assertEquals( 24, seenRecord.getPrevProp() );
        assertEquals( 42, currentRecord.getId() );
        assertEquals( -1, currentRecord.getPrevProp() );
    }

    @Test
    public void shouldReportPropertyInconsistenciesFromDifferentLogs() throws IOException
    {
        // Given
        File log1 = logFile( 1 );
        File log2 = logFile( 2 );
        File log3 = logFile( 3 );

        writeTxContent( log1,
                new Command.NodeCommand().init(
                        new NodeRecord( 42, false, false, -1, -1, 1 ),
                        new NodeRecord( 42, true, false, 42, -1, 1 )
                ),
                new Command.PropertyCommand().init(
                        propertyRecord( 5, true, -1, -1, 777 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.NodeCommand().init(
                        new NodeRecord( 1, true, true, 2, -1, 1 ),
                        new NodeRecord( 1, true, false, -1, -1, 1 )
                )
        );

        writeTxContent( log2,
                new Command.PropertyCommand().init(
                        propertyRecord( 24, false, -1, -1 ),
                        propertyRecord( 24, true, -1, -1, 777 )
                )
        );

        writeTxContent( log3,
                new Command.PropertyCommand().init(
                        propertyRecord( 24, false, -1, -1 ),
                        propertyRecord( 24, true, -1, -1, 777 )
                ),
                new Command.NodeCommand().init(
                        new NodeRecord( 42, true, true, 42, -1, 1 ),
                        new NodeRecord( 42, true, true, 42, 10, 1 )
                ),
                new Command.PropertyCommand().init(
                        propertyRecord( 5, true, -1, -1, 777, 888 ),
                        propertyRecord( 5, true, -1, 9, 777, 888, 999 )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( System.out, fsRule.get() );

        // When
        boolean success = checker.scan( storeDirectory, CheckType.PROPERTY, handler );

        // Then
        assertFalse( success );

        assertEquals( 2, handler.inconsistencies.size() );

        Inconsistency inconsistency1 = handler.inconsistencies.get( 0 );
        PropertyRecord seenRecord1 = (PropertyRecord) inconsistency1.committed.record();
        PropertyRecord currentRecord1 = (PropertyRecord) inconsistency1.current.record();

        assertEquals( 24, seenRecord1.getId() );
        assertTrue( seenRecord1.inUse() );
        assertEquals( 24, currentRecord1.getId() );
        assertFalse( currentRecord1.inUse() );
        assertEquals( 2, inconsistency1.committed.logVersion() );
        assertEquals( 3, inconsistency1.current.logVersion() );

        Inconsistency inconsistency2 = handler.inconsistencies.get( 1 );
        PropertyRecord seenRecord2 = (PropertyRecord) inconsistency2.committed.record();
        PropertyRecord currentRecord2 = (PropertyRecord) inconsistency2.current.record();

        assertEquals( 5, seenRecord2.getId() );
        assertEquals( 777, seenRecord2.getPropertyBlock( 0 ).getSingleValueInt() );
        assertEquals( 5, currentRecord2.getId() );
        assertEquals( 777, currentRecord2.getPropertyBlock( 0 ).getSingleValueInt() );
        assertEquals( 888, currentRecord2.getPropertyBlock( 1 ).getSingleValueInt() );
        assertEquals( 1, inconsistency2.committed.logVersion() );
        assertEquals( 3, inconsistency2.current.logVersion() );
    }

    private File logFile( long version )
    {
        fsRule.get().mkdirs( storeDirectory );
        return new File( storeDirectory,
                PhysicalLogFile.DEFAULT_NAME + PhysicalLogFile.DEFAULT_VERSION_SUFFIX + version );
    }

    private void writeTxContent( File log, Command... commands ) throws IOException
    {
        FileSystemAbstraction fs = fsRule.get();
        if ( !fs.fileExists( log ) )
        {
            LogHeaderWriter.writeLogHeader( fs, log, PhysicalLogFiles.getLogVersion( log ), 0 );
        }

        try ( StoreChannel channel = fs.open( log, "rw" );
              LogVersionedStoreChannel versionedChannel = new PhysicalLogVersionedStoreChannel( channel, 0, (byte) 0 );
              PhysicalWritableLogChannel writableLogChannel = new PhysicalWritableLogChannel( versionedChannel );
              CommandWriter serializer = new CommandWriter( writableLogChannel ) )
        {
            long offset = channel.size();
            channel.position( offset );

            LogEntryWriter writer = new LogEntryWriter( writableLogChannel, serializer );
            TransactionLogWriter txWriter = new TransactionLogWriter( writer );

            PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( Arrays.asList( commands ) );
            tx.setHeader( new byte[0], 0, 0, 0, 0, 0, 0 );

            txWriter.append( tx, 0 );
        }
    }

    private static PropertyRecord propertyRecord( long id, boolean inUse, long prevProp, long nextProp, long... blocks )
    {
        PropertyRecord record = new PropertyRecord( id );
        record.setInUse( inUse );
        record.setPrevProp( prevProp );
        record.setNextProp( nextProp );
        for ( int i = 0; i < blocks.length; i++ )
        {
            long blockValue = blocks[i];
            PropertyBlock block = new PropertyBlock();
            long value = PropertyStore.singleBlockLongValue( i, PropertyType.INT, blockValue );
            block.setSingleBlock( value );
            record.addPropertyBlock( block );
        }
        return record;
    }

    private static class CapturingInconsistenciesHandler implements InconsistenciesHandler
    {
        List<Inconsistency> inconsistencies = new ArrayList<>();

        @Override
        public void handle( LogRecord<?> committed, LogRecord<?> current )
        {
            inconsistencies.add( new Inconsistency( committed, current ) );
        }
    }

    private static class Inconsistency
    {
        final LogRecord<?> committed;
        final LogRecord<?> current;

        Inconsistency( LogRecord<?> committed, LogRecord<?> current )
        {
            this.committed = committed;
            this.current = current;
        }
    }
}
