/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.SuppressOutput;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.tools.txlog.checktypes.CheckTypes.NEO_STORE;
import static org.neo4j.tools.txlog.checktypes.CheckTypes.NODE;
import static org.neo4j.tools.txlog.checktypes.CheckTypes.PROPERTY;
import static org.neo4j.tools.txlog.checktypes.CheckTypes.RELATIONSHIP;
import static org.neo4j.tools.txlog.checktypes.CheckTypes.RELATIONSHIP_GROUP;

public class CheckTxLogsTest
{
    @Rule
    public final SuppressOutput mute = SuppressOutput.suppressAll();
    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    @Test
    public void shouldReportNodeInconsistenciesFromSingleLog() throws IOException
    {
        // Given
        File log = logFile( 1 );

        writeTxContent( log, 0,
                new Command.NodeCommand(
                        new NodeRecord( 42, false, false, -1, -1, 1 ),
                        new NodeRecord( 42, true, false, 42, -1, 1 )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, false, -1, -1 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.NodeCommand(
                        new NodeRecord( 1, true, true, 2, -1, 1 ),
                        new NodeRecord( 1, true, false, -1, -1, 1 )
                )
        );

        writeTxContent( log, 0,
                new Command.NodeCommand(
                        new NodeRecord( 2, false, false, -1, -1, 1 ),
                        new NodeRecord( 2, true, false, -1, -1, 1 )
                ),
                new Command.NodeCommand(
                        new NodeRecord( 42, true, false, 24, -1, 1 ),
                        new NodeRecord( 42, true, false, 24, 5, 1 )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( fsRule.get() );

        // When
        checker.scan( new File[]{log}, handler, NODE );

        // Then
        assertEquals( 1, handler.recordInconsistencies.size() );

        NodeRecord seenRecord = (NodeRecord) handler.recordInconsistencies.get( 0 ).committed.record();
        NodeRecord currentRecord = (NodeRecord) handler.recordInconsistencies.get( 0 ).current.record();

        assertEquals( 42, seenRecord.getId() );
        assertEquals( 42, seenRecord.getNextRel() );
        assertEquals( 42, currentRecord.getId() );
        assertEquals( 24, currentRecord.getNextRel() );
    }

    @Test
    public void shouldReportTransactionIdAndInconsistencyCount() throws IOException
    {
        // Given
        File log = logFile( 1 );

        writeTxContent( log, 0,
                new Command.NodeCommand(
                        new NodeRecord( 42, false, false, -1, -1, 1 ),
                        new NodeRecord( 42, true, false, 42, -1, 1 )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, false, -1, -1 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.NodeCommand(
                        new NodeRecord( 1, true, true, 2, -1, 1 ),
                        new NodeRecord( 1, true, false, -1, -1, 1 )
                ),
                new Command.NodeCommand(
                        new NodeRecord( 5, true, true, 2, -1, 1 ),
                        new NodeRecord( 5, true, false, -1, -1, 1 )
                )

        );

        writeTxContent( log, 1,
                new Command.NodeCommand(
                        new NodeRecord( 2, false, false, -1, -1, 1 ),
                        new NodeRecord( 2, true, false, -1, -1, 1 )
                ),
                new Command.NodeCommand(
                        new NodeRecord( 5, true, true, 2, -1, 1 ), // inconsistent
                        new NodeRecord( 5, true, false, -1, -1, 1 )
                ),
                new Command.NodeCommand(
                        new NodeRecord( 1, true, false, -1, -1, 1 ),
                        new NodeRecord( 1, true, true, 2, 1, 1 )
                ),
                new Command.NodeCommand(
                        new NodeRecord( 42, true, false, 24, -1, 1 ), // inconsistent
                        new NodeRecord( 42, true, false, 24, 5, 1 )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( fsRule.get() );

        // When
        checker.scan( new File[]{log}, handler, NODE );

        // Then
        assertEquals( 2, handler.recordInconsistencies.size() );

        assertEquals( 0, handler.recordInconsistencies.get( 0 ).committed.txId() );
        assertEquals( 1, handler.recordInconsistencies.get( 0 ).current.txId() );

        assertEquals( 0, handler.recordInconsistencies.get( 1 ).committed.txId() );
        assertEquals( 1, handler.recordInconsistencies.get( 1 ).current.txId() );
    }

    @Test
    public void shouldReportNodeInconsistenciesFromDifferentLogs() throws IOException
    {
        // Given
        File log1 = logFile( 1 );
        File log2 = logFile( 2 );
        File log3 = logFile( 3 );

        writeTxContent( log1, 0,
                new Command.NodeCommand(
                        new NodeRecord( 42, false, false, -1, -1, 1 ),
                        new NodeRecord( 42, true, false, 42, -1, 1 )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, true, -1, -1, 777 ),
                        propertyRecord( 5, true, -1, -1, 777, 888 )
                ),
                new Command.NodeCommand(
                        new NodeRecord( 1, true, true, 2, -1, 1 ),
                        new NodeRecord( 1, true, false, -1, -1, 1 )
                )
        );

        writeTxContent( log2, 0,
                new Command.NodeCommand(
                        new NodeRecord( 2, false, false, -1, -1, 1 ),
                        new NodeRecord( 2, true, false, -1, -1, 1 )
                )
        );

        writeTxContent( log3, 0,
                new Command.NodeCommand(
                        new NodeRecord( 42, true, true, 42, -1, 1 ),
                        new NodeRecord( 42, true, true, 42, 10, 1 )
                ),
                new Command.NodeCommand(
                        new NodeRecord( 2, true, false, -1, -1, 5 ),
                        new NodeRecord( 2, false, false, -1, -1, 5 )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( fsRule.get() );

        // When
        checker.scan( new File[]{log1, log2, log3}, handler, NODE );

        // Then
        assertEquals( 2, handler.recordInconsistencies.size() );

        NodeRecord seenRecord1 = (NodeRecord) handler.recordInconsistencies.get( 0 ).committed.record();
        NodeRecord currentRecord1 = (NodeRecord) handler.recordInconsistencies.get( 0 ).current.record();

        assertEquals( 42, seenRecord1.getId() );
        assertFalse( seenRecord1.isDense() );
        assertEquals( 42, currentRecord1.getId() );
        assertTrue( currentRecord1.isDense() );

        NodeRecord seenRecord2 = (NodeRecord) handler.recordInconsistencies.get( 1 ).committed.record();
        NodeRecord currentRecord2 = (NodeRecord) handler.recordInconsistencies.get( 1 ).current.record();

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

        writeTxContent( log, 0,
                new Command.PropertyCommand(
                        propertyRecord( 42, false, -1, -1 ),
                        propertyRecord( 42, true, -1, -1, 10 )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 42, true, -1, -1, 10 ),
                        propertyRecord( 42, true, 24, -1, 10 )
                )
        );

        writeTxContent( log, 0,
                new Command.NodeCommand(
                        new NodeRecord( 2, false, false, -1, -1, 1 ),
                        new NodeRecord( 2, true, false, -1, -1, 1 )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 42, true, -1, -1, 10 ),
                        propertyRecord( 42, true, -1, -1, 10, 20 )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( fsRule.get() );

        // When
        checker.scan( new File[]{log}, handler, PROPERTY );

        // Then
        assertEquals( 1, handler.recordInconsistencies.size() );

        PropertyRecord seenRecord = (PropertyRecord) handler.recordInconsistencies.get( 0 ).committed.record();
        PropertyRecord currentRecord = (PropertyRecord) handler.recordInconsistencies.get( 0 ).current.record();

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

        writeTxContent( log1, 0,
                new Command.NodeCommand(
                        new NodeRecord( 42, false, false, -1, -1, 1 ),
                        new NodeRecord( 42, true, false, 42, -1, 1 )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, true, -1, -1, 777 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.NodeCommand(
                        new NodeRecord( 1, true, true, 2, -1, 1 ),
                        new NodeRecord( 1, true, false, -1, -1, 1 )
                )
        );

        writeTxContent( log2, 0,
                new Command.PropertyCommand(
                        propertyRecord( 24, false, -1, -1 ),
                        propertyRecord( 24, true, -1, -1, 777 )
                )
        );

        writeTxContent( log3, 0,
                new Command.PropertyCommand(
                        propertyRecord( 24, false, -1, -1 ),
                        propertyRecord( 24, true, -1, -1, 777 )
                ),
                new Command.NodeCommand(
                        new NodeRecord( 42, true, true, 42, -1, 1 ),
                        new NodeRecord( 42, true, true, 42, 10, 1 )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, true, -1, -1, 777, 888 ),
                        propertyRecord( 5, true, -1, 9, 777, 888, 999 )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( fsRule.get() );

        // When
        checker.scan( new File[]{log1, log2, log3}, handler, PROPERTY );

        // Then
        assertEquals( 2, handler.recordInconsistencies.size() );

        PropertyRecord seenRecord1 = (PropertyRecord) handler.recordInconsistencies.get( 0 ).committed.record();
        PropertyRecord currentRecord1 = (PropertyRecord) handler.recordInconsistencies.get( 0 ).current.record();

        assertEquals( 24, seenRecord1.getId() );
        assertTrue( seenRecord1.inUse() );
        assertEquals( 24, currentRecord1.getId() );
        assertFalse( currentRecord1.inUse() );

        PropertyRecord seenRecord2 = (PropertyRecord) handler.recordInconsistencies.get( 1 ).committed.record();
        PropertyRecord currentRecord2 = (PropertyRecord) handler.recordInconsistencies.get( 1 ).current.record();

        assertEquals( 5, seenRecord2.getId() );
        assertEquals( 777, seenRecord2.getPropertyBlock( 0 ).getSingleValueInt() );
        assertEquals( 5, currentRecord2.getId() );
        assertEquals( 777, currentRecord2.getPropertyBlock( 0 ).getSingleValueInt() );
        assertEquals( 888, currentRecord2.getPropertyBlock( 1 ).getSingleValueInt() );
    }

    @Test
    public void shouldReportRelationshipInconsistenciesFromSingleLog() throws IOException
    {
        // Given
        File log = logFile( 1 );

        writeTxContent( log, 0,
                new Command.RelationshipCommand(
                        new RelationshipRecord( 42, false, -1, -1, -1, -1, -1, -1, -1, false, false ),
                        new RelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, true, true )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, false, -1, -1 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.RelationshipCommand(
                        new RelationshipRecord( 21, true, 1, 2, 3, 4, 5, 6, 7, true, true ),
                        new RelationshipRecord( 21, false, -1, -1, -1, -1, -1, -1, -1, false, false )
                )
        );

        writeTxContent( log, 0,
                new Command.RelationshipCommand(
                        new RelationshipRecord( 53, true, 1, 2, 3, 4, 5, 6, 7, true, true ),
                        new RelationshipRecord( 53, true, 1, 2, 30, 4, 14, 6, 7, true, true )
                ),
                new Command.RelationshipCommand(
                        new RelationshipRecord( 42, true, 1, 2, 3, 9, 5, 6, 7, true, true ),
                        new RelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, true, true )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( fsRule.get() );

        // When
        checker.scan( new File[]{log}, handler, RELATIONSHIP );

        // Then
        assertEquals( 1, handler.recordInconsistencies.size() );

        RelationshipRecord seenRecord = (RelationshipRecord) handler.recordInconsistencies.get( 0 ).committed.record();
        RelationshipRecord currentRecord = (RelationshipRecord) handler.recordInconsistencies.get( 0 ).current.record();

        assertEquals( 42, seenRecord.getId() );
        assertEquals( 4, seenRecord.getFirstPrevRel() );
        assertEquals( 42, currentRecord.getId() );
        assertEquals( 9, currentRecord.getFirstPrevRel() );
    }

    @Test
    public void shouldReportRelationshipInconsistenciesFromDifferentLogs() throws IOException
    {
        // Given
        File log1 = logFile( 1 );
        File log2 = logFile( 2 );
        File log3 = logFile( 3 );

        writeTxContent( log1, 0,
                new Command.RelationshipCommand(
                        new RelationshipRecord( 42, false, -1, -1, -1, -1, -1, -1, -1, false, false ),
                        new RelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, true, true )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, false, -1, -1 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.RelationshipCommand(
                        new RelationshipRecord( 21, true, 1, 2, 3, 4, 5, 6, 7, true, true ),
                        new RelationshipRecord( 21, false, -1, -1, -1, -1, -1, -1, -1, false, false )
                )
        );

        writeTxContent( log2, 0,
                new Command.RelationshipCommand(
                        new RelationshipRecord( 42, true, 1, 2, 3, 9, 5, 6, 7, true, true ),
                        new RelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, true, true )
                )
        );

        writeTxContent( log3, 0,
                new Command.RelationshipCommand(
                        new RelationshipRecord( 53, true, 1, 2, 3, 4, 5, 6, 7, true, true ),
                        new RelationshipRecord( 53, true, 1, 2, 30, 4, 14, 6, 7, true, true )
                ),
                new Command.RelationshipCommand(
                        new RelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, false, true ),
                        new RelationshipRecord( 42, true, 1, 2, 3, 4, 5, 6, 7, false, true )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( fsRule.get() );

        // When
        checker.scan( new File[]{log1, log2, log3}, handler, RELATIONSHIP );

        // Then
        assertEquals( 2, handler.recordInconsistencies.size() );

        RelationshipRecord seenRecord1 = (RelationshipRecord) handler.recordInconsistencies.get( 0 ).committed.record();
        RelationshipRecord currentRecord1 = (RelationshipRecord) handler.recordInconsistencies.get( 0 ).current.record();

        assertEquals( 42, seenRecord1.getId() );
        assertEquals( 4, seenRecord1.getFirstPrevRel() );
        assertEquals( 42, currentRecord1.getId() );
        assertEquals( 9, currentRecord1.getFirstPrevRel() );

        RelationshipRecord seenRecord2 = (RelationshipRecord) handler.recordInconsistencies.get( 1 ).committed.record();
        RelationshipRecord currentRecord2 = (RelationshipRecord) handler.recordInconsistencies.get( 1 ).current.record();

        assertEquals( 42, seenRecord2.getId() );
        assertTrue( seenRecord2.isFirstInFirstChain() );
        assertEquals( 42, currentRecord2.getId() );
        assertFalse(currentRecord2.isFirstInFirstChain() );
    }

    @Test
    public void shouldReportRelationshipGroupInconsistenciesFromSingleLog() throws IOException
    {
        // Given
        File log = logFile( 1 );

        writeTxContent( log, 0,
                new Command.RelationshipGroupCommand(
                        new RelationshipGroupRecord( 42, -1, -1, -1, -1, -1, -1, false ),
                        new RelationshipGroupRecord( 42, 1, 2, 3, 4, 5, 6, true )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, false, -1, -1 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.RelationshipGroupCommand(
                        new RelationshipGroupRecord( 21, 1, 2, 3, 4, 5, 7, true ),
                        new RelationshipGroupRecord( 21, -1, -1, -1, -1, -1, -1, false )
                )
        );

        writeTxContent( log, 0,
                new Command.RelationshipGroupCommand(
                        new RelationshipGroupRecord( 53, 1, 2, 3, 4, 5, 6, true ),
                        new RelationshipGroupRecord( 53, 1, 2, 30, 4, 14, 6, true )
                ),
                new Command.RelationshipGroupCommand(
                        new RelationshipGroupRecord( 42, 1, 2, 3, 9, 5, 6, true ),
                        new RelationshipGroupRecord( 42, 1, 2, 3, 4, 5, 6, true )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( fsRule.get() );

        // When
        checker.scan( new File[]{log}, handler, RELATIONSHIP_GROUP );

        // Then
        assertEquals( 1, handler.recordInconsistencies.size() );

        RelationshipGroupRecord seenRecord = (RelationshipGroupRecord) handler.recordInconsistencies.get( 0 ).committed.record();
        RelationshipGroupRecord currentRecord = (RelationshipGroupRecord) handler.recordInconsistencies.get( 0 ).current.record();

        assertEquals( 42, seenRecord.getId() );
        assertEquals( 4, seenRecord.getFirstLoop() );
        assertEquals( 42, currentRecord.getId() );
        assertEquals( 9, currentRecord.getFirstLoop() );
    }

    @Test
    public void shouldReportRelationshipGroupInconsistenciesFromDifferentLogs() throws IOException
    {
        // Given
        File log1 = logFile( 1 );
        File log2 = logFile( 2 );
        File log3 = logFile( 3 );

        writeTxContent( log1, 0,
                new Command.RelationshipGroupCommand(
                        new RelationshipGroupRecord( 42, -1, -1, -1, -1, -1, -1, false ),
                        new RelationshipGroupRecord( 42, 1, 2, 3, 4, 5, 6, true )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, false, -1, -1 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.RelationshipGroupCommand(
                        new RelationshipGroupRecord( 21, 1, 2, 3, 4, 5, 6, true ),
                        new RelationshipGroupRecord( 21, -1, -1, -1, -1, -1, -1, false )
                )
        );

        writeTxContent( log2, 0,
                new Command.RelationshipGroupCommand(
                        new RelationshipGroupRecord( 42, 1, 2, 3, 9, 5, 6, true ),
                        new RelationshipGroupRecord( 42, 1, 2, 3, 4, 5, 6, true )
                )
        );

        writeTxContent( log3, 0,
                new Command.RelationshipGroupCommand(
                        new RelationshipGroupRecord( 53, 1, 2, 3, 4, 5, 6, true ),
                        new RelationshipGroupRecord( 53, 1, 2, 30, 4, 14, 6, true )
                ),
                new Command.RelationshipGroupCommand(
                        new RelationshipGroupRecord( 42, 1, 2, 3, 4, 5, 6, false ),
                        new RelationshipGroupRecord( 42, 1, 2, 3, 4, 5, 6, false )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( fsRule.get() );

        // When
        checker.scan( new File[]{log1, log2, log3}, handler, RELATIONSHIP_GROUP );

        // Then
        assertEquals( 2, handler.recordInconsistencies.size() );

        RelationshipGroupRecord seenRecord1 = (RelationshipGroupRecord) handler.recordInconsistencies.get( 0 ).committed.record();
        RelationshipGroupRecord currentRecord1 = (RelationshipGroupRecord) handler.recordInconsistencies.get( 0 ).current.record();

        assertEquals( 42, seenRecord1.getId() );
        assertEquals( 4, seenRecord1.getFirstLoop() );
        assertEquals( 42, currentRecord1.getId() );
        assertEquals( 9, currentRecord1.getFirstLoop() );

        RelationshipGroupRecord seenRecord2 = (RelationshipGroupRecord) handler.recordInconsistencies.get( 1 ).committed.record();
        RelationshipGroupRecord currentRecord2 = (RelationshipGroupRecord) handler.recordInconsistencies.get( 1 ).current.record();

        assertEquals( 42, seenRecord2.getId() );
        assertTrue( seenRecord2.inUse() );
        assertEquals( 42, currentRecord2.getId() );
        assertFalse(currentRecord2.inUse() );
    }

    @Test
    public void shouldReportNeoStoreInconsistenciesFromSingleLog() throws IOException
    {
        // Given
        File log = logFile( 1 );

        writeTxContent( log, 0,
                new Command.NeoStoreCommand(
                        new NeoStoreRecord(),
                        createNeoStoreRecord( 42 )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, false, -1, -1 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.NeoStoreCommand(
                        createNeoStoreRecord( 42 ),
                        createNeoStoreRecord( 21 )
                )
        );

        writeTxContent( log, 0,
                new Command.NeoStoreCommand(
                        createNeoStoreRecord( 42 ),
                        createNeoStoreRecord( 33 )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( fsRule.get() );

        // When
        checker.scan( new File[]{log}, handler, NEO_STORE );

        // Then
        assertEquals( 1, handler.recordInconsistencies.size() );

        NeoStoreRecord seenRecord = (NeoStoreRecord) handler.recordInconsistencies.get( 0 ).committed.record();
        NeoStoreRecord currentRecord = (NeoStoreRecord) handler.recordInconsistencies.get( 0 ).current.record();

        assertEquals( 21, seenRecord.getNextProp() );
        assertEquals( 42, currentRecord.getNextProp() );
    }

    @Test
    public void shouldReportNeoStoreInconsistenciesFromDifferentLogs() throws IOException
    {
        // Given
        File log1 = logFile( 1 );
        File log2 = logFile( 2 );
        File log3 = logFile( 3 );

        writeTxContent( log1, 0,
                new Command.NeoStoreCommand(
                        new NeoStoreRecord(),
                        createNeoStoreRecord( 42 )
                ),
                new Command.PropertyCommand(
                        propertyRecord( 5, false, -1, -1 ),
                        propertyRecord( 5, true, -1, -1, 777 )
                ),
                new Command.NeoStoreCommand(
                        createNeoStoreRecord( 42 ),
                        createNeoStoreRecord( 21 )
                )
        );

        writeTxContent( log2, 0,
                new Command.NeoStoreCommand(
                        createNeoStoreRecord( 12 ),
                        createNeoStoreRecord( 21 )
                )
        );

        writeTxContent( log3, 0,
                new Command.NeoStoreCommand(
                        createNeoStoreRecord( 13 ),
                        createNeoStoreRecord( 21 )
                )
        );

        CapturingInconsistenciesHandler handler = new CapturingInconsistenciesHandler();
        CheckTxLogs checker = new CheckTxLogs( fsRule.get() );

        // When
        checker.scan( new File[]{log1, log2, log3}, handler, NEO_STORE );

        // Then
        assertEquals( 2, handler.recordInconsistencies.size() );

        NeoStoreRecord seenRecord1 = (NeoStoreRecord) handler.recordInconsistencies.get( 0 ).committed.record();
        NeoStoreRecord currentRecord1 = (NeoStoreRecord) handler.recordInconsistencies.get( 0 ).current.record();

        assertEquals( 21, seenRecord1.getNextProp() );
        assertEquals( 12, currentRecord1.getNextProp() );

        NeoStoreRecord seenRecord2 = (NeoStoreRecord) handler.recordInconsistencies.get( 1 ).committed.record();
        NeoStoreRecord currentRecord2 = (NeoStoreRecord) handler.recordInconsistencies.get( 1 ).current.record();

        assertEquals( 21, seenRecord2.getNextProp() );
        assertEquals( 13, currentRecord2.getNextProp() );
    }

    private NeoStoreRecord createNeoStoreRecord( int nextProp )
    {
        NeoStoreRecord neoStoreRecord = new NeoStoreRecord();
        neoStoreRecord.setNextProp( nextProp );
        return neoStoreRecord;
    }

    private static File logFile( long version )
    {
        return new File( PhysicalLogFile.DEFAULT_NAME + PhysicalLogFile.DEFAULT_VERSION_SUFFIX + version );
    }

    private void writeTxContent( File log, long txId, Command... commands ) throws IOException
    {
        FileSystemAbstraction fs = fsRule.get();
        if ( !fs.fileExists( log ) )
        {
            LogHeaderWriter.writeLogHeader( fs, log, PhysicalLogFiles.getLogVersion( log ), 0 );
        }

        try ( StoreChannel channel = fs.open( log, "rw" );
              LogVersionedStoreChannel versionedChannel = new PhysicalLogVersionedStoreChannel( channel, 0, (byte) 0 );
              PhysicalFlushableChannel writableLogChannel = new PhysicalFlushableChannel( versionedChannel ) )
        {
            long offset = channel.size();
            channel.position( offset );

            LogEntryWriter writer = new LogEntryWriter( writableLogChannel );
            TransactionLogWriter txWriter = new TransactionLogWriter( writer );

            PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( Arrays.asList( commands ) );
            tx.setHeader( new byte[0], 0, 0, 0, 0, 0, 0 );

            txWriter.append( tx, txId );
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
        List<RecordInconsistency> recordInconsistencies = new ArrayList<>();

        @Override
        public void reportInconsistentCommand( RecordInfo<?> committed, RecordInfo<?> current )
        {
            recordInconsistencies.add( new RecordInconsistency( committed, current ) );
        }
    }

    private static class RecordInconsistency
    {
        final RecordInfo<?> committed;
        final RecordInfo<?> current;

        RecordInconsistency( RecordInfo<?> committed, RecordInfo<?> current )
        {
            this.committed = committed;
            this.current = current;
        }
    }
}
