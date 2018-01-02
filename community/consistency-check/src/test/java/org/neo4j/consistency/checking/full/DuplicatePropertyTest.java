/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.consistency.checking.full;

import org.junit.Test;

import org.neo4j.consistency.checking.ChainCheck;
import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccessStub;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import static org.neo4j.consistency.checking.RecordCheckTestBase.inUse;

public class DuplicatePropertyTest
{
    @Test
    public void shouldReportDuplicatePropertyIndexesInPropertyRecordForNode() throws Exception
    {
        // given
        ChainCheck check = new ChainCheck();

        RecordAccessStub records = new RecordAccessStub();

        NodeRecord master = records.add( inUse( new NodeRecord( 1, false, -1, 1 ) ) );

        PropertyRecord propertyRecord = inUse( new PropertyRecord(1) );
        PropertyBlock firstBlock = new PropertyBlock();
        firstBlock.setSingleBlock( 1 );
        firstBlock.setKeyIndexId( 1 );

        PropertyBlock secondBlock = new PropertyBlock();
        secondBlock.setSingleBlock( 1 );
        secondBlock.setKeyIndexId( 2 );

        PropertyBlock thirdBlock = new PropertyBlock();
        thirdBlock.setSingleBlock( 1 );
        thirdBlock.setKeyIndexId( 1 );

        propertyRecord.addPropertyBlock( firstBlock );
        propertyRecord.addPropertyBlock( secondBlock );
        propertyRecord.addPropertyBlock( thirdBlock );

        records.add( propertyRecord );

        // when
        ConsistencyReport.NodeConsistencyReport report = mock( ConsistencyReport.NodeConsistencyReport.class );
        CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> checkEngine = records.engine(
                master, report );
        check.checkReference( master, propertyRecord, checkEngine, records );

        // then
        verify( report ).propertyKeyNotUniqueInChain();
    }

    @Test
    public void shouldReportDuplicatePropertyIndexesAcrossRecordsInPropertyChainForNode() throws Exception
    {
        // given
        ChainCheck check = new ChainCheck();

        RecordAccessStub records = new RecordAccessStub();

        RelationshipRecord master = records.add( inUse( new RelationshipRecord( 1, 2, 3, 4 ) ) );
        master.setNextProp( 1 );

        PropertyRecord firstRecord = inUse( new PropertyRecord( 1 ) );
        firstRecord.setNextProp( 12 );

        PropertyBlock firstBlock = new PropertyBlock();
        firstBlock.setSingleBlock( 1 );
        firstBlock.setKeyIndexId( 1 );

        PropertyBlock secondBlock = new PropertyBlock();
        secondBlock.setSingleBlock( 1 );
        secondBlock.setKeyIndexId( 2 );

        PropertyRecord secondRecord = inUse( new PropertyRecord( 12 ) );
        secondRecord.setPrevProp( 1 );

        PropertyBlock thirdBlock = new PropertyBlock();
        thirdBlock.setSingleBlock( 1 );
        thirdBlock.setKeyIndexId( 4 );

        PropertyBlock fourthBlock = new PropertyBlock();
        fourthBlock.setSingleBlock( 1 );
        fourthBlock.setKeyIndexId( 1 );

        firstRecord.addPropertyBlock( firstBlock );
        firstRecord.addPropertyBlock( secondBlock );
        secondRecord.addPropertyBlock( thirdBlock );
        secondRecord.addPropertyBlock( fourthBlock );

        records.add( firstRecord );
        records.add( secondRecord );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = mock( ConsistencyReport.RelationshipConsistencyReport.class );
        CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checkEngine = records.engine(
                master, report );
        check.checkReference( master, firstRecord, checkEngine, records );
        records.checkDeferred();

        // then
        verify( report ).propertyKeyNotUniqueInChain();
    }

    @Test
    public void shouldNotReportAnythingForConsistentChains() throws Exception
    {
        // given
        ChainCheck check = new ChainCheck();

        RecordAccessStub records = new RecordAccessStub();

        RelationshipRecord master = records.add( inUse( new RelationshipRecord( 1, 2, 3, 4 ) ) );
        master.setNextProp( 1 );

        PropertyRecord firstRecord = inUse( new PropertyRecord( 1 ) );
        firstRecord.setNextProp( 12 );

        PropertyBlock firstBlock = new PropertyBlock();
        firstBlock.setSingleBlock( 1 );
        firstBlock.setKeyIndexId( 1 );

        PropertyBlock secondBlock = new PropertyBlock();
        secondBlock.setSingleBlock( 1 );
        secondBlock.setKeyIndexId( 2 );

        PropertyRecord secondRecord = inUse( new PropertyRecord( 12 ) );
        secondRecord.setPrevProp( 1 );

        PropertyBlock thirdBlock = new PropertyBlock();
        thirdBlock.setSingleBlock( 1 );
        thirdBlock.setKeyIndexId( 4 );

        PropertyBlock fourthBlock = new PropertyBlock();
        fourthBlock.setSingleBlock( 11 );
        fourthBlock.setKeyIndexId( 11 );

        firstRecord.addPropertyBlock( firstBlock );
        firstRecord.addPropertyBlock( secondBlock );
        secondRecord.addPropertyBlock( thirdBlock );
        secondRecord.addPropertyBlock( fourthBlock );

        records.add( firstRecord );
        records.add( secondRecord );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = mock( ConsistencyReport.RelationshipConsistencyReport.class );
        CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checkEngine = records.engine(
                master, report );
        check.checkReference( master, firstRecord, checkEngine, records );
        records.checkDeferred();

        // then
        verifyZeroInteractions( report );
    }
}
