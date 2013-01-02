/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.consistency.checking.full;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.consistency.checking.DynamicRecordCheckTest.configureDynamicStore;
import static org.neo4j.consistency.checking.RecordCheckTestBase.NONE;
import static org.neo4j.consistency.checking.RecordCheckTestBase.array;
import static org.neo4j.consistency.checking.RecordCheckTestBase.check;
import static org.neo4j.consistency.checking.RecordCheckTestBase.dummyDynamicCheck;
import static org.neo4j.consistency.checking.RecordCheckTestBase.dummyNeoStoreCheck;
import static org.neo4j.consistency.checking.RecordCheckTestBase.dummyNodeCheck;
import static org.neo4j.consistency.checking.RecordCheckTestBase.dummyPropertyChecker;
import static org.neo4j.consistency.checking.RecordCheckTestBase.dummyPropertyKeyCheck;
import static org.neo4j.consistency.checking.RecordCheckTestBase.dummyRelationshipChecker;
import static org.neo4j.consistency.checking.RecordCheckTestBase.dummyRelationshipLabelCheck;
import static org.neo4j.consistency.checking.RecordCheckTestBase.inUse;
import static org.neo4j.consistency.checking.RecordCheckTestBase.notInUse;
import static org.neo4j.consistency.checking.RecordCheckTestBase.propertyBlock;
import static org.neo4j.consistency.checking.RecordCheckTestBase.string;
import static org.neo4j.consistency.checking.RecordCheckTestBase.verifyOnlyReferenceDispatch;

import org.junit.Test;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.DynamicStore;
import org.neo4j.consistency.checking.PrimitiveRecordCheck;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccessStub;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

public class OwnerCheckTest
{
    @Test
    public void shouldNotDecorateCheckerWhenInactive() throws Exception
    {
        // given
        OwnerCheck decorator = new OwnerCheck( false );
        PrimitiveRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker = dummyNodeCheck();

        // when
        RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> decorated =
                decorator.decorateNodeChecker( checker );

        // then
        assertSame( checker, decorated );
    }

    @Test
    public void shouldNotReportAnythingForNodesWithDifferentPropertyChains() throws Exception
    {
        // given
        OwnerCheck decorator = new OwnerCheck( true );
        RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker =
                decorator.decorateNodeChecker( dummyNodeCheck() );

        RecordAccessStub records = new RecordAccessStub();

        NodeRecord node1 = records.add( inUse( new NodeRecord( 1, NONE, 7 ) ) );
        NodeRecord node2 = records.add( inUse( new NodeRecord( 2, NONE, 8 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report1 =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node1, records );
        ConsistencyReport.NodeConsistencyReport report2 =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node2, records );

        // then
        verifyZeroInteractions( report1 );
        verifyZeroInteractions( report2 );
    }

    @Test
    public void shouldNotReportAnythingForNodesNotInUse() throws Exception
    {
        // given
        OwnerCheck decorator = new OwnerCheck( true );
        RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker =
                decorator.decorateNodeChecker( dummyNodeCheck() );

        RecordAccessStub records = new RecordAccessStub();

        NodeRecord node1 = records.add( notInUse( new NodeRecord( 1, NONE, 6 ) ) );
        NodeRecord node2 = records.add( notInUse( new NodeRecord( 2, NONE, 6 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report1 =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node1, records );
        ConsistencyReport.NodeConsistencyReport report2 =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node2, records );

        // then
        verifyZeroInteractions( report1 );
        verifyZeroInteractions( report2 );
    }

    @Test
    public void shouldNotReportAnythingForRelationshipsWithDifferentPropertyChains() throws Exception
    {
        // given
        OwnerCheck decorator = new OwnerCheck( true );
        RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> relationshipChecker =
                decorator.decorateRelationshipChecker( dummyRelationshipChecker() );

        RecordAccessStub records = new RecordAccessStub();

        RelationshipRecord relationship1 = records.add( inUse( new RelationshipRecord( 1, 0, 1, 0 ) ) );
        relationship1.setNextProp( 7 );
        RelationshipRecord relationship2 = records.add( inUse( new RelationshipRecord( 2, 0, 1, 0 ) ) );
        relationship2.setNextProp( 8 );

        // when
        ConsistencyReport.RelationshipConsistencyReport report1 =
                check( ConsistencyReport.RelationshipConsistencyReport.class,
                       relationshipChecker, relationship1, records );
        ConsistencyReport.RelationshipConsistencyReport report2 =
                check( ConsistencyReport.RelationshipConsistencyReport.class,
                       relationshipChecker, relationship2, records );

        // then
        verifyZeroInteractions( report1 );
        verifyZeroInteractions( report2 );
    }

    @Test
    public void shouldReportTwoNodesWithSamePropertyChain() throws Exception
    {
        // given
        OwnerCheck decorator = new OwnerCheck( true );
        RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker =
                decorator.decorateNodeChecker( dummyNodeCheck() );

        RecordAccessStub records = new RecordAccessStub();

        NodeRecord node1 = records.add( inUse( new NodeRecord( 1, NONE, 7 ) ) );
        NodeRecord node2 = records.add( inUse( new NodeRecord( 2, NONE, 7 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report1 =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node1, records );
        ConsistencyReport.NodeConsistencyReport report2 =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node2, records );

        // then
        verifyZeroInteractions( report1 );
        verify( report2 ).multipleOwners( node1 );
    }

    @Test
    public void shouldReportTwoRelationshipsWithSamePropertyChain() throws Exception
    {
        // given
        OwnerCheck decorator = new OwnerCheck( true );
        RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> relationshipChecker =
                decorator.decorateRelationshipChecker( dummyRelationshipChecker() );

        RecordAccessStub records = new RecordAccessStub();

        RelationshipRecord relationship1 = records.add( inUse( new RelationshipRecord( 1, 0, 1, 0 ) ) );
        relationship1.setNextProp( 7 );
        RelationshipRecord relationship2 = records.add( inUse( new RelationshipRecord( 2, 0, 1, 0 ) ) );
        relationship2.setNextProp( relationship1.getNextProp() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report1 =
                check( ConsistencyReport.RelationshipConsistencyReport.class,
                       relationshipChecker, relationship1, records );
        ConsistencyReport.RelationshipConsistencyReport report2 =
                check( ConsistencyReport.RelationshipConsistencyReport.class,
                       relationshipChecker, relationship2, records );

        // then
        verifyZeroInteractions( report1 );
        verify( report2 ).multipleOwners( relationship1 );
    }

    @Test
    public void shouldReportRelationshipWithSamePropertyChainAsNode() throws Exception
    {
        // given
        OwnerCheck decorator = new OwnerCheck( true );
        RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker =
                decorator.decorateNodeChecker( dummyNodeCheck() );
        RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> relationshipChecker =
                decorator.decorateRelationshipChecker( dummyRelationshipChecker() );

        RecordAccessStub records = new RecordAccessStub();

        NodeRecord node = records.add( inUse( new NodeRecord( 1, NONE, 7 ) ) );
        RelationshipRecord relationship = records.add( inUse( new RelationshipRecord( 1, 0, 1, 0 ) ) );
        relationship.setNextProp( node.getNextProp() );

        // when
        ConsistencyReport.NodeConsistencyReport nodeReport =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node, records );
        ConsistencyReport.RelationshipConsistencyReport relationshipReport =
                check( ConsistencyReport.RelationshipConsistencyReport.class,
                       relationshipChecker, relationship, records );

        // then
        verifyZeroInteractions( nodeReport );
        verify( relationshipReport ).multipleOwners( node );
    }

    @Test
    public void shouldReportRelationshipWithReferenceToTheGraphGlobalChain() throws Exception
    {
        // given
        OwnerCheck decorator = new OwnerCheck( true );
        RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> relationshipChecker =
                decorator.decorateRelationshipChecker( dummyRelationshipChecker() );
        RecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> neoStoreCheck =
                decorator.decorateNeoStoreChecker( dummyNeoStoreCheck() );

        RecordAccessStub records = new RecordAccessStub();

        NeoStoreRecord master = records.add( new NeoStoreRecord() );
        master.setNextProp( 7 );
        RelationshipRecord relationship = records.add( inUse( new RelationshipRecord( 1, 0, 1, 0 ) ) );
        relationship.setNextProp( 7 );

        // when
        ConsistencyReport.NeoStoreConsistencyReport masterReport =
                check( ConsistencyReport.NeoStoreConsistencyReport.class, neoStoreCheck, master, records );
        ConsistencyReport.RelationshipConsistencyReport relationshipReport =
                check( ConsistencyReport.RelationshipConsistencyReport.class,
                       relationshipChecker, relationship, records );

        // then
        verifyZeroInteractions( masterReport );
        verify( relationshipReport ).multipleOwners( master );
    }

    @Test
    public void shouldReportNodeWithSamePropertyChainAsRelationship() throws Exception
    {
        // given
        OwnerCheck decorator = new OwnerCheck( true );
        RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker =
                decorator.decorateNodeChecker( dummyNodeCheck() );
        RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> relationshipChecker =
                decorator.decorateRelationshipChecker( dummyRelationshipChecker() );

        RecordAccessStub records = new RecordAccessStub();

        NodeRecord node = records.add( inUse( new NodeRecord( 1, NONE, 7 ) ) );
        RelationshipRecord relationship = records.add( inUse( new RelationshipRecord( 1, 0, 1, 0 ) ) );
        relationship.setNextProp( node.getNextProp() );

        // when
        ConsistencyReport.RelationshipConsistencyReport relationshipReport =
                check( ConsistencyReport.RelationshipConsistencyReport.class,
                       relationshipChecker, relationship, records );
        ConsistencyReport.NodeConsistencyReport nodeReport =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node, records );

        // then
        verifyZeroInteractions( relationshipReport );
        verify( nodeReport ).multipleOwners( relationship );
    }

    @Test
    public void shouldReportNodeWithReferenceToTheGraphGlobalChain() throws Exception
    {
        // given
        OwnerCheck decorator = new OwnerCheck( true );
        RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker =
                decorator.decorateNodeChecker( dummyNodeCheck() );
        RecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> neoStoreCheck =
                decorator.decorateNeoStoreChecker( dummyNeoStoreCheck() );

        RecordAccessStub records = new RecordAccessStub();

        NodeRecord node = records.add( inUse( new NodeRecord( 1, NONE, 7 ) ) );
        NeoStoreRecord master = records.add( new NeoStoreRecord() );
        master.setNextProp( node.getNextProp() );

        // when
        ConsistencyReport.NeoStoreConsistencyReport masterReport =
                check( ConsistencyReport.NeoStoreConsistencyReport.class, neoStoreCheck, master, records );
        ConsistencyReport.NodeConsistencyReport nodeReport =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node, records );

        // then
        verifyZeroInteractions( masterReport );
        verify( nodeReport ).multipleOwners( master );
    }

    @Test
    public void shouldReportNodeStoreReferencingSameChainAsNode() throws Exception
    {
        // given
        OwnerCheck decorator = new OwnerCheck( true );
        RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker =
                decorator.decorateNodeChecker( dummyNodeCheck() );
        RecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> neoStoreCheck =
                decorator.decorateNeoStoreChecker( dummyNeoStoreCheck() );

        RecordAccessStub records = new RecordAccessStub();

        NodeRecord node = records.add( inUse( new NodeRecord( 1, NONE, 7 ) ) );
        NeoStoreRecord master = records.add( new NeoStoreRecord() );
        master.setNextProp( node.getNextProp() );

        // when
        ConsistencyReport.NodeConsistencyReport nodeReport =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node, records );
        ConsistencyReport.NeoStoreConsistencyReport masterReport =
                check( ConsistencyReport.NeoStoreConsistencyReport.class, neoStoreCheck, master, records );

        // then
        verifyZeroInteractions( nodeReport );
        verify( masterReport ).multipleOwners( node );
    }

    @Test
    public void shouldReportNodeStoreReferencingSameChainAsRelationship() throws Exception
    {
        // given
        OwnerCheck decorator = new OwnerCheck( true );
        RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> relationshipChecker =
                decorator.decorateRelationshipChecker( dummyRelationshipChecker() );
        RecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> neoStoreCheck =
                decorator.decorateNeoStoreChecker( dummyNeoStoreCheck() );

        RecordAccessStub records = new RecordAccessStub();

        NeoStoreRecord master = records.add( new NeoStoreRecord() );
        master.setNextProp( 7 );
        RelationshipRecord relationship = records.add( inUse( new RelationshipRecord( 1, 0, 1, 0 ) ) );
        relationship.setNextProp( 7 );

        // when
        ConsistencyReport.RelationshipConsistencyReport relationshipReport =
                check( ConsistencyReport.RelationshipConsistencyReport.class,
                       relationshipChecker, relationship, records );
        ConsistencyReport.NeoStoreConsistencyReport masterReport =
                check( ConsistencyReport.NeoStoreConsistencyReport.class, neoStoreCheck, master, records );

        // then
        verifyZeroInteractions( relationshipReport );
        verify( masterReport ).multipleOwners( relationship );
    }

    @Test
    public void shouldReportOrphanPropertyChain() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck decorator = new OwnerCheck( true );
        RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker = decorator
                .decoratePropertyChecker( dummyPropertyChecker() );

        PropertyRecord record = inUse( new PropertyRecord( 42 ) );
        ConsistencyReport.PropertyConsistencyReport report = check(
                ConsistencyReport.PropertyConsistencyReport.class, checker, record, records );

        // when
        decorator.scanForOrphanChains( ProgressMonitorFactory.NONE );

        records.checkDeferred();

        // then
        verify( report ).orphanPropertyChain();
    }

    @Test
    public void shouldNotReportOrphanIfOwnedByNode() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck decorator = new OwnerCheck( true );

        PropertyRecord record = inUse( new PropertyRecord( 42 ) );
        ConsistencyReport.PropertyConsistencyReport report =
                check( ConsistencyReport.PropertyConsistencyReport.class,
                       decorator.decoratePropertyChecker( dummyPropertyChecker() ),
                       record, records );
        ConsistencyReport.NodeConsistencyReport nodeReport =
                check( ConsistencyReport.NodeConsistencyReport.class,
                       decorator.decorateNodeChecker( dummyNodeCheck() ),
                       inUse( new NodeRecord( 10, NONE, 42 ) ), records );

        // when
        decorator.scanForOrphanChains( ProgressMonitorFactory.NONE );

        records.checkDeferred();

        // then
        verifyOnlyReferenceDispatch( report );
        verifyOnlyReferenceDispatch( nodeReport );
    }

    @Test
    public void shouldNotReportOrphanIfOwnedByRelationship() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck decorator = new OwnerCheck( true );

        PropertyRecord record = inUse( new PropertyRecord( 42 ) );
        ConsistencyReport.PropertyConsistencyReport report =
                check( ConsistencyReport.PropertyConsistencyReport.class,
                       decorator.decoratePropertyChecker( dummyPropertyChecker() ),
                       record, records );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 10, 1, 1, 0 ) );
        relationship.setNextProp( 42 );
        ConsistencyReport.RelationshipConsistencyReport relationshipReport =
                check( ConsistencyReport.RelationshipConsistencyReport.class,
                       decorator.decorateRelationshipChecker( dummyRelationshipChecker() ),
                       relationship, records );

        // when
        decorator.scanForOrphanChains( ProgressMonitorFactory.NONE );

        records.checkDeferred();

        // then
        verifyOnlyReferenceDispatch( report );
        verifyOnlyReferenceDispatch( relationshipReport );
    }

    @Test
    public void shouldNotReportOrphanIfOwnedByNeoStore() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck decorator = new OwnerCheck( true );

        PropertyRecord record = inUse( new PropertyRecord( 42 ) );
        ConsistencyReport.PropertyConsistencyReport report =
                check( ConsistencyReport.PropertyConsistencyReport.class,
                       decorator.decoratePropertyChecker( dummyPropertyChecker() ),
                       record, records );
        NeoStoreRecord master = inUse( new NeoStoreRecord() );
        master.setNextProp( 42 );
        ConsistencyReport.NeoStoreConsistencyReport masterReport =
                check( ConsistencyReport.NeoStoreConsistencyReport.class,
                       decorator.decorateNeoStoreChecker( dummyNeoStoreCheck() ),
                       master, records );

        // when
        decorator.scanForOrphanChains( ProgressMonitorFactory.NONE );

        records.checkDeferred();

        // then
        verifyOnlyReferenceDispatch( report );
        verifyOnlyReferenceDispatch( masterReport );
    }

    @Test
    public void shouldReportDynamicRecordOwnedByTwoOtherDynamicRecords() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck decorator = new OwnerCheck( true, DynamicStore.STRING );

        RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker = decorator
                .decorateDynamicChecker( RecordType.STRING_PROPERTY,
                                         dummyDynamicCheck( configureDynamicStore( 50 ), DynamicStore.STRING ) );

        DynamicRecord record1 = records.add( inUse( string( new DynamicRecord( 1 ) ) ) );
        DynamicRecord record2 = records.add( inUse( string( new DynamicRecord( 2 ) ) ) );
        DynamicRecord record3 = records.add( inUse( string( new DynamicRecord( 3 ) ) ) );
        record1.setNextBlock( record3.getId() );
        record2.setNextBlock( record3.getId() );

        // when
        ConsistencyReport.DynamicConsistencyReport report1 = check( ConsistencyReport.DynamicConsistencyReport.class,
                                                                    checker, record1, records );
        ConsistencyReport.DynamicConsistencyReport report2 = check( ConsistencyReport.DynamicConsistencyReport.class,
                                                                    checker, record2, records );

        // then
        verifyOnlyReferenceDispatch( report1 );
        verify( report2 ).nextMultipleOwners( record1 );
        verifyOnlyReferenceDispatch( report2 );
    }

    @Test
    public void shouldReportDynamicStringRecordOwnedByTwoPropertyRecords() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck decorator = new OwnerCheck( true, DynamicStore.STRING );

        RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> propChecker = decorator
                .decoratePropertyChecker( dummyPropertyChecker() );

        DynamicRecord dynamic = records.add( inUse( string( new DynamicRecord( 42 ) ) ) );
        PropertyRecord property1 = records.add( inUse( new PropertyRecord( 1 ) ) );
        PropertyRecord property2 = records.add( inUse( new PropertyRecord( 2 ) ) );
        PropertyIndexRecord key = records.add( inUse( new PropertyIndexRecord( 10 ) ) );
        property1.addPropertyBlock( propertyBlock( key, PropertyType.STRING, dynamic.getId() ) );
        property2.addPropertyBlock( propertyBlock( key, PropertyType.STRING, dynamic.getId() ) );

        // when
        ConsistencyReport.PropertyConsistencyReport report1 = check( ConsistencyReport.PropertyConsistencyReport.class,
                                                                     propChecker, property1, records );
        ConsistencyReport.PropertyConsistencyReport report2 = check( ConsistencyReport.PropertyConsistencyReport.class,
                                                                     propChecker, property2, records );

        // then
        verifyOnlyReferenceDispatch( report1 );
        verify( report2 ).stringMultipleOwners( property1 );
        verifyOnlyReferenceDispatch( report2 );
    }

    @Test
    public void shouldReportDynamicArrayRecordOwnedByTwoPropertyRecords() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck decorator = new OwnerCheck( true, DynamicStore.ARRAY );

        RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> propChecker = decorator
                .decoratePropertyChecker( dummyPropertyChecker() );

        DynamicRecord dynamic = records.add( inUse( array( new DynamicRecord( 42 ) ) ) );
        PropertyRecord property1 = records.add( inUse( new PropertyRecord( 1 ) ) );
        PropertyRecord property2 = records.add( inUse( new PropertyRecord( 2 ) ) );
        PropertyIndexRecord key = records.add( inUse( new PropertyIndexRecord( 10 ) ) );
        property1.addPropertyBlock( propertyBlock( key, PropertyType.ARRAY, dynamic.getId() ) );
        property2.addPropertyBlock( propertyBlock( key, PropertyType.ARRAY, dynamic.getId() ) );

        // when
        ConsistencyReport.PropertyConsistencyReport report1 = check( ConsistencyReport.PropertyConsistencyReport.class,
                                                                     propChecker, property1, records );
        ConsistencyReport.PropertyConsistencyReport report2 = check( ConsistencyReport.PropertyConsistencyReport.class,
                                                                     propChecker, property2, records );

        // then
        verifyOnlyReferenceDispatch( report1 );
        verify( report2 ).arrayMultipleOwners( property1 );
        verifyOnlyReferenceDispatch( report2 );
    }

    @Test
    public void shouldReportDynamicRecordOwnedByPropertyAndOtherDynamic() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck decorator = new OwnerCheck( true, DynamicStore.STRING );

        RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> dynChecker = decorator
                .decorateDynamicChecker( RecordType.STRING_PROPERTY,
                                         dummyDynamicCheck( configureDynamicStore( 50 ), DynamicStore.STRING ) );
        RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> propChecker = decorator
                .decoratePropertyChecker( dummyPropertyChecker() );

        DynamicRecord owned = records.add( inUse( string( new DynamicRecord( 42 ) ) ) );
        DynamicRecord dynamic = records.add( inUse( string( new DynamicRecord( 100 ) ) ) );
        dynamic.setNextBlock( owned.getId() );
        PropertyRecord property = records.add( inUse( new PropertyRecord( 1 ) ) );
        PropertyIndexRecord key = records.add( inUse( new PropertyIndexRecord( 10 ) ) );
        property.addPropertyBlock( propertyBlock( key, PropertyType.STRING, owned.getId() ) );

        // when
        ConsistencyReport.PropertyConsistencyReport propReport = check(
                ConsistencyReport.PropertyConsistencyReport.class, propChecker, property, records );
        ConsistencyReport.DynamicConsistencyReport dynReport = check(
                ConsistencyReport.DynamicConsistencyReport.class, dynChecker, dynamic, records );

        // then
        verifyOnlyReferenceDispatch( propReport );
        verify( dynReport ).nextMultipleOwners( property );
        verifyOnlyReferenceDispatch( dynReport );
    }

    @Test
    public void shouldReportDynamicStringRecordOwnedByOtherDynamicAndProperty() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck decorator = new OwnerCheck( true, DynamicStore.STRING );

        RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> dynChecker = decorator
                .decorateDynamicChecker( RecordType.STRING_PROPERTY,
                                         dummyDynamicCheck( configureDynamicStore( 50 ), DynamicStore.STRING ) );
        RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> propChecker = decorator
                .decoratePropertyChecker( dummyPropertyChecker() );

        DynamicRecord owned = records.add( inUse( string( new DynamicRecord( 42 ) ) ) );
        DynamicRecord dynamic = records.add( inUse( string( new DynamicRecord( 100 ) ) ) );
        dynamic.setNextBlock( owned.getId() );
        PropertyRecord property = records.add( inUse( new PropertyRecord( 1 ) ) );
        PropertyIndexRecord key = records.add( inUse( new PropertyIndexRecord( 10 ) ) );
        property.addPropertyBlock( propertyBlock( key, PropertyType.STRING, owned.getId() ) );

        // when
        ConsistencyReport.DynamicConsistencyReport dynReport = check(
                ConsistencyReport.DynamicConsistencyReport.class, dynChecker, dynamic, records );
        ConsistencyReport.PropertyConsistencyReport propReport = check(
                ConsistencyReport.PropertyConsistencyReport.class, propChecker, property, records );

        // then
        verifyOnlyReferenceDispatch( dynReport );
        verify( propReport ).stringMultipleOwners( dynamic );
        verifyOnlyReferenceDispatch( dynReport );
    }

    @Test
    public void shouldReportDynamicArrayRecordOwnedByOtherDynamicAndProperty() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck decorator = new OwnerCheck( true, DynamicStore.ARRAY );

        RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> dynChecker = decorator
                .decorateDynamicChecker( RecordType.ARRAY_PROPERTY,
                                         dummyDynamicCheck( configureDynamicStore( 50 ), DynamicStore.ARRAY ) );
        RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> propChecker = decorator
                .decoratePropertyChecker( dummyPropertyChecker() );

        DynamicRecord owned = records.add( inUse( array( new DynamicRecord( 42 ) ) ) );
        DynamicRecord dynamic = records.add( inUse( array( new DynamicRecord( 100 ) ) ) );
        dynamic.setNextBlock( owned.getId() );
        PropertyRecord property = records.add( inUse( new PropertyRecord( 1 ) ) );
        PropertyIndexRecord key = records.add( inUse( new PropertyIndexRecord( 10 ) ) );
        property.addPropertyBlock( propertyBlock( key, PropertyType.ARRAY, owned.getId() ) );

        // when
        ConsistencyReport.DynamicConsistencyReport dynReport = check(
                ConsistencyReport.DynamicConsistencyReport.class, dynChecker, dynamic, records );
        ConsistencyReport.PropertyConsistencyReport propReport = check(
                ConsistencyReport.PropertyConsistencyReport.class, propChecker, property, records );

        // then
        verifyOnlyReferenceDispatch( dynReport );
        verify( propReport ).arrayMultipleOwners( dynamic );
        verifyOnlyReferenceDispatch( dynReport );
    }

    @Test
    public void shouldReportDynamicRecordOwnedByTwoRelationshipLabels() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck decorator = new OwnerCheck( true, DynamicStore.RELATIONSHIP_LABEL );

        RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> checker =
                decorator.decorateLabelChecker( dummyRelationshipLabelCheck() );

        DynamicRecord dynamic = records.addLabelName( inUse( string( new DynamicRecord( 42 ) ) ) );
        RelationshipTypeRecord record1 = records.add( inUse( new RelationshipTypeRecord( 1 ) ) );
        RelationshipTypeRecord record2 = records.add( inUse( new RelationshipTypeRecord( 2 ) ) );
        record1.setNameId( (int) dynamic.getId() );
        record2.setNameId( (int) dynamic.getId() );

        // when
        ConsistencyReport.LabelConsistencyReport report1 = check( ConsistencyReport.LabelConsistencyReport.class,
                                                                  checker,record1, records );
        ConsistencyReport.LabelConsistencyReport report2 = check( ConsistencyReport.LabelConsistencyReport.class,
                                                                  checker,record2, records );

        // then
        verifyOnlyReferenceDispatch( report1 );
        verify( report2 ).nameMultipleOwners( record1 );
        verifyOnlyReferenceDispatch( report2 );
    }

    @Test
    public void shouldReportDynamicRecordOwnedByRelationshipLabelAndOtherDynamicRecord() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck decorator = new OwnerCheck( true, DynamicStore.RELATIONSHIP_LABEL );

        RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> dynChecker =
                decorator.decorateDynamicChecker(
                        RecordType.RELATIONSHIP_LABEL_NAME,
                        dummyDynamicCheck( configureDynamicStore( 50 ), DynamicStore.RELATIONSHIP_LABEL ) );

        RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> labelCheck =
                decorator.decorateLabelChecker( dummyRelationshipLabelCheck() );

        DynamicRecord owned = records.addLabelName( inUse( string( new DynamicRecord( 42 ) ) ) );
        DynamicRecord dynamic = records.addLabelName( inUse( string( new DynamicRecord( 1 ) ) ) );
        RelationshipTypeRecord label = records.add( inUse( new RelationshipTypeRecord( 1 ) ) );
        dynamic.setNextBlock( owned.getId() );
        label.setNameId( (int) owned.getId() );

        // when
        ConsistencyReport.LabelConsistencyReport labelReport = check( ConsistencyReport.LabelConsistencyReport.class,
                                                                      labelCheck, label, records );
        ConsistencyReport.DynamicConsistencyReport dynReport = check( ConsistencyReport.DynamicConsistencyReport.class,
                                                                      dynChecker, dynamic, records );

        // then
        verifyOnlyReferenceDispatch( labelReport );
        verify( dynReport ).nextMultipleOwners( label );
        verifyOnlyReferenceDispatch( dynReport );
    }

    @Test
    public void shouldReportDynamicRecordOwnedByOtherDynamicRecordAndRelationshipLabel() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck decorator = new OwnerCheck( true, DynamicStore.RELATIONSHIP_LABEL );

        RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> dynChecker =
                decorator.decorateDynamicChecker(
                        RecordType.RELATIONSHIP_LABEL_NAME,
                        dummyDynamicCheck( configureDynamicStore( 50 ), DynamicStore.RELATIONSHIP_LABEL ) );

        RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> labelCheck =
                decorator.decorateLabelChecker( dummyRelationshipLabelCheck() );

        DynamicRecord owned = records.addLabelName( inUse( string( new DynamicRecord( 42 ) ) ) );
        DynamicRecord dynamic = records.addLabelName( inUse( string( new DynamicRecord( 1 ) ) ) );
        RelationshipTypeRecord label = records.add( inUse( new RelationshipTypeRecord( 1 ) ) );
        dynamic.setNextBlock( owned.getId() );
        label.setNameId( (int) owned.getId() );

        // when
        ConsistencyReport.DynamicConsistencyReport dynReport = check( ConsistencyReport.DynamicConsistencyReport.class,
                                                                      dynChecker, dynamic, records );
        ConsistencyReport.LabelConsistencyReport labelReport = check( ConsistencyReport.LabelConsistencyReport.class,
                                                                      labelCheck, label, records );

        // then
        verifyOnlyReferenceDispatch( dynReport );
        verify( labelReport ).nameMultipleOwners( dynamic );
        verifyOnlyReferenceDispatch( labelReport );
    }

    @Test
    public void shouldReportDynamicRecordOwnedByTwoPropertyKeys() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck decorator = new OwnerCheck( true, DynamicStore.PROPERTY_KEY );

        RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> checker =
                decorator.decoratePropertyKeyChecker( dummyPropertyKeyCheck() );

        DynamicRecord dynamic = records.addKeyName( inUse( string( new DynamicRecord( 42 ) ) ) );
        PropertyIndexRecord record1 = records.add( inUse( new PropertyIndexRecord( 1 ) ) );
        PropertyIndexRecord record2 = records.add( inUse( new PropertyIndexRecord( 2 ) ) );
        record1.setNameId( (int) dynamic.getId() );
        record2.setNameId( (int) dynamic.getId() );

        // when
        ConsistencyReport.PropertyKeyConsistencyReport report1 = check(
                ConsistencyReport.PropertyKeyConsistencyReport.class, checker,record1, records );
        ConsistencyReport.PropertyKeyConsistencyReport report2 = check(
                ConsistencyReport.PropertyKeyConsistencyReport.class, checker,record2, records );

        // then
        verifyOnlyReferenceDispatch( report1 );
        verify( report2 ).nameMultipleOwners( record1 );
        verifyOnlyReferenceDispatch( report2 );
    }

    @Test
    public void shouldReportDynamicRecordOwnedByPropertyKeyAndOtherDynamicRecord() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck decorator = new OwnerCheck( true, DynamicStore.PROPERTY_KEY );

        RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> dynChecker =
                decorator.decorateDynamicChecker(
                        RecordType.PROPERTY_KEY_NAME,
                        dummyDynamicCheck( configureDynamicStore( 50 ), DynamicStore.PROPERTY_KEY ) );

        RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> keyCheck =
                decorator.decoratePropertyKeyChecker( dummyPropertyKeyCheck() );

        DynamicRecord owned = records.addKeyName( inUse( string( new DynamicRecord( 42 ) ) ) );
        DynamicRecord dynamic = records.addKeyName( inUse( string( new DynamicRecord( 1 ) ) ) );
        PropertyIndexRecord key = records.add( inUse( new PropertyIndexRecord( 1 ) ) );
        dynamic.setNextBlock( owned.getId() );
        key.setNameId( (int) owned.getId() );

        // when
        ConsistencyReport.PropertyKeyConsistencyReport keyReport = check(
                ConsistencyReport.PropertyKeyConsistencyReport.class, keyCheck, key, records );
        ConsistencyReport.DynamicConsistencyReport dynReport = check(
                ConsistencyReport.DynamicConsistencyReport.class, dynChecker, dynamic, records );

        // then
        verifyOnlyReferenceDispatch( keyReport );
        verify( dynReport ).nextMultipleOwners( key );
        verifyOnlyReferenceDispatch( dynReport );
    }

    @Test
    public void shouldReportDynamicRecordOwnedByOtherDynamicRecordAndPropertyKey() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck decorator = new OwnerCheck( true, DynamicStore.PROPERTY_KEY );

        RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> dynChecker =
                decorator.decorateDynamicChecker(
                        RecordType.PROPERTY_KEY_NAME,
                        dummyDynamicCheck( configureDynamicStore( 50 ), DynamicStore.PROPERTY_KEY ) );

        RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> keyCheck =
                decorator.decoratePropertyKeyChecker( dummyPropertyKeyCheck() );

        DynamicRecord owned = records.addKeyName( inUse( string( new DynamicRecord( 42 ) ) ) );
        DynamicRecord dynamic = records.addKeyName( inUse( string( new DynamicRecord( 1 ) ) ) );
        PropertyIndexRecord key = records.add( inUse( new PropertyIndexRecord( 1 ) ) );
        dynamic.setNextBlock( owned.getId() );
        key.setNameId( (int) owned.getId() );

        // when
        ConsistencyReport.DynamicConsistencyReport dynReport = check(
                ConsistencyReport.DynamicConsistencyReport.class,dynChecker, dynamic, records );
        ConsistencyReport.PropertyKeyConsistencyReport keyReport = check(
                ConsistencyReport.PropertyKeyConsistencyReport.class, keyCheck, key, records );

        // then
        verifyOnlyReferenceDispatch( dynReport );
        verify( keyReport ).nameMultipleOwners( dynamic );
        verifyOnlyReferenceDispatch( keyReport );
    }

    @Test
    public void shouldReportOrphanedDynamicStringRecord() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck owners = new OwnerCheck( true, DynamicStore.STRING );

        RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> stringCheck =
                owners.decorateDynamicChecker( RecordType.STRING_PROPERTY,
                                               dummyDynamicCheck( configureDynamicStore( 60 ),
                                                                  DynamicStore.STRING ) );
        DynamicRecord record = string( inUse( new DynamicRecord( 42 ) ) );

        // when
        ConsistencyReport.DynamicConsistencyReport report = check( ConsistencyReport.DynamicConsistencyReport.class,
                                                                   stringCheck, record, records );
        owners.scanForOrphanChains( ProgressMonitorFactory.NONE );
        records.checkDeferred();

        // then
        verify( report ).orphanDynamicRecord();
    }

    @Test
    public void shouldReportOrphanedDynamicArrayRecord() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck owners = new OwnerCheck( true, DynamicStore.ARRAY );

        RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> stringCheck =
                owners.decorateDynamicChecker( RecordType.ARRAY_PROPERTY,
                                               dummyDynamicCheck( configureDynamicStore( 60 ),
                                                                  DynamicStore.ARRAY ) );
        DynamicRecord record = string( inUse( new DynamicRecord( 42 ) ) );

        // when
        ConsistencyReport.DynamicConsistencyReport report = check( ConsistencyReport.DynamicConsistencyReport.class,
                                                                   stringCheck, record, records );
        owners.scanForOrphanChains( ProgressMonitorFactory.NONE );
        records.checkDeferred();

        // then
        verify( report ).orphanDynamicRecord();
    }

    @Test
    public void shouldReportOrphanedDynamicRelationshipLabelRecord() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck owners = new OwnerCheck( true, DynamicStore.RELATIONSHIP_LABEL );

        RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> stringCheck =
                owners.decorateDynamicChecker( RecordType.RELATIONSHIP_LABEL_NAME,
                                               dummyDynamicCheck( configureDynamicStore( 60 ),
                                                                  DynamicStore.RELATIONSHIP_LABEL ) );
        DynamicRecord record = string( inUse( new DynamicRecord( 42 ) ) );

        // when
        ConsistencyReport.DynamicConsistencyReport report = check( ConsistencyReport.DynamicConsistencyReport.class,
                                                                   stringCheck, record, records );
        owners.scanForOrphanChains( ProgressMonitorFactory.NONE );
        records.checkDeferred();

        // then
        verify( report ).orphanDynamicRecord();
    }

    @Test
    public void shouldReportOrphanedDynamicPropertyKeyRecord() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        OwnerCheck owners = new OwnerCheck( true, DynamicStore.PROPERTY_KEY );

        RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> stringCheck =
                owners.decorateDynamicChecker( RecordType.PROPERTY_KEY_NAME,
                                               dummyDynamicCheck( configureDynamicStore( 60 ),
                                                                  DynamicStore.PROPERTY_KEY ) );
        DynamicRecord record = string( inUse( new DynamicRecord( 42 ) ) );

        // when
        ConsistencyReport.DynamicConsistencyReport report = check( ConsistencyReport.DynamicConsistencyReport.class,
                                                                   stringCheck, record, records );
        owners.scanForOrphanChains( ProgressMonitorFactory.NONE );
        records.checkDeferred();

        // then
        verify( report ).orphanDynamicRecord();
    }
}
