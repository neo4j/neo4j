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

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.nioneo.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PreAllocatedRecords;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.RecordSerializer;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule;
import org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.GraphStoreFixture;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.consistency.checking.RecordCheckTestBase.inUse;
import static org.neo4j.consistency.checking.RecordCheckTestBase.notInUse;
import static org.neo4j.consistency.checking.full.ExecutionOrderIntegrationTest.config;
import static org.neo4j.helpers.collection.IteratorUtil.asIterator;
import static org.neo4j.kernel.impl.nioneo.store.AbstractDynamicStore.readFullByteArrayFromHeavyRecords;
import static org.neo4j.kernel.impl.nioneo.store.DynamicArrayStore.allocateFromNumbers;
import static org.neo4j.kernel.impl.nioneo.store.DynamicArrayStore.getRightArray;
import static org.neo4j.kernel.impl.nioneo.store.PropertyType.ARRAY;
import static org.neo4j.kernel.impl.nioneo.store.labels.DynamicNodeLabels.dynamicPointer;
import static org.neo4j.test.Property.property;
import static org.neo4j.test.Property.set;

public class FullCheckIntegrationTest
{
    @Rule
    public final GraphStoreFixture fixture = new GraphStoreFixture()
    {
        @Override
        protected void generateInitialData( GraphDatabaseService graphDb )
        {
            org.neo4j.graphdb.Transaction tx = graphDb.beginTx();
            try
            {
                Node node1 = set( graphDb.createNode() );
                node1.addLabel( DynamicLabel.label( "label1" ) );
                Node node2 = set( graphDb.createNode(), property( "key", "value" ) );
                node2.addLabel( DynamicLabel.label( "label2" ) );
                node1.createRelationshipTo( node2, DynamicRelationshipType.withName( "C" ) );
                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }
    };
    private final StringWriter log = new StringWriter();

    private ConsistencySummaryStatistics check() throws ConsistencyCheckIncompleteException
    {
        return check( fixture.storeAccess() );
    }

    private ConsistencySummaryStatistics check( StoreAccess access ) throws ConsistencyCheckIncompleteException
    {
        FullCheck checker = new FullCheck( config( TaskExecutionOrder.MULTI_PASS ), ProgressMonitorFactory.NONE );
        return checker.execute( access, StringLogger.wrap( log ) );
    }

    private void verifyInconsistency( RecordType recordType, ConsistencySummaryStatistics stats )
    {
        int count = stats.getInconsistencyCountForRecordType( recordType );
        assertTrue( "Expected inconsistencies for records of type " + recordType, count > 0 );
        assertEquals( "Expected only inconsistencies of type " + recordType + ", got:\n" + log,
                      count, stats.getTotalInconsistencyCount() );
    }

    @Test
    public void shouldCheckConsistencyOfAConsistentStore() throws Exception
    {
        // when
        ConsistencySummaryStatistics result = check();

        // then
        assertEquals( 0, result.getTotalInconsistencyCount() );
    }

    @Test
    @Ignore("Support for checking NeoStore needs to be added")
    public void shouldReportNeoStoreInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                NeoStoreRecord record = new NeoStoreRecord();
                record.setNextProp( next.property() );
                tx.update( record );
                // We get exceptions when only the above happens in a transaction...
                tx.create( new NodeRecord( next.node(), -1, -1 ) );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        verifyInconsistency( RecordType.NEO_STORE, stats );
    }

    @Test
    public void shouldReportNodeInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                tx.create( new NodeRecord( next.node(), next.relationship(), -1 ) );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        verifyInconsistency( RecordType.NODE, stats );
    }

    @Test
    public void shouldReportInlineNodeLabelInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                NodeRecord nodeRecord = new NodeRecord( next.node(), -1, -1 );
                NodeLabelsField.parseLabelsField( nodeRecord ).add( 10, null );
                tx.create( nodeRecord );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        verifyInconsistency( RecordType.NODE, stats );
    }

    @Test
    public void shouldReportNodeDynamicLabelContainingUnknownLabelAsNodeInconsistency() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                NodeRecord nodeRecord = new NodeRecord( next.node(), -1, -1 );
                DynamicRecord record = inUse( new DynamicRecord( next.nodeLabel() ) );
                Collection<DynamicRecord> newRecords = allocateFromNumbers( new long[] { nodeRecord.getLongId(), 42l },
                        asIterator( record ), new PreAllocatedRecords( 60 ) );
                nodeRecord.setLabelField( dynamicPointer( newRecords ), newRecords );

                tx.create( nodeRecord );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        verifyInconsistency( RecordType.NODE, stats );
    }

    @Test
    public void shouldNotReportAnythingForNodeWithConsistentChainOfDynamicRecordsWithLabels() throws Exception
    {
        // given
        assertEquals( 3, chainOfDynamicRecordsWithLabelsForANode( 130 ).size() );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        assertTrue( "should be consistent", stats.isConsistent() );
    }

    @Test @Ignore("2013-07-17 Revisit once we store sorted label ids")
    public void shouldReportOrphanNodeDynamicLabelAsInconsistency() throws Exception
    {
        // given
        final List<DynamicRecord> chain = chainOfDynamicRecordsWithLabelsForANode( 130 );
        assertEquals( 3, chain.size() );
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                DynamicRecord record1 = inUse( new DynamicRecord( chain.get( 0 ).getId() ) );
                DynamicRecord record2 = notInUse( new DynamicRecord( chain.get( 1 ).getId() ) );
                long[] data = (long[]) getRightArray( readFullByteArrayFromHeavyRecords( chain, ARRAY ) );
                PreAllocatedRecords allocator = new PreAllocatedRecords( 60 );
                allocateFromNumbers( Arrays.copyOf( data, 11 ), asIterator( record1 ), allocator );

                NodeRecord before = inUse( new NodeRecord( data[0], -1, -1 ) );
                NodeRecord after = inUse( new NodeRecord( data[0], -1, -1 ) );
                before.setLabelField( dynamicPointer( asList( record1 ) ), chain );
                after.setLabelField( dynamicPointer( asList( record1 ) ), asList( record1, record2 ) );
                tx.update( before, after );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        verifyInconsistency( RecordType.NODE_DYNAMIC_LABEL, stats );
    }

    @Test
    public void shouldReportCyclesInDynamicRecordsWithLabels() throws Exception
    {
        // given
        final List<DynamicRecord> chain = chainOfDynamicRecordsWithLabelsForANode( 176/*3 full records*/ );
        assertEquals( "number of records in chain", 3, chain.size() );
        assertEquals( "all records full", chain.get( 0 ).getLength(),  chain.get( 2 ).getLength() );
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                long nodeId = ((long[]) getRightArray( readFullByteArrayFromHeavyRecords( chain, ARRAY ) ))[0];
                NodeRecord before = inUse( new NodeRecord( nodeId, -1, -1 ) );
                NodeRecord after = inUse( new NodeRecord( nodeId, -1, -1 ) );
                DynamicRecord record1 = chain.get( 0 ).clone();
                DynamicRecord record2 = chain.get( 1 ).clone();
                DynamicRecord record3 = chain.get( 2 ).clone();

                record3.setNextBlock( record2.getId() );
                before.setLabelField( dynamicPointer( chain ), chain );
                after.setLabelField( dynamicPointer( chain ), asList( record1, record2, record3 ) );
                tx.update( before, after );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        verifyInconsistency( RecordType.NODE, stats );
    }

    private List<DynamicRecord> chainOfDynamicRecordsWithLabelsForANode( int labelCount ) throws IOException
    {
        final long[] labels = new long[labelCount+1]; // allocate enough labels to need three records
        for ( int i = 1/*leave space for the node id*/; i < labels.length; i++ )
        {
            final int offset = i;
            fixture.apply( new GraphStoreFixture.Transaction()
            { // Neo4j can create no more than one label per transaction...
                @Override
                protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                                GraphStoreFixture.IdGenerator next )
                {
                    tx.nodeLabel( (int) (labels[offset] = next.label()), "label:" + offset );
                }
            } );
        }
        final List<DynamicRecord> chain = new ArrayList<>();
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                NodeRecord nodeRecord = new NodeRecord( next.node(), -1, -1 );
                DynamicRecord record1 = inUse( new DynamicRecord( next.nodeLabel() ) );
                DynamicRecord record2 = inUse( new DynamicRecord( next.nodeLabel() ) );
                DynamicRecord record3 = inUse( new DynamicRecord( next.nodeLabel() ) );
                labels[0] = nodeRecord.getLongId(); // the first id should not be a label id, but the id of the node
                PreAllocatedRecords allocator = new PreAllocatedRecords( 60 );
                chain.addAll( allocateFromNumbers(
                        labels, asIterator( record1, record2, record3 ), allocator ) );

                nodeRecord.setLabelField( dynamicPointer( chain ), chain );

                tx.create( nodeRecord );
            }
        } );
        return chain;
    }

    @Test
    public void shouldReportNodeDynamicLabelContainingDuplicateLabelAsNodeInconsistency() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                tx.nodeLabel( 42, "Label" );

                NodeRecord nodeRecord = new NodeRecord( next.node(), -1, -1 );
                DynamicRecord record = inUse( new DynamicRecord( next.nodeLabel() ) );
                Collection<DynamicRecord> newRecords = allocateFromNumbers( new long[] { nodeRecord.getLongId(), 42l, 42l },
                        asIterator( record ), new PreAllocatedRecords( 60 ) );
                nodeRecord.setLabelField( dynamicPointer( newRecords ), newRecords );

                tx.create( nodeRecord );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        verifyInconsistency( RecordType.NODE, stats );
    }

    @Test
    public void shouldReportOrphanedNodeDynamicLabelAsNodeInconsistency() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                tx.nodeLabel( 42, "Label" );

                NodeRecord nodeRecord = new NodeRecord( next.node(), -1, -1 );
                DynamicRecord record = inUse( new DynamicRecord( next.nodeLabel() ) );
                Collection<DynamicRecord> newRecords = allocateFromNumbers( new long[] { next.node(), 42l },
                        asIterator( record ), new PreAllocatedRecords( 60 ) );
                nodeRecord.setLabelField( dynamicPointer( newRecords ), newRecords );

                tx.create( nodeRecord );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        verifyInconsistency( RecordType.NODE_DYNAMIC_LABEL, stats );
    }

    @Test
    public void shouldReportRelationshipInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                tx.create( new RelationshipRecord( next.relationship(), 1, 2, 0 ) );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        verifyInconsistency( RecordType.RELATIONSHIP, stats );
    }

    @Test
    public void shouldReportPropertyInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                PropertyRecord property = new PropertyRecord( next.property() );
                property.setPrevProp( next.property() );
                PropertyBlock block = new PropertyBlock();
                block.setSingleBlock( 1 | (((long) PropertyType.INT.intValue()) << 24) | (666 << 28) );
                property.addPropertyBlock( block );
                tx.create( property );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        verifyInconsistency( RecordType.PROPERTY, stats );
    }

    @Test
    public void shouldReportStringPropertyInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                DynamicRecord string = new DynamicRecord( next.stringProperty() );
                string.setInUse( true );
                string.setCreated();
                string.setType( PropertyType.STRING.intValue() );
                string.setNextBlock( next.stringProperty() );
                string.setData( UTF8.encode( "hello world" ) );

                PropertyBlock block = new PropertyBlock();
                block.setSingleBlock( (((long) PropertyType.STRING.intValue()) << 24) | (string.getId() << 28) );
                block.addValueRecord( string );

                PropertyRecord property = new PropertyRecord( next.property() );
                property.addPropertyBlock( block );

                tx.create( property );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        verifyInconsistency( RecordType.STRING_PROPERTY, stats );
    }

    @Test
    public void shouldReportBrokenSchemaRecordChain() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                DynamicRecord schema = new DynamicRecord( next.schema() );
                schema.setNextBlock( next.schema() ); // Point to a record that isn't in use.
                IndexRule rule = IndexRule.indexRule( 1, 1, 1,
                        new SchemaIndexProvider.Descriptor( "in-memory", "1.0" ) );
                schema.setData( new RecordSerializer().append( rule ).serialize() );

                tx.createSchema( asList( schema ) );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        verifyInconsistency( RecordType.SCHEMA, stats );
    }

    @Test
    public void shouldReportDuplicateConstraintReferences() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                int ruleId1 = (int) next.schema();
                int ruleId2 = (int) next.schema();
                int labelId = (int) next.label();
                int propertyKeyId = next.propertyKey();

                DynamicRecord record1 = new DynamicRecord( ruleId1 );
                DynamicRecord record2 = new DynamicRecord( ruleId2 );

                SchemaIndexProvider.Descriptor providerDescriptor = new SchemaIndexProvider.Descriptor( "in-memory", "1.0" );

                IndexRule rule1 = IndexRule.constraintIndexRule( ruleId1, labelId, propertyKeyId, providerDescriptor,
                        (long) ruleId1 );
                IndexRule rule2 = IndexRule.constraintIndexRule( ruleId2, labelId, propertyKeyId, providerDescriptor, (long) ruleId1 );

                Collection<DynamicRecord> records1 = serializeRule( rule1, record1 );
                Collection<DynamicRecord> records2 = serializeRule( rule2, record2 );

                assertEquals( asList( record1 ), records1 );
                assertEquals( asList( record2 ), records2 );

                tx.nodeLabel( labelId, "label" );
                tx.propertyKey( propertyKeyId, "property" );

                tx.createSchema( records1 );
                tx.createSchema( records2 );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        verifyInconsistency( RecordType.SCHEMA, stats );
    }

    @Test
    public void shouldReportInvalidConstraintBackReferences() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                int ruleId1 = (int) next.schema();
                int ruleId2 = (int) next.schema();
                int labelId = (int) next.label();
                int propertyKeyId = next.propertyKey();

                DynamicRecord record1 = new DynamicRecord( ruleId1 );
                DynamicRecord record2 = new DynamicRecord( ruleId2 );

                SchemaIndexProvider.Descriptor providerDescriptor = new SchemaIndexProvider.Descriptor( "in-memory", "1.0" );

                IndexRule rule1 = IndexRule.constraintIndexRule( ruleId1, labelId, propertyKeyId, providerDescriptor, (long) ruleId2 );
                UniquenessConstraintRule rule2 = UniquenessConstraintRule.uniquenessConstraintRule( ruleId2, labelId, propertyKeyId, ruleId2 );


                Collection<DynamicRecord> records1 = serializeRule( rule1, record1 );
                Collection<DynamicRecord> records2 = serializeRule( rule2, record2 );

                assertEquals( asList( record1 ), records1 );
                assertEquals( asList( record2 ), records2 );

                tx.nodeLabel( labelId, "label" );
                tx.propertyKey( propertyKeyId, "property" );

                tx.createSchema( records1 );
                tx.createSchema( records2 );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        verifyInconsistency( RecordType.SCHEMA, stats );
    }

    public static Collection<DynamicRecord> serializeRule( SchemaRule rule, DynamicRecord... records )
    {
        return serializeRule( rule, asList( records ) );
    }

    public static Collection<DynamicRecord> serializeRule( SchemaRule rule, Collection<DynamicRecord> records )
    {
        RecordSerializer serializer = new RecordSerializer();
        serializer.append( rule );

        byte[] data = serializer.serialize();
        PreAllocatedRecords dynamicRecordAllocator = new PreAllocatedRecords( data.length );
        return AbstractDynamicStore.allocateRecordsFromBytes( data, records.iterator(), dynamicRecordAllocator );
    }

    @Test
    public void shouldReportArrayPropertyInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                DynamicRecord array = new DynamicRecord( next.arrayProperty() );
                array.setInUse( true );
                array.setCreated();
                array.setType( ARRAY.intValue() );
                array.setNextBlock( next.arrayProperty() );
                array.setData( UTF8.encode( "hello world" ) );

                PropertyBlock block = new PropertyBlock();
                block.setSingleBlock( (((long) ARRAY.intValue()) << 24) | (array.getId() << 28) );
                block.addValueRecord( array );

                PropertyRecord property = new PropertyRecord( next.property() );
                property.addPropertyBlock( block );

                tx.create( property );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        verifyInconsistency( RecordType.ARRAY_PROPERTY, stats );
    }

    @Test
    public void shouldReportRelationshipLabelNameInconsistencies() throws Exception
    {
        // given
        final Reference<Integer> inconsistentName = new Reference<>();
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                inconsistentName.set( next.relationshipType() );
                tx.relationshipType( inconsistentName.get(), "FOO" );
            }
        } );
        StoreAccess access = fixture.storeAccess();
        DynamicRecord record = access.getRelationshipTypeNameStore().forceGetRecord( inconsistentName.get() );
        record.setNextBlock( record.getId() );
        access.getRelationshipTypeNameStore().updateRecord( record );

        // when
        ConsistencySummaryStatistics stats = check( access );

        // then
        verifyInconsistency( RecordType.RELATIONSHIP_TYPE_NAME, stats );
    }

    @Test
    public void shouldReportPropertyKeyNameInconsistencies() throws Exception
    {
        // given
        final Reference<Integer> inconsistentName = new Reference<>();
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                inconsistentName.set( next.propertyKey() );
                tx.propertyKey( inconsistentName.get(), "FOO" );
            }
        } );
        StoreAccess access = fixture.storeAccess();
        DynamicRecord record = access.getPropertyKeyNameStore().forceGetRecord( inconsistentName.get() );
        record.setNextBlock( record.getId() );
        access.getPropertyKeyNameStore().updateRecord( record );

        // when
        ConsistencySummaryStatistics stats = check( access );

        // then
        verifyInconsistency( RecordType.PROPERTY_KEY_NAME, stats );
    }

    @Test
    public void shouldReportRelationshipTypeInconsistencies() throws Exception
    {
        // given
        StoreAccess access = fixture.storeAccess();
        RelationshipTypeTokenRecord record = access.getRelationshipTypeTokenStore().forceGetRecord( 1 );
        record.setNameId( 20 );
        record.setInUse( true );
        access.getRelationshipTypeTokenStore().updateRecord( record );

        // when
        ConsistencySummaryStatistics stats = check( access );

        // then
        verifyInconsistency( RecordType.RELATIONSHIP_TYPE, stats );
    }

    @Test
    public void shouldReportLabelInconsistencies() throws Exception
    {
        // given
        StoreAccess access = fixture.storeAccess();
        LabelTokenRecord record = access.getLabelTokenStore().forceGetRecord( 1 );
        record.setNameId( 20 );
        record.setInUse( true );
        access.getLabelTokenStore().updateRecord( record );

        // when
        ConsistencySummaryStatistics stats = check( access );

        // then
        verifyInconsistency( RecordType.LABEL, stats );
    }

    @Test
    public void shouldReportPropertyKeyInconsistencies() throws Exception
    {
        // given
        final Reference<Integer> inconsistentKey = new Reference<>();
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                inconsistentKey.set( next.propertyKey() );
                tx.propertyKey( inconsistentKey.get(), "FOO" );
            }
        } );
        StoreAccess access = fixture.storeAccess();
        DynamicRecord record = access.getPropertyKeyNameStore().forceGetRecord( inconsistentKey.get() );
        record.setInUse( false );
        access.getPropertyKeyNameStore().updateRecord( record );

        // when
        ConsistencySummaryStatistics stats = check( access );

        // then
        verifyInconsistency( RecordType.PROPERTY_KEY, stats );
    }

    private static class Reference<T>
    {
        private T value;

        void set(T value)
        {
            this.value = value;
        }

        T get()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return String.valueOf( value );
        }
    }
}
