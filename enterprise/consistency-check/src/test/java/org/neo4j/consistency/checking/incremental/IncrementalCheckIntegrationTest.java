/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.consistency.checking.incremental;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.consistency.ConsistencyCheckingError;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.GraphStoreFixture;
import org.neo4j.consistency.checking.incremental.intercept.VerifyingTransactionInterceptorProvider;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.LongerShortString;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.RecordSerializer;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.consistency.checking.full.FullCheckIntegrationTest.serializeRule;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.test.Property.property;
import static org.neo4j.test.Property.set;

public class IncrementalCheckIntegrationTest
{
    @Test
    public void shouldReportBrokenSchemaRecordChain() throws Exception
    {
        verifyInconsistencyReported( RecordType.SCHEMA, new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                DynamicRecord schema = new DynamicRecord( next.schema() );
                DynamicRecord before = schema.clone();
                schema.setNextBlock( next.schema() );
                IndexRule rule = IndexRule.indexRule( 1, 1, 1, new SchemaIndexProvider.Descriptor( "in-memory",
                        "1.0" ) );
                new RecordSerializer().append( rule ).serialize();
                schema.setData( new RecordSerializer().append( rule ).serialize() );

                tx.createSchema( asList(before), asList( schema ) );
            }
        } );
    }

    @Test
    public void shouldReportDuplicateConstraintReferences() throws Exception
    {
        verifyInconsistencyReported( RecordType.SCHEMA, new GraphStoreFixture.Transaction()
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

                DynamicRecord record1Before = record1.clone();
                DynamicRecord record2Before = record2.clone();

                SchemaIndexProvider.Descriptor providerDescriptor = new SchemaIndexProvider.Descriptor( "in-memory", "1.0" );

                IndexRule rule1 = IndexRule.constraintIndexRule( ruleId1, labelId, propertyKeyId, providerDescriptor, (long) ruleId1);
                IndexRule rule2 = IndexRule.constraintIndexRule( ruleId2, labelId, propertyKeyId, providerDescriptor, (long) ruleId1);

                Collection<DynamicRecord> records1 = serializeRule( rule1, record1 );
                Collection<DynamicRecord> records2 = serializeRule( rule2, record2 );

                assertEquals( asList( record1 ), records1 );
                assertEquals( asList( record2 ), records2 );

                tx.nodeLabel( labelId, "label" );
                tx.propertyKey( propertyKeyId, "property" );

                tx.createSchema( asList(record1Before), records1 );
                tx.createSchema( asList(record2Before), records2 );
            }
        } );
   }

    @Test
    public void shouldReportInvalidConstraintBackReferences() throws Exception
    {
        verifyInconsistencyReported( RecordType.SCHEMA, new GraphStoreFixture.Transaction()
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
                DynamicRecord record1Before = record1.clone();
                DynamicRecord record2Before = record2.clone();

                SchemaIndexProvider.Descriptor providerDescriptor = new SchemaIndexProvider.Descriptor( "in-memory",
                        "1.0" );

                IndexRule rule1 = IndexRule.constraintIndexRule( ruleId1, labelId, propertyKeyId, providerDescriptor,
                        (long) ruleId2 );
                UniquenessConstraintRule rule2 = UniquenessConstraintRule.uniquenessConstraintRule( ruleId2, labelId,
                        propertyKeyId, ruleId2 );


                Collection<DynamicRecord> records1 = serializeRule( rule1, record1 );
                Collection<DynamicRecord> records2 = serializeRule( rule2, record2 );

                assertEquals( asList( record1 ), records1 );
                assertEquals( asList( record2 ), records2 );

                tx.nodeLabel( labelId, "label" );
                tx.propertyKey( propertyKeyId, "property" );

                tx.createSchema( asList(record1Before), records1 );
                tx.createSchema( asList(record2Before), records2 );
            }
        } );
    }

    @Test
    @Ignore("Support for checking NeoStore needs to be added")
    public void shouldReportNeoStoreInconsistencies() throws Exception
    {
        verifyInconsistencyReported( RecordType.NEO_STORE, new GraphStoreFixture.Transaction()
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
    }

    @Test
    public void shouldReportNodeInconsistency() throws Exception
    {
        verifyInconsistencyReported( RecordType.NODE, new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                tx.create( new NodeRecord( next.node(), next.relationship(), -1 ) );
            }
        } );
    }

    @Test
    public void shouldReportRelationshipInconsistency() throws Exception
    {
        verifyInconsistencyReported( RecordType.RELATIONSHIP, new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                long node = next.node();
                tx.create( new RelationshipRecord( next.relationship(), node, node, 0 ) );
            }
        } );
    }

    @Test
    public void shouldReportPropertyInconsistency() throws Exception
    {
        verifyInconsistencyReported( RecordType.PROPERTY, new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                PropertyRecord property = new PropertyRecord( next.property() );
                property.setPrevProp( next.property() );
                property.setNodeId( 1 );

                PropertyBlock block = new PropertyBlock();
                block.setSingleBlock( (((long) PropertyType.INT.intValue()) << 24) | (666 << 28) );
                property.addPropertyBlock( block );

                tx.create( property );
            }
        } );
    }

    @Test
    public void shouldReportStringPropertyInconsistency() throws Exception
    {
        verifyInconsistencyReported( RecordType.STRING_PROPERTY, new GraphStoreFixture.Transaction()
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

                NodeRecord node = new NodeRecord( next.node(), -1, property.getId() );
                property.setNodeId( node.getId() );
                tx.create( node );

                tx.create( property );
            }
        } );
    }

    @Test
    public void shouldReportArrayPropertyInconsistency() throws Exception
    {
        verifyInconsistencyReported( RecordType.ARRAY_PROPERTY, new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                DynamicRecord array = new DynamicRecord( next.arrayProperty() );
                array.setInUse( true );
                array.setCreated();
                array.setType( PropertyType.ARRAY.intValue() );
                array.setNextBlock( next.arrayProperty() );
                array.setData( UTF8.encode( "hello world" ) );

                PropertyBlock block = new PropertyBlock();
                block.setSingleBlock( (((long) PropertyType.ARRAY.intValue()) << 24) | (array.getId() << 28) );
                block.addValueRecord( array );

                PropertyRecord property = new PropertyRecord( next.property() );
                property.addPropertyBlock( block );

                NodeRecord node = new NodeRecord( next.node(), -1, property.getId() );
                property.setNodeId( node.getId() );
                tx.create( node );

                tx.create( property );
            }
        } );
    }

    private static String LONG_STRING, LONG_SHORT_STRING;

    static
    {
        StringBuilder longString = new StringBuilder();
        String longShortString = "";
        for ( int i = 0; LongerShortString.encode( 0, longString.toString(), new PropertyBlock(),
                                                   PropertyType.getPayloadSize() ); i++ )
        {
            longShortString = longString.toString();
            longString.append( 'a' + (i % ('z' - 'a')) );
        }
        LONG_SHORT_STRING = longShortString;
        LONG_STRING = longString.toString();
    }

    @Rule
    public final GraphStoreFixture fixture = new GraphStoreFixture()
    {
        @Override
        protected void generateInitialData( GraphDatabaseService graphDb )
        {
            org.neo4j.graphdb.Transaction tx = graphDb.beginTx();
            try
            {
                Node node1 = set( graphDb.createNode(),
                                  property( "long short short", LONG_SHORT_STRING ) );
                Node node2 = set( graphDb.createNode(),
                                  property( "long string", LONG_STRING ) );
                Node node3 = set( graphDb.createNode(),
                                  property( "one", "1" ),
                                  property( "two", "2" ),
                                  property( "three", "3" ),
                                  property( "four", "4" ),
                                  property( "five", "5" ) );
                Node node4 = set( graphDb.createNode(),
                                  property( "name", "Leeloo Dallas" ) );
                Node node5 = set( graphDb.createNode(),
                                  property( "payload", LONG_SHORT_STRING ),
                                  property( "more", LONG_STRING ) );
                Node node6 = set( graphDb.createNode() );

                set( node1.createRelationshipTo( node2, withName( "WHEEL" ) ) );
                set( node2.createRelationshipTo( node3, withName( "WHEEL" ) ) );
                set( node3.createRelationshipTo( node4, withName( "WHEEL" ) ) );
                set( node4.createRelationshipTo( node5, withName( "WHEEL" ) ) );
                set( node5.createRelationshipTo( node1, withName( "WHEEL" ) ) );

                set( node6.createRelationshipTo( node1, withName( "STAR" ) ) );
                set( node6.createRelationshipTo( node2, withName( "STAR" ) ) );
                set( node6.createRelationshipTo( node3, withName( "STAR" ) ) );
                set( node6.createRelationshipTo( node4, withName( "STAR" ) ) );
                set( node6.createRelationshipTo( node5, withName( "STAR" ) ) );

                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }

        @Override
        protected Map<String, String> configuration( boolean initialData )
        {
            Map<String, String> config = super.configuration( initialData );
            if ( !initialData )
            {
                config.put( GraphDatabaseSettings.intercept_deserialized_transactions.name(), "true" );
                config.put( GraphDatabaseSettings.intercept_committing_transactions.name(), "true" );
                config.put( TransactionInterceptorProvider.class.getSimpleName() + "." +
                                    VerifyingTransactionInterceptorProvider.NAME, "true" );
            }
            return config;
        }
    };

    private void verifyInconsistencyReported( RecordType recordType,
                                              GraphStoreFixture.Transaction inconsistentTransaction )
            throws IOException
    {
        // when
        try
        {
            fixture.apply( inconsistentTransaction );
            fail( "should have thrown error" );
        }
        // then
        catch ( ConsistencyCheckingError expected )
        {
            int count = expected.getInconsistencyCountForRecordType( recordType );
            int total = expected.getTotalInconsistencyCount();
            String summary = expected.getMessage().replace( "\n", "\n\t" );
            assertTrue( "Expected failures for " + recordType + ", got " + summary, count > 0 );
            assertEquals( "Didn't expect failures for any other type than " + recordType + ", got " + summary,
                          count, total );
        }
    }
}
