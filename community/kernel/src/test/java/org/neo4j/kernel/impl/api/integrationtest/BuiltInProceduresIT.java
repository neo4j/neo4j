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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.collection.RawIterator;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.StubResourceManager;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProviderFactory;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.internal.Version;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;

public class BuiltInProceduresIT extends KernelIntegrationTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final ResourceTracker resourceTracker = new StubResourceManager();
    private AtomicBoolean failNextIndexPopulation = new AtomicBoolean();

    @Test
    public void listAllLabels() throws Throwable
    {
        // Given
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeId = transaction.dataWrite().nodeCreate();
        int labelId = transaction.tokenWrite().labelGetOrCreateForName( "MyLabel" );
        transaction.dataWrite().nodeAddLabel( nodeId, labelId );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "labels" ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), contains( equalTo( new Object[]{"MyLabel"} ) ) );
    }

    @Test
    public void listPropertyKeys() throws Throwable
    {
        // Given
        TokenWrite ops = tokenWriteInNewTransaction();
        ops.propertyKeyGetOrCreateForName( "MyProp" );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream = procs()
                .procedureCallRead( procs().procedureGet( procedureName( "db", "propertyKeys" ) ).id(),
                        new Object[0] );

        // Then
        assertThat( asList( stream ), contains( equalTo( new Object[]{"MyProp"} ) ) );
    }

    @Test
    public void listRelationshipTypes() throws Throwable
    {
        // Given
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        int relType = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "MyRelType" );
        long startNodeId = transaction.dataWrite().nodeCreate();
        long endNodeId = transaction.dataWrite().nodeCreate();
        transaction.dataWrite().relationshipCreate(  startNodeId, relType, endNodeId );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream = procs()
                .procedureCallRead( procs().procedureGet( procedureName( "db", "relationshipTypes" ) ).id(),
                        new Object[0] );

        // Then
        assertThat( asList( stream ), contains( equalTo( new Object[]{"MyRelType"} ) ) );
    }

    @Test
    public void listProcedures() throws Throwable
    {
        // When
        RawIterator<Object[],ProcedureException> stream = procs()
                .procedureCallRead( procs().procedureGet( procedureName( "dbms", "procedures" ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( new Object[]{"dbms.listConfig",
                        "dbms.listConfig(searchString =  :: STRING?) :: (name :: STRING?, description :: STRING?, " +
                        "value :: STRING?)",
                        "List the currently active config of Neo4j.", "DBMS"} ),
                equalTo( new Object[]{"db.constraints", "db.constraints() :: (description :: STRING?)",
                        "List all constraints in the database.", "READ"} ),
                equalTo( new Object[]{"db.indexes",
                        "db.indexes() :: (description :: STRING?, label :: STRING?, properties :: LIST? OF STRING?, state :: STRING?, " +
                                "type :: STRING?, provider :: MAP?, failureMessage :: STRING?)",
                        "List all indexes in the database.", "READ"} ),
                equalTo( new Object[]{"db.awaitIndex",
                        "db.awaitIndex(index :: STRING?, timeOutSeconds = 300 :: INTEGER?) :: VOID",
                        "Wait for an index to come online (for example: CALL db.awaitIndex(\":Person(name)\")).", "READ"} ),
                equalTo( new Object[]{"db.awaitIndexes", "db.awaitIndexes(timeOutSeconds = 300 :: INTEGER?) :: VOID",
                        "Wait for all indexes to come online (for example: CALL db.awaitIndexes(\"500\")).", "READ"} ),
                equalTo( new Object[]{"db.resampleIndex", "db.resampleIndex(index :: STRING?) :: VOID",
                        "Schedule resampling of an index (for example: CALL db.resampleIndex(\":Person(name)\")).", "READ"} ),
                equalTo( new Object[]{"db.resampleOutdatedIndexes", "db.resampleOutdatedIndexes() :: VOID",
                        "Schedule resampling of all outdated indexes.", "READ"} ),
                equalTo( new Object[]{"db.propertyKeys", "db.propertyKeys() :: (propertyKey :: STRING?)",
                        "List all property keys in the database.", "READ"} ),
                equalTo( new Object[]{"db.labels", "db.labels() :: (label :: STRING?)",
                        "List all labels in the database.", "READ"} ),
                equalTo( new Object[]{"db.schema", "db.schema() :: (nodes :: LIST? OF NODE?, relationships :: LIST? " +
                                                   "OF " +
                                                   "RELATIONSHIP?)", "Show the schema of the data.", "READ"} ),
                equalTo( new Object[]{ "db.schema.nodeTypeProperties",
                        "db.schema.nodeTypeProperties() :: (nodeType :: STRING?, nodeLabels :: LIST? OF STRING?, propertyName :: STRING?, " +
                                "propertyTypes :: LIST? OF STRING?, mandatory :: BOOLEAN?)",
                        "Show the derived property schema of the nodes in tabular form.", "READ"} ),
                equalTo( new Object[]{ "db.schema.relTypeProperties", "db.schema.relTypeProperties() :: (relType :: STRING?, " +
                                "propertyName :: STRING?, propertyTypes :: LIST? OF STRING?, mandatory :: BOOLEAN?)",
                        "Show the derived property schema of the relationships in tabular form.", "READ"} ),
                equalTo( new Object[]{"db.relationshipTypes", "db.relationshipTypes() :: (relationshipType :: " +
                                                              "STRING?)",
                        "List all relationship types in the database.", "READ"} ),
                equalTo( new Object[]{"dbms.procedures", "dbms.procedures() :: (name :: STRING?, signature :: " +
                                                         "STRING?, description :: STRING?, mode :: STRING?)",
                        "List all procedures in the DBMS.", "DBMS"} ),
                equalTo( new Object[]{"dbms.functions", "dbms.functions() :: (name :: STRING?, signature :: " +
                                                        "STRING?, description :: STRING?)",
                        "List all user functions in the DBMS.", "DBMS"} ),
                equalTo( new Object[]{"dbms.components", "dbms.components() :: (name :: STRING?, versions :: LIST? OF" +
                                                         " STRING?, edition :: STRING?)",
                        "List DBMS components and their versions.", "READ"} ),
                equalTo( new Object[]{"dbms.queryJmx", "dbms.queryJmx(query :: STRING?) :: (name :: STRING?, " +
                                                       "description :: STRING?, attributes :: MAP?)",
                        "Query JMX management data by domain and name." +
                        " For instance, \"org.neo4j:*\"", "READ"} ),
                equalTo( new Object[]{"db.createLabel", "db.createLabel(newLabel :: STRING?) :: VOID", "Create a label", "WRITE"
                } ),
                equalTo( new Object[]{"db.createProperty", "db.createProperty(newProperty :: STRING?) :: VOID",
                        "Create a Property", "WRITE"
                } ),
                equalTo( new Object[]{"db.createRelationshipType",
                        "db.createRelationshipType(newRelationshipType :: STRING?) :: VOID",
                        "Create a RelationshipType", "WRITE"
                } ),
                equalTo( new Object[]{"db.index.explicit.searchNodes",
                        "db.index.explicit.searchNodes(indexName :: STRING?, query :: ANY?) :: (node :: NODE?, weight :: FLOAT?)",
                        "Search nodes in explicit index. Replaces `START n=node:nodes('key:foo*')`", "READ"
                } ),
                equalTo( new Object[]{"db.index.explicit.seekNodes",
                        "db.index.explicit.seekNodes(indexName :: STRING?, key :: STRING?, value :: ANY?) :: (node :: " +
                        "NODE?)",
                        "Get node from explicit index. Replaces `START n=node:nodes(key = 'A')`", "READ"
                } ),
                equalTo( new Object[]{"db.index.explicit.searchRelationships",
                        "db.index.explicit.searchRelationships(indexName :: STRING?, query :: ANY?) :: (relationship :: " +
                        "RELATIONSHIP?, weight :: FLOAT?)",
                        "Search relationship in explicit index. Replaces `START r=relationship:relIndex('key:foo*')`", "READ"
                } ),
                equalTo( new Object[]{ "db.index.explicit.auto.searchNodes",
                        "db.index.explicit.auto.searchNodes(query :: ANY?) :: (node :: NODE?, weight :: FLOAT?)",
                        "Search nodes in explicit automatic index. Replaces `START n=node:node_auto_index('key:foo*')`", "READ"} ),
                equalTo( new Object[]{ "db.index.explicit.auto.seekNodes",
                        "db.index.explicit.auto.seekNodes(key :: STRING?, value :: ANY?) :: (node :: NODE?)",
                        "Get node from explicit automatic index. Replaces `START n=node:node_auto_index(key = 'A')`", "READ"} ),
                equalTo( new Object[]{ "db.index.explicit.auto.searchRelationships",
                        "db.index.explicit.auto.searchRelationships(query :: ANY?) :: (relationship :: RELATIONSHIP?, weight :: FLOAT?)",
                        "Search relationship in explicit automatic index. Replaces `START r=relationship:relationship_auto_index('key:foo*')`", "READ"} ),
                equalTo( new Object[]{ "db.index.explicit.auto.seekRelationships",
                        "db.index.explicit.auto.seekRelationships(key :: STRING?, value :: ANY?) :: " +
                        "(relationship :: RELATIONSHIP?)",
                        "Get relationship from explicit automatic index. Replaces `START r=relationship:relationship_auto_index(key = 'A')`", "READ"} ),
                equalTo( new Object[]{ "db.index.explicit.addNode",
                        "db.index.explicit.addNode(indexName :: STRING?, node :: NODE?, key :: STRING?, value :: ANY?) :: (success :: BOOLEAN?)",
                        "Add a node to an explicit index based on a specified key and value", "WRITE"} ),
                equalTo( new Object[]{ "db.index.explicit.addRelationship",
                        "db.index.explicit.addRelationship(indexName :: STRING?, relationship :: RELATIONSHIP?, key :: STRING?, value :: ANY?) :: " +
                        "(success :: BOOLEAN?)",
                        "Add a relationship to an explicit index based on a specified key and value", "WRITE"} ),
                equalTo( new Object[]{ "db.index.explicit.removeNode",
                        "db.index.explicit.removeNode(indexName :: STRING?, node :: NODE?, " +
                        "key =  <[9895b15e-8693-4a21-a58b-4b7b87e09b8e]>  :: STRING?) :: (success :: BOOLEAN?)",
                        "Remove a node from an explicit index with an optional key", "WRITE"} ),
                equalTo( new Object[]{ "db.index.explicit.removeRelationship",
                        "db.index.explicit.removeRelationship(indexName :: STRING?, relationship :: RELATIONSHIP?, " +
                        "key =  <[9895b15e-8693-4a21-a58b-4b7b87e09b8e]>  :: STRING?) :: (success :: BOOLEAN?)",
                        "Remove a relationship from an explicit index with an optional key", "WRITE"} ),
                equalTo( new Object[]{ "db.index.explicit.drop",
                        "db.index.explicit.drop(indexName :: STRING?) :: " +
                        "(type :: STRING?, name :: STRING?, config :: MAP?)",
                        "Remove an explicit index - YIELD type,name,config", "WRITE"} ),
                equalTo( new Object[]{ "db.index.explicit.forNodes",
                        "db.index.explicit.forNodes(indexName :: STRING?, config = {} :: MAP?) :: " +
                        "(type :: STRING?, name :: STRING?, config :: MAP?)",
                        "Get or create a node explicit index - YIELD type,name,config", "WRITE"} ),
                equalTo( new Object[]{ "db.index.explicit.forRelationships",
                        "db.index.explicit.forRelationships(indexName :: STRING?, config = {} :: MAP?) :: " +
                        "(type :: STRING?, name :: STRING?, config :: MAP?)",
                        "Get or create a relationship explicit index - YIELD type,name,config", "WRITE"} ),
                equalTo( new Object[]{ "db.index.explicit.existsForNodes",
                        "db.index.explicit.existsForNodes(indexName :: STRING?) :: (success :: BOOLEAN?)",
                        "Check if a node explicit index exists", "READ"} ),
                equalTo( new Object[]{ "db.index.explicit.existsForRelationships",
                        "db.index.explicit.existsForRelationships(indexName :: STRING?) :: (success :: BOOLEAN?)",
                        "Check if a relationship explicit index exists", "DEFAULT"} ),
                equalTo( new Object[]{ "db.index.explicit.list",
                        "db.index.explicit.list() :: (type :: STRING?, name :: STRING?, config :: MAP?)",
                        "List all explicit indexes - YIELD type,name,config", "READ"} ),

                equalTo( new Object[]{"db.index.explicit.seekRelationships",
                        "db.index.explicit.seekRelationships(indexName :: STRING?, key :: STRING?, value :: ANY?) :: " +
                        "(relationship :: RELATIONSHIP?)",
                        "Get relationship from explicit index. Replaces `START r=relationship:relIndex(key = 'A')`", "READ"
                } ),
                equalTo( new Object[]{"db.index.explicit.searchRelationshipsBetween",
                        "db.index.explicit.searchRelationshipsBetween(indexName :: STRING?, in :: NODE?, out :: NODE?, query :: ANY?) :: " +
                                "(relationship :: RELATIONSHIP?, weight :: FLOAT?)",
                        "Search relationship in explicit index, starting at the node 'in' and ending at 'out'.", "READ"
                } ),
                equalTo( new Object[]{"db.index.explicit.searchRelationshipsIn",
                        "db.index.explicit.searchRelationshipsIn(indexName :: STRING?, in :: NODE?, query :: ANY?) :: " +
                                "(relationship :: RELATIONSHIP?, weight :: FLOAT?)",
                        "Search relationship in explicit index, starting at the node 'in'.", "READ"
                } ),
                equalTo( new Object[]{"db.index.explicit.searchRelationshipsOut",
                        "db.index.explicit.searchRelationshipsOut(indexName :: STRING?, out :: NODE?, query :: ANY?) :: " +
                                "(relationship :: RELATIONSHIP?, weight :: FLOAT?)",
                        "Search relationship in explicit index, ending at the node 'out'.", "READ"
                } ),
                equalTo( new Object[]{"dbms.clearQueryCaches",
                        "dbms.clearQueryCaches() :: (value :: STRING?)",
                        "Clears all query caches.", "DBMS"
                } ),
                equalTo( new Object[]{"db.createIndex",
                        "db.createIndex(index :: STRING?, providerName :: STRING?) :: (index :: STRING?, providerName :: STRING?, status :: STRING?)",
                        "Create a schema index with specified index provider (for example: CALL db.createIndex(\":Person(name)\", \"lucene+native-2.0\")) - " +
                                "YIELD index, providerName, status",
                        "SCHEMA"
                } ),
                equalTo( new Object[]{"db.createUniquePropertyConstraint",
                        "db.createUniquePropertyConstraint(index :: STRING?, providerName :: STRING?) :: " +
                                "(index :: STRING?, providerName :: STRING?, status :: STRING?)",
                        "Create a unique property constraint with index backed by specified index provider " +
                                "(for example: CALL db.createUniquePropertyConstraint(\":Person(name)\", \"lucene+native-2.0\")) - " +
                                "YIELD index, providerName, status", "SCHEMA"} )
        ) );
        commit();
    }

    @Test
    public void failWhenCallingNonExistingProcedures()
    {
        try
        {
            // When
            dbmsOperations().procedureCallDbms( procedureName( "dbms", "iDoNotExist" ), new Object[0],
                    AnonymousContext.none().authorize( s -> -1 ), resourceTracker );
            fail( "This should never get here" );
        }
        catch ( Exception e )
        {
            // Then
            assertThat( e.getClass(), equalTo( ProcedureException.class ) );
        }
    }

    @Test
    public void listAllComponents() throws Throwable
    {
        // Given a running database

        // When
        RawIterator<Object[],ProcedureException> stream = procs()
                .procedureCallRead(procs().procedureGet( procedureName( "dbms", "components" ) ).id(),
                        new Object[0] );

        // Then
        assertThat( asList( stream ), contains( equalTo( new Object[]{"Neo4j Kernel",
                singletonList( Version.getNeo4jVersion() ), "community"} ) ) );

        commit();
    }

    @Test
    public void listAllIndexes() throws Throwable
    {
        // Given
        Transaction transaction = newTransaction( AUTH_DISABLED );
        int labelId1 = transaction.tokenWrite().labelGetOrCreateForName( "Person" );
        int labelId2 = transaction.tokenWrite().labelGetOrCreateForName( "Age" );
        int propertyKeyId1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "foo" );
        int propertyKeyId2 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "bar" );
        transaction.schemaWrite().indexCreate( forLabel( labelId1, propertyKeyId1 ), null );
        transaction.schemaWrite().uniquePropertyConstraintCreate( forLabel( labelId2, propertyKeyId1 ) );
        transaction.schemaWrite().indexCreate( forLabel( labelId1, propertyKeyId1, propertyKeyId2 ), null );
        commit();

        //let indexes come online
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 2, MINUTES );
            tx.success();
        }

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "indexes" ) ).id(),
                        new Object[0] );

        Set<Object[]> result = new HashSet<>();
        while ( stream.hasNext() )
        {
            result.add( stream.next() );
        }

        // Then
        Map<String,String> providerDescriptionMap = MapUtil.stringMap(
                "key", DelegatingIndexProvider.DESCRIPTOR.getKey(),
                "version", DelegatingIndexProvider.DESCRIPTOR.getVersion() );
        assertThat( result, containsInAnyOrder(
                new Object[]{"INDEX ON :Age(foo)", "Age", singletonList( "foo" ), "ONLINE", "node_unique_property", providerDescriptionMap, ""},
                new Object[]{"INDEX ON :Person(foo)", "Person", singletonList( "foo" ), "ONLINE", "node_label_property", providerDescriptionMap, ""},
                new Object[]{"INDEX ON :Person(foo, bar)", "Person", Arrays.asList( "foo", "bar" ), "ONLINE", "node_label_property", providerDescriptionMap, ""}
        ) );
        commit();
    }

    @Test
    public void listAllIndexesWithFailedIndex() throws Throwable
    {
        // Given
        Transaction transaction = newTransaction( AUTH_DISABLED );
        int failedLabel = transaction.tokenWrite().labelGetOrCreateForName( "Fail" );
        int propertyKeyId1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "foo" );
        failNextIndexPopulation.set( true );
        transaction.schemaWrite().indexCreate( forLabel( failedLabel, propertyKeyId1 ) );
        commit();

        //let indexes come online
        try ( org.neo4j.graphdb.Transaction ignored = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 2, MINUTES );
            fail( "Expected to fail when awaiting for index to come online" );
        }
        catch ( IllegalStateException e )
        {
            // expected
        }

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "indexes" ) ).id(),
                        new Object[0] );
        assertTrue( stream.hasNext() );
        Object[] result = stream.next();
        assertFalse( stream.hasNext() );

        // Then
        assertEquals( "INDEX ON :Fail(foo)", result[0] );
        assertEquals( "Fail", result[1] );
        assertEquals( Collections.singletonList( "foo" ), result[2] );
        assertEquals( "FAILED", result[3] );
        assertEquals( "node_label_property", result[4] );
        Map<String,String> providerDescriptionMap = MapUtil.stringMap(
                "key", DelegatingIndexProvider.DESCRIPTOR.getKey(),
                "version", DelegatingIndexProvider.DESCRIPTOR.getVersion() );
        assertEquals( providerDescriptionMap, result[5] );
        assertThat( (String) result[6], containsString( "java.lang.IllegalStateException: Was told to fail" ) );

        commit();
    }

    private static class DelegatingIndexProvider extends IndexProvider
    {
        private static final IndexProvider.Descriptor DESCRIPTOR = new IndexProvider.Descriptor( "delegating", "1.0" );
        private final IndexProvider delegate;

        DelegatingIndexProvider( IndexProvider delegate )
        {
            super( DESCRIPTOR, 100, descriptor -> delegate.directoryStructure() );
            this.delegate = delegate;
        }

        @Override
        public IndexPopulator getPopulator( long indexId, SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
        {
            return delegate.getPopulator( indexId, descriptor, samplingConfig );
        }

        @Override
        public IndexAccessor getOnlineAccessor( long indexId, SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
        {
            return delegate.getOnlineAccessor( indexId, descriptor, samplingConfig );
        }

        @Override
        public String getPopulationFailure( long indexId, SchemaIndexDescriptor descriptor ) throws IllegalStateException
        {
            return delegate.getPopulationFailure( indexId, descriptor );
        }

        @Override
        public InternalIndexState getInitialState( long indexId, SchemaIndexDescriptor descriptor )
        {
            return delegate.getInitialState( indexId, descriptor );
        }

        @Override
        public IndexCapability getCapability( SchemaIndexDescriptor schemaIndexDescriptor )
        {
            return delegate.getCapability( schemaIndexDescriptor );
        }

        @Override
        public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
        {
            return delegate.storeMigrationParticipant( fs, pageCache );
        }
    }

    private class DelegatingIndexPopulator implements IndexPopulator
    {
        private final IndexPopulator delegate;

        DelegatingIndexPopulator( IndexPopulator delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public void create() throws IOException
        {
            delegate.create();
        }

        @Override
        public void drop() throws IOException
        {
            delegate.drop();
        }

        @Override
        public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException, IOException
        {
            delegate.add( updates );
        }

        @Override
        public void verifyDeferredConstraints( PropertyAccessor propertyAccessor ) throws IndexEntryConflictException, IOException
        {
            delegate.verifyDeferredConstraints( propertyAccessor );
        }

        @Override
        public IndexUpdater newPopulatingUpdater( PropertyAccessor accessor )
        {
            return delegate.newPopulatingUpdater( accessor );
        }

        @Override
        public void close( boolean populationCompletedSuccessfully ) throws IOException
        {
            delegate.close( populationCompletedSuccessfully );
        }

        @Override
        public void markAsFailed( String failure ) throws IOException
        {
            delegate.markAsFailed( failure );
        }

        @Override
        public void includeSample( IndexEntryUpdate<?> update )
        {
            delegate.includeSample( update );
        }

        @Override
        public IndexSample sampleResult()
        {
            return delegate.sampleResult();
        }
    }

    @Override
    protected TestGraphDatabaseFactory createGraphDatabaseFactory()
    {
        TestGraphDatabaseFactory graphDatabaseFactory = super.createGraphDatabaseFactory();
        InMemoryIndexProvider delegate = new InMemoryIndexProvider( 100 );
        InMemoryIndexProviderFactory failableIndexProviderFactory = new InMemoryIndexProviderFactory( new DelegatingIndexProvider( delegate )
        {
            @Override
            public IndexPopulator getPopulator( long indexId, SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
            {
                IndexPopulator populator = super.getPopulator( indexId, descriptor, samplingConfig );
                return new DelegatingIndexPopulator( populator )
                {
                    @Override
                    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException, IOException
                    {
                        if ( failNextIndexPopulation.get() )
                        {
                            failNextIndexPopulation.set( false );
                            throw new IllegalStateException( "Was told to fail" );
                        }
                        super.add( updates );
                    }
                };
            }
        } );
        graphDatabaseFactory.addKernelExtension( failableIndexProviderFactory );
        return graphDatabaseFactory;
    }
}
