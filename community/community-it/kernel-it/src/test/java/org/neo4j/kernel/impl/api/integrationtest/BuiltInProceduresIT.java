/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.RawIterator;
import org.neo4j.cypher.internal.StringCacheMonitor;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.StubResourceManager;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.monitoring.Monitors;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsEqual.equalTo;
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
        assertThat( asList( stream ), contains( equalTo( new Object[]{"MyLabel", 1L} ) ) );
    }

    @Test
    public void listPropertyKeys() throws Throwable
    {
        // Given
        TokenWrite ops = tokenWriteInNewTransaction();
        ops.propertyKeyGetOrCreateForName( "MyProp" );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "propertyKeys" ) ).id(), new Object[0] );

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
        transaction.dataWrite().relationshipCreate( startNodeId, relType, endNodeId );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "relationshipTypes" ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), contains( equalTo( new Object[]{"MyRelType", 1L} ) ) );
    }

    @Test
    public void listProcedures() throws Throwable
    {
        // When
        ProcedureHandle procedures = procs().procedureGet( procedureName( "dbms", "procedures" ) );
        RawIterator<Object[],ProcedureException> stream = procs().procedureCallRead( procedures.id(), new Object[0] );

        // Then
        //noinspection unchecked
        assertThat( asList( stream ), containsInAnyOrder(
                proc( "dbms.listConfig", "(searchString =  :: STRING?) :: (name :: STRING?, description :: STRING?, value :: STRING?, dynamic :: BOOLEAN?)",
                        "List the currently active config of Neo4j.", "DBMS" ),
                proc( "db.constraints", "() :: (description :: STRING?)", "List all constraints in the database.", "READ" ),
                proc( "db.indexes", "() :: (description :: STRING?, indexName :: STRING?, tokenNames :: LIST? OF STRING?, properties :: " +
                                "LIST? OF STRING?, state :: STRING?, type :: STRING?, progress :: FLOAT?, provider :: MAP?, id :: INTEGER?, " +
                                "failureMessage :: STRING?)",
                        "List all indexes in the database.", "READ" ),
                proc( "db.awaitIndex", "(index :: STRING?, timeOutSeconds = 300 :: INTEGER?) :: VOID",
                        "Wait for an index to come online (for example: CALL db.awaitIndex(\":Person(name)\")).", "READ" ),
                proc( "db.awaitIndexes", "(timeOutSeconds = 300 :: INTEGER?) :: VOID",
                        "Wait for all indexes to come online (for example: CALL db.awaitIndexes(\"500\")).", "READ" ),
                proc( "db.resampleIndex", "(index :: STRING?) :: VOID",
                        "Schedule resampling of an index (for example: CALL db.resampleIndex(\":Person(name)\")).", "READ" ),
                proc( "db.resampleOutdatedIndexes", "() :: VOID", "Schedule resampling of all outdated indexes.", "READ" ),
                proc( "db.propertyKeys", "() :: (propertyKey :: STRING?)", "List all property keys in the database.", "READ" ),
                proc( "db.labels", "() :: (label :: STRING?, nodeCount :: INTEGER?)", "List all labels in the database and their total count.", "READ" ),
                proc( "db.schema", "() :: (nodes :: LIST? OF NODE?, relationships :: LIST? " + "OF " + "RELATIONSHIP?)",
                        "Show the schema of the data.", "READ" ),
                proc( "db.schema.visualization","() :: (nodes :: LIST? OF NODE?, relationships :: LIST? OF RELATIONSHIP?)",
                        "Visualize the schema of the data. Replaces db.schema.", "READ" ),
                proc( "db.schema.nodeTypeProperties",
                        "() :: (nodeType :: STRING?, nodeLabels :: LIST? OF STRING?, propertyName :: STRING?, " +
                                "propertyTypes :: LIST? OF STRING?, mandatory :: BOOLEAN?)",
                        "Show the derived property schema of the nodes in tabular form.", "READ" ),
                proc( "db.schema.relTypeProperties", "() :: (relType :: STRING?, " +
                                "propertyName :: STRING?, propertyTypes :: LIST? OF STRING?, mandatory :: BOOLEAN?)",
                        "Show the derived property schema of the relationships in tabular form.", "READ" ),
                proc( "db.relationshipTypes", "() :: (relationshipType :: STRING?, relationshipCount :: INTEGER?)",
                        "List all relationship types in the database and their total count.", "READ" ),
                proc( "dbms.procedures", "() :: (name :: STRING?, signature :: " + "STRING?, description :: STRING?, mode :: STRING?)",
                        "List all procedures in the DBMS.", "DBMS" ),
                proc( "dbms.functions", "() :: (name :: STRING?, signature :: " + "STRING?, description :: STRING?)",
                        "List all user functions in the DBMS.", "DBMS" ),
                proc( "dbms.components", "() :: (name :: STRING?, versions :: LIST? OF" + " STRING?, edition :: STRING?)",
                        "List DBMS components and their versions.", "DBMS" ),
                proc( "dbms.queryJmx", "(query :: STRING?) :: (name :: STRING?, " + "description :: STRING?, attributes :: MAP?)",
                        "Query JMX management data by domain and name." + " For instance, \"org.neo4j:*\"", "DBMS" ),
                proc( "db.createLabel", "(newLabel :: STRING?) :: VOID", "Create a label", "WRITE" ),
                proc( "db.createProperty", "(newProperty :: STRING?) :: VOID", "Create a Property", "WRITE" ),
                proc( "db.createRelationshipType", "(newRelationshipType :: STRING?) :: VOID", "Create a RelationshipType", "WRITE" ),
                proc( "dbms.clearQueryCaches", "() :: (value :: STRING?)", "Clears all query caches.", "DBMS" ),
                proc( "db.createIndex", "(index :: STRING?, providerName :: STRING?) :: (index :: STRING?, providerName :: STRING?, status :: STRING?)",
                        "Create a schema index with specified index provider (for example: CALL db.createIndex(\":Person(name)\", \"lucene+native-2.0\")) - " +
                                "YIELD index, providerName, status", "SCHEMA" ),
                proc( "db.createUniquePropertyConstraint", "(index :: STRING?, providerName :: STRING?) :: " +
                                "(index :: STRING?, providerName :: STRING?, status :: STRING?)",
                        "Create a unique property constraint with index backed by specified index provider " +
                                "(for example: CALL db.createUniquePropertyConstraint(\":Person(name)\", \"lucene+native-2.0\")) - " +
                                "YIELD index, providerName, status", "SCHEMA" ),
                proc( "db.index.fulltext.awaitEventuallyConsistentIndexRefresh", "() :: VOID",
                        "Wait for the updates from recently committed transactions to be applied to any eventually-consistent fulltext indexes.", "READ" ),
                proc( "db.index.fulltext.awaitIndex", "(index :: STRING?, timeOutSeconds = 300 :: INTEGER?) :: VOID",
                        "Similar to db.awaitIndex(index, timeout), except instead of an index pattern, the index is specified by name. " +
                                "The name can be quoted by backticks, if necessary.", "READ" ),
                proc( "db.index.fulltext.createNodeIndex", "(indexName :: STRING?, labels :: LIST? OF STRING?, propertyNames :: LIST? OF STRING?, " +
                        "config = {} :: MAP?) :: VOID", startsWith( "Create a node fulltext index for the given labels and properties." ), "SCHEMA" ),
                proc( "db.index.fulltext.createRelationshipIndex",
                        "(indexName :: STRING?, relationshipTypes :: LIST? OF STRING?, propertyNames :: LIST? OF STRING?, config = {} :: MAP?) :: VOID",
                        startsWith( "Create a relationship fulltext index for the given relationship types and properties." ), "SCHEMA" ),
                proc( "db.index.fulltext.drop", "(indexName :: STRING?) :: VOID", "Drop the specified index.", "SCHEMA" ),
                proc( "db.index.fulltext.listAvailableAnalyzers", "() :: (analyzer :: STRING?, description :: STRING?)",
                        "List the available analyzers that the fulltext indexes can be configured with.", "READ" ),
                proc( "db.index.fulltext.queryNodes", "(indexName :: STRING?, queryString :: STRING?) :: (node :: NODE?, score :: FLOAT?)",
                        "Query the given fulltext index. Returns the matching nodes and their lucene query score, ordered by score.", "READ"),
                proc( "db.index.fulltext.queryRelationships", "(indexName :: STRING?, queryString :: STRING?) :: (relationship :: RELATIONSHIP?, " +
                        "score :: FLOAT?)", "Query the given fulltext index. Returns the matching relationships and their lucene query score, ordered by " +
                        "score.", "READ" ),
                proc( "db.prepareForReplanning", "(timeOutSeconds = 300 :: INTEGER?) :: VOID",
                        "Triggers an index resample and waits for it to complete, and after that clears query caches." +
                        " After this procedure has finished queries will be planned using the latest database " +
                        "statistics.",
                        "READ" ),
                proc( "db.stats.retrieve", "(section :: STRING?) :: (section :: STRING?, data :: MAP?)",
                      "Retrieve statistical data about the current database. Valid sections are 'GRAPH COUNTS', 'TOKENS', 'QUERIES'", "READ" ),
                proc( "db.stats.retrieveAllAnonymized", "(graphToken :: STRING?) :: (section :: STRING?, data :: MAP?)",
                      "Retrieve all available statistical data about the current database, in an anonymized form.", "READ" ),
                proc( "db.stats.status", "() :: (section :: STRING?, status :: STRING?, data :: MAP?)",
                      "Retrieve the status of all available collector daemons, for this database.", "READ" ),
                proc( "db.stats.collect", "(section :: STRING?) :: (section :: STRING?, success :: BOOLEAN?, message :: STRING?)",
                      "Start data collection of a given data section. Valid sections are 'QUERIES'", "READ" ),
                proc( "db.stats.stop", "(section :: STRING?) :: (section :: STRING?, success :: BOOLEAN?, message :: STRING?)",
                      "Stop data collection of a given data section. Valid sections are 'QUERIES'", "READ" ),
                proc( "db.stats.clear", "(section :: STRING?) :: (section :: STRING?, success :: BOOLEAN?, message :: STRING?)",
                      "Clear collected data of a given data section. Valid sections are 'QUERIES'", "READ" )
        ) );
        commit();
    }

    @Test
    public void failWhenCallingNonExistingProcedures()
    {
        try
        {
            dbmsOperations().procedureCallDbms( procedureName( "dbms", "iDoNotExist" ), new Object[0], dependencyResolver,
                    AnonymousContext.none().authorize( s -> -1, GraphDatabaseSettings.DEFAULT_DATABASE_NAME ), resourceTracker );
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
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "dbms", "components" ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), contains( equalTo( new Object[]{"Neo4j Kernel", singletonList( Version.getNeo4jVersion() ), "community"} ) ) );

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
        LabelSchemaDescriptor personFooDescriptor = forLabel( labelId1, propertyKeyId1 );
        LabelSchemaDescriptor ageFooDescriptor = forLabel( labelId2, propertyKeyId1 );
        LabelSchemaDescriptor personFooBarDescriptor = forLabel( labelId1, propertyKeyId1, propertyKeyId2 );
        transaction.schemaWrite().indexCreate( personFooDescriptor );
        transaction.schemaWrite().uniquePropertyConstraintCreate( ageFooDescriptor );
        transaction.schemaWrite().indexCreate( personFooBarDescriptor );
        commit();

        //let indexes come online
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 2, MINUTES );
            tx.success();
        }

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "indexes" ) ).id(), new Object[0] );

        Set<Object[]> result = new HashSet<>();
        while ( stream.hasNext() )
        {
            result.add( stream.next() );
        }

        // Then
        IndexProviderMap indexProviderMap = db.getDependencyResolver().resolveDependency( IndexProviderMap.class );
        IndexingService indexingService = db.getDependencyResolver().resolveDependency( IndexingService.class );
        IndexProvider provider = indexProviderMap.getDefaultProvider();
        Map<String,String> pdm = MapUtil.stringMap( // Provider Descriptor Map.
                "key", provider.getProviderDescriptor().getKey(), "version", provider.getProviderDescriptor().getVersion() );
        assertThat( result, containsInAnyOrder(
                new Object[]{"INDEX ON :Age(foo)", "index_1", singletonList( "Age" ), singletonList( "foo" ), "ONLINE",
                        "node_unique_property", 100D, pdm, indexingService.getIndexId( ageFooDescriptor ), ""},
                new Object[]{"INDEX ON :Person(foo)", "Unnamed index", singletonList( "Person" ),
                        singletonList( "foo" ), "ONLINE", "node_label_property", 100D, pdm, indexingService.getIndexId( personFooDescriptor ), ""},
                new Object[]{"INDEX ON :Person(foo, bar)", "Unnamed index", singletonList( "Person" ),
                        Arrays.asList( "foo", "bar" ), "ONLINE", "node_label_property", 100D, pdm, indexingService.getIndexId( personFooBarDescriptor ), ""}
        ) );
        commit();
    }

    @Test
    public void prepareForReplanningShouldEmptyQueryCache()
    {
        // Given, something is cached
        db.execute( "MATCH (n) RETURN n" );

        ReplanMonitor monitor = replanMonitor();

        // When
        db.execute( "CALL db.prepareForReplanning()" );

        // Then, the initial query and the procedure call should now have been cleared
        assertThat( monitor.numberOfFlushedItems(), equalTo( 2L ) );
    }

    @Test
    public void prepareForReplanningShouldTriggerIndexesSampling()
    {
        // Given
        ReplanMonitor monitor = replanMonitor();

        // When
        db.execute( "CALL db.prepareForReplanning()" );

        // Then
        assertThat( monitor.samplingMode(), equalTo( IndexSamplingMode.TRIGGER_REBUILD_UPDATED ) );
    }

    private ReplanMonitor replanMonitor()
    {
        Monitors monitors =
                dependencyResolver.resolveDependency( Monitors.class, DependencyResolver.SelectionStrategy.FIRST );

        ReplanMonitor monitorListener = new ReplanMonitor();
        monitors.addMonitorListener( monitorListener );
        return monitorListener;
    }

    private static class ReplanMonitor extends IndexingService.MonitorAdapter implements StringCacheMonitor
    {
        private long numberOfFlushedItems = -1L;
        private IndexSamplingMode samplingMode;

        @Override
        public void cacheFlushDetected( long sizeBeforeFlush )
        {
            numberOfFlushedItems = sizeBeforeFlush;
        }

        @Override
        public void indexSamplingTriggered( IndexSamplingMode mode )
        {
            samplingMode = mode;
        }

        long numberOfFlushedItems()
        {
            return numberOfFlushedItems;
        }

        IndexSamplingMode samplingMode()
        {
            return samplingMode;
        }

        @Override
        public void cacheHit( Pair<String,scala.collection.immutable.Map<String,Class<?>>> key )
        {
        }

        @Override
        public void cacheMiss( Pair<String,scala.collection.immutable.Map<String,Class<?>>> key )
        {
        }

        @Override
        public void cacheDiscard( Pair<String,scala.collection.immutable.Map<String,Class<?>>> key, String userKey,
                int secondsSinceReplan )
        {
        }

        @Override
        public void cacheRecompile( Pair<String,scala.collection.immutable.Map<String,Class<?>>> key )
        {
        }
    }

    private Matcher<Object[]> proc( String procName, String procSignature, String description, String mode )
    {
        return equalTo( new Object[]{procName, procName + procSignature, description, mode} );
    }

    @SuppressWarnings( {"unchecked", "TypeParameterExplicitlyExtendsObject"} )
    private Matcher<Object[]> proc( String procName, String procSignature, Matcher<String> description, String mode )
    {
        Matcher<Object> desc = (Matcher<Object>) (Matcher<? extends Object>) description;
        Matcher<Object>[] matchers =
                new Matcher[]{equalTo( procName ), equalTo( procName + procSignature ), desc, equalTo( mode )};
        return arrayContaining( matchers );
    }
}
