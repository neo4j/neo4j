/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.impl.lucene.legacy;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.QueryContext;
import org.neo4j.index.lucene.ValueContext;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class LegacyIndexFieldsRestorerIT
{

    private static final String INT_PROPERTY_KEY = "intProperty";
    private static final String STRING_PROPERTY_KEY = "stringProperty";
    private static final int STRING_VALUE_SHIFT = 100;

    private final TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    private final CleanupRule cleanupRule = new CleanupRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( cleanupRule ).around( directory );

    private final RelationshipType relationshipType = RelationshipType.withName( "test" );

    @After
    public void tearDown()
    {
        LegacyIndexFieldsRestorer.SKIP_LEGACY_INDEX_FIELDS_MIGRATION = false;
    }

    @Test
    public void skipFieldRestoreationMigrationWhenRequested() throws IOException
    {
        LegacyIndexFieldsRestorer.SKIP_LEGACY_INDEX_FIELDS_MIGRATION = true;
        LegacyIndexFieldsRestorer fieldsRestorer =
                new LegacyIndexFieldsRestorer( null, null, NullLogProvider.getInstance() );
        fieldsRestorer.restoreIndexSortFields( null, null, null, null );
    }

    @Test
    public void restoreNumericNodeIndex() throws IOException
    {
        File storeDir = directory.graphDbDir();
        File migrationDir = directory.directory( "migrationDir" );

        String indexName = "testNumericNodeIndex";
        int numberOfNodes = 100;
        GraphDatabaseService databaseService = createDatabase( storeDir );
        cleanupRule.add( databaseService );
        createAndPopulateNumericNodeIndex( databaseService, indexName, numberOfNodes );
        databaseService.shutdown();

        restoreIndexes( storeDir, migrationDir, SilentMigrationProgressMonitor.NO_OP_SECTION );

        GraphDatabaseService db = createDatabase( storeDir );
        cleanupRule.add( db );
        verifySortingOnNumericProperty( db, getNodeIndexSupplier( db, indexName ), numberOfNodes );
        db.shutdown();
    }

    @Test
    public void restoreStringNodeIndex() throws IOException
    {
        File storeDir = directory.graphDbDir();
        File migrationDir = directory.directory( "migrationDir" );

        String indexName = "testStringNodeIndex";
        int numberOfNodes = 100;
        GraphDatabaseService databaseService = createDatabase( storeDir );
        cleanupRule.add( databaseService );
        createAndPopulateStringNodeIndex( databaseService, indexName, numberOfNodes );
        databaseService.shutdown();

        restoreIndexes( storeDir, migrationDir, SilentMigrationProgressMonitor.NO_OP_SECTION );

        GraphDatabaseService db = createDatabase( storeDir );
        cleanupRule.add( db );
        verifySortingOnStringProperty( db, getNodeIndexSupplier( db, indexName ), numberOfNodes );
        db.shutdown();
    }

    @Test
    public void restoreNumericRelationshipIndex() throws IOException
    {
        File storeDir = directory.graphDbDir();
        File migrationDir = directory.directory( "migrationDir" );

        String indexName = "testRelationshipIndex";
        int numberOfRelationships = 100;
        GraphDatabaseService databaseService = createDatabase( storeDir );
        cleanupRule.add( databaseService );
        createAndPopulateNumericRelationshipIndex( databaseService, indexName, numberOfRelationships );
        databaseService.shutdown();

        restoreIndexes( storeDir, migrationDir, SilentMigrationProgressMonitor.NO_OP_SECTION );

        GraphDatabaseService db = createDatabase( storeDir );
        cleanupRule.add( db );
        verifySortingOnNumericProperty( db, getRelationshipIndexSupplier( db, indexName ), numberOfRelationships );
        db.shutdown();
    }

    @Test
    public void restoreStringRelationshipIndex() throws IOException
    {
        File storeDir = directory.graphDbDir();
        File migrationDir = directory.directory( "migrationDir" );

        String indexName = "testStringRelationshipIndex";
        int numberOfRelationships = 100;
        GraphDatabaseService databaseService = createDatabase( storeDir );
        cleanupRule.add( databaseService );
        createAndPopulateStringRelationshipIndex( databaseService, indexName, numberOfRelationships );
        databaseService.shutdown();

        restoreIndexes( storeDir, migrationDir, SilentMigrationProgressMonitor.NO_OP_SECTION );

        GraphDatabaseService db = createDatabase( storeDir );
        cleanupRule.add( db );
        verifySortingOnStringProperty( db, getNodeIndexSupplier( db, indexName ), numberOfRelationships );
        db.shutdown();
    }

    @Test
    public void restoreCoupleOfNodeAndRelationshipIndexes() throws IOException
    {
        File storeDir = directory.graphDbDir();
        File migrationDir = directory.directory( "migrationDir" );

        String indexName = "testRelationshipIndex";
        String indexName2 = "testStringRelationshipIndex";
        String indexName3 = "testNumericNodeIndex";
        String indexName4 = "testStringNodeIndex";
        int numberOfEntries = 100;
        GraphDatabaseService databaseService = createDatabase( storeDir );
        cleanupRule.add( databaseService );
        createAndPopulateNumericRelationshipIndex( databaseService, indexName, numberOfEntries );
        createAndPopulateStringRelationshipIndex( databaseService, indexName2, numberOfEntries );
        createAndPopulateNumericNodeIndex( databaseService, indexName3, numberOfEntries );
        createAndPopulateStringNodeIndex( databaseService, indexName4, numberOfEntries );
        databaseService.shutdown();

        restoreIndexes( storeDir, migrationDir, SilentMigrationProgressMonitor.NO_OP_SECTION );

        GraphDatabaseService db = createDatabase( storeDir );
        cleanupRule.add( db );
        verifySortingOnNumericProperty( db, getRelationshipIndexSupplier( db, indexName ), numberOfEntries );
        verifySortingOnStringProperty( db, getRelationshipIndexSupplier( db, indexName2 ), numberOfEntries );
        verifySortingOnNumericProperty( db, getRelationshipIndexSupplier( db, indexName3 ), numberOfEntries );
        verifySortingOnStringProperty( db, getRelationshipIndexSupplier( db, indexName4 ), numberOfEntries );
        db.shutdown();
    }

    @Test
    public void restoreKeyDocIdNodeIndex() throws IOException
    {
        File storeDir = directory.graphDbDir();
        File migrationDir = directory.directory( "migrationDir" );

        String indexName = "testNumericNodeIndex";
        int numberOfNodes = 100;
        GraphDatabaseService databaseService = createDatabase( storeDir );
        cleanupRule.add( databaseService );
        createAndPopulateNumericNodeIndex( databaseService, indexName, numberOfNodes );
        databaseService.shutdown();

        restoreIndexes( storeDir, migrationDir, SilentMigrationProgressMonitor.NO_OP_SECTION );

        GraphDatabaseService db = createDatabase( storeDir );
        cleanupRule.add( db );

        verifySortingOnKeyDocId( db, getNodeIndexSupplier( db, indexName ) );
        db.shutdown();
    }

    @Test
    public void restoreEmptyIndexStore() throws IOException
    {
        File storeDir = directory.graphDbDir();
        File migrationDir = directory.directory( "migrationDir" );
        GraphDatabaseService databaseService = createDatabase( storeDir );
        cleanupRule.add( databaseService );
        databaseService.shutdown();

        DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        LegacyIndexFieldsRestorer fieldsRestorer =
                new LegacyIndexFieldsRestorer( Config.defaults(), fileSystem, NullLogProvider.getInstance() );
        fieldsRestorer.restoreIndexSortFields( storeDir, storeDir, migrationDir,
                SilentMigrationProgressMonitor.NO_OP_SECTION );

        assertThat( "Nothing to restore. Migration directory should be empty.", migrationDir.listFiles(),
                is( emptyArray() ) );
    }

    @Test
    public void reportProgressOnIndexMigration() throws IOException
    {
        File storeDir = directory.graphDbDir();
        File migrationDir = directory.directory( "migrationDir" );

        String indexName = "testRelationshipIndex";
        String indexName2 = "testStringRelationshipIndex";
        String indexName3 = "testNumericNodeIndex";
        String indexName4 = "testStringNodeIndex";
        int numberOfNodes = 100;
        GraphDatabaseService databaseService = createDatabase( storeDir );
        cleanupRule.add( databaseService );
        createAndPopulateNumericRelationshipIndex( databaseService, indexName, numberOfNodes );
        createAndPopulateStringRelationshipIndex( databaseService, indexName2, numberOfNodes );
        createAndPopulateNumericNodeIndex( databaseService, indexName3, numberOfNodes );
        createAndPopulateStringNodeIndex( databaseService, indexName4, numberOfNodes );
        databaseService.shutdown();

        TrackingProgressMonitor progressMonitor = new TrackingProgressMonitor();
        restoreIndexes( storeDir, migrationDir, progressMonitor );

        assertEquals( "Should be equal to total number of indexes.", 4,
                progressMonitor.getProgressInvocationCounter() );
        assertEquals( "Should be equal to total number of indexes.", 4, progressMonitor.getProgressSum() );
    }

    @Test
    public void restoreStringNodeIndexWithCustomConfig() throws IOException
    {
        File storeDir = directory.graphDbDir();
        File migrationDir = directory.directory( "migrationDir" );

        String indexName = "testStringNodeIndex";
        int numberOfNodes = 100;
        GraphDatabaseService databaseService = createDatabase( storeDir );
        cleanupRule.add( databaseService );

        Map<String,String> stringStringMap =
                MapUtil.stringMap( IndexManager.PROVIDER, "lucene", "analyzer", CustomAnalyzer.class.getName(),
                        "to_lower_case", "true" );
        Index<Node> nodeIndex = createNodeIndex( databaseService, indexName, stringStringMap );
        Stream<Node> nodeStream = Stream.generate( databaseService::createNode ).limit( numberOfNodes );
        try ( Transaction transaction = databaseService.beginTx() )
        {
            nodeStream.forEach( nodeStringPropertyPopulator( nodeIndex, getStringValueSupplier() ) );
            transaction.success();
        }
        databaseService.shutdown();

        restoreIndexes( storeDir, migrationDir, SilentMigrationProgressMonitor.NO_OP_SECTION );

        GraphDatabaseService db = createDatabase( storeDir );
        cleanupRule.add( db );
        verifySortingOnStringProperty( db, getNodeIndexSupplier( db, indexName ), numberOfNodes );
        db.shutdown();
    }

    private Supplier<Index<? extends PropertyContainer>> getNodeIndexSupplier( GraphDatabaseService db,
            String indexName )
    {
        return () -> db.index().forNodes( indexName );
    }

    private Supplier<Index<? extends PropertyContainer>> getRelationshipIndexSupplier( GraphDatabaseService db,
            String indexName )
    {
        return () -> db.index().forRelationships( indexName );
    }

    private void restoreIndexes( File storeDir, File migrationDir, MigrationProgressMonitor.Section progressMonitor )
            throws IOException
    {
        DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        LegacyIndexFieldsRestorer fieldsRestorer =
                new LegacyIndexFieldsRestorer( Config.defaults(), fileSystem, NullLogProvider.getInstance() );
        fieldsRestorer.restoreIndexSortFields( storeDir, storeDir, migrationDir, progressMonitor );
        moveMigratedIndexesToStore( storeDir, migrationDir, fileSystem );
    }

    private void createAndPopulateNumericNodeIndex( GraphDatabaseService databaseService, String indexName,
            int numberOfNodes )
    {
        Index<Node> nodeIndex = createNodeIndex( databaseService, indexName );
        Stream<Node> nodeStream = Stream.generate( databaseService::createNode ).limit( numberOfNodes );
        try ( Transaction transaction = databaseService.beginTx() )
        {
            nodeStream.forEach( nodeIntPropertyPopulator( nodeIndex, getIntegerValueSupplier() ) );
            transaction.success();
        }
    }

    private void createAndPopulateStringNodeIndex( GraphDatabaseService databaseService, String indexName,
            int numberOfNodes )
    {
        Index<Node> nodeIndex = createNodeIndex( databaseService, indexName );
        Stream<Node> nodeStream = Stream.generate( databaseService::createNode ).limit( numberOfNodes );
        try ( Transaction transaction = databaseService.beginTx() )
        {
            nodeStream.forEach( nodeStringPropertyPopulator( nodeIndex, getStringValueSupplier() ) );
            transaction.success();
        }
    }

    private void createAndPopulateNumericRelationshipIndex( GraphDatabaseService databaseService, String indexName,
            int numberOfRelationships )
    {
        Index<Relationship> index = createRelationshipIndex( databaseService, indexName );
        Stream<Relationship> relationshipStream = getRelationshipGenerator( databaseService, numberOfRelationships );
        try ( Transaction transaction = databaseService.beginTx() )
        {
            relationshipStream.forEach( relationshipIntPropertyPopulator( index, getIntegerValueSupplier() ) );
            transaction.success();
        }
    }

    private void createAndPopulateStringRelationshipIndex( GraphDatabaseService databaseService, String indexName,
            int numberOfRelationships )
    {
        Index<Relationship> index = createRelationshipIndex( databaseService, indexName );
        Stream<Relationship> relationshipStream = getRelationshipGenerator( databaseService, numberOfRelationships );
        try ( Transaction transaction = databaseService.beginTx() )
        {
            relationshipStream.forEach( relationshipStringPropertyPopulator( index, getStringValueSupplier() ) );
            transaction.success();
        }
    }

    private Stream<Relationship> getRelationshipGenerator( GraphDatabaseService databaseService,
            int numberOfRelationships )
    {
        return Stream.generate( () ->
        {
            Node start = databaseService.createNode();
            Node end = databaseService.createNode();
            return start.createRelationshipTo( end, relationshipType );
        } ).limit( numberOfRelationships );
    }

    private void verifySortingOnNumericProperty( GraphDatabaseService db,
            Supplier<Index<? extends PropertyContainer>> indexSupplier, int numberOfEntities )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            QueryContext queryContext = new QueryContext( INT_PROPERTY_KEY + ":**" );
            queryContext.sort( new Sort( new SortedNumericSortField( INT_PROPERTY_KEY, SortField.Type.INT, true ) ) );
            Index<? extends PropertyContainer> index = indexSupplier.get();
            IndexHits<? extends PropertyContainer> propertyContainers = index.query( queryContext );

            int value = numberOfEntities;
            for ( PropertyContainer container : propertyContainers )
            {
                assertEquals( "Expect values to be reverse ordered by int property value", value--,
                        container.getProperty( INT_PROPERTY_KEY ) );
            }
        }
    }

    private void verifySortingOnStringProperty( GraphDatabaseService db,
            Supplier<Index<? extends PropertyContainer>> indexSupplier, int numberOfEntities )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            QueryContext queryContext = new QueryContext( STRING_PROPERTY_KEY + ":**" );
            queryContext.sort( new Sort( new SortedSetSortField( STRING_PROPERTY_KEY, true ) ) );
            Index<? extends PropertyContainer> index = indexSupplier.get();
            IndexHits<? extends PropertyContainer> propertyContainers = index.query( queryContext );

            int value = numberOfEntities;
            for ( PropertyContainer container : propertyContainers )
            {
                assertEquals( "Expect values to be reverse ordered by string property value",
                        "a" + (STRING_VALUE_SHIFT + value--), container.getProperty( STRING_PROPERTY_KEY ) );
            }
        }
    }

    private void verifySortingOnKeyDocId( GraphDatabaseService db,
            Supplier<Index<? extends PropertyContainer>> indexSupplier )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            QueryContext queryContext = new QueryContext( LuceneLegacyIndex.KEY_DOC_ID + ":**" );
            queryContext.sort( new Sort(
                    new SortedNumericSortField( LuceneLegacyIndex.KEY_DOC_ID, SortField.Type.INT, false ) ) );
            Index<? extends PropertyContainer> index = indexSupplier.get();
            IndexHits<? extends PropertyContainer> propertyContainers = index.query( queryContext );

            int value = 1;
            for ( PropertyContainer container : propertyContainers )
            {
                assertEquals( "Expect values to be reverse ordered by int property value", value++,
                        container.getProperty( INT_PROPERTY_KEY ) );
            }
        }
    }

    private void moveMigratedIndexesToStore( File storeDir, File migrationDir, DefaultFileSystemAbstraction fileSystem )
            throws IOException
    {
        fileSystem.deleteRecursively( LuceneDataSource.getLuceneIndexStoreDirectory( storeDir ) );
        fileSystem.copyRecursively( migrationDir, storeDir );
    }

    private GraphDatabaseService createDatabase( File storeDir )
    {
        GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        cleanupRule.add( database );
        return database;
    }

    private Consumer<Node> nodeIntPropertyPopulator( Index<Node> nodeIndex, Supplier<Integer> propertyValueSupplier )
    {
        return node ->
        {
            Integer value = propertyValueSupplier.get();
            node.setProperty( INT_PROPERTY_KEY, value );
            nodeIndex.add( node, INT_PROPERTY_KEY, ValueContext.numeric( value ) );
        };
    }

    private Consumer<Node> nodeStringPropertyPopulator( Index<Node> nodeIndex, Supplier<String> propertyValueSupplier )
    {
        return node ->
        {
            String value = propertyValueSupplier.get();
            node.setProperty( STRING_PROPERTY_KEY, value );
            nodeIndex.add( node, STRING_PROPERTY_KEY, value );
        };
    }

    private Consumer<Relationship> relationshipIntPropertyPopulator( Index<Relationship> relationshipIndex,
            Supplier<Integer> propertyValueSupplier )
    {
        return relationship ->
        {
            Integer value = propertyValueSupplier.get();
            relationship.setProperty( INT_PROPERTY_KEY, value );
            relationshipIndex.add( relationship, INT_PROPERTY_KEY, ValueContext.numeric( value ) );
        };
    }

    private Consumer<Relationship> relationshipStringPropertyPopulator( Index<Relationship> relationshipIndex,
            Supplier<String> propertyValueSupplier )
    {
        return relationship ->
        {
            String value = propertyValueSupplier.get();
            relationship.setProperty( STRING_PROPERTY_KEY, value );
            relationshipIndex.add( relationship, STRING_PROPERTY_KEY, value );
        };
    }

    private Index<Node> createNodeIndex( GraphDatabaseService databaseService, String indexName )
    {
        return createNodeIndex( databaseService, indexName, MapUtil.stringMap() );
    }

    private Index<Node> createNodeIndex( GraphDatabaseService databaseService, String indexName,
            Map<String,String> customIndexConfig )
    {
        try ( Transaction transaction = databaseService.beginTx() )
        {
            Index<Node> nodeIndex = databaseService.index().forNodes( indexName, customIndexConfig );
            transaction.success();
            return nodeIndex;
        }
    }

    private RelationshipIndex createRelationshipIndex( GraphDatabaseService databaseService, String indexName )
    {
        try ( Transaction transaction = databaseService.beginTx() )
        {
            RelationshipIndex relationshipIndex = databaseService.index().forRelationships( indexName );
            transaction.success();
            return relationshipIndex;
        }
    }

    private Supplier<Integer> getIntegerValueSupplier()
    {
        MutableInt counter = new MutableInt();
        return () ->
        {
            counter.increment();
            return counter.intValue();
        };
    }

    private Supplier<String> getStringValueSupplier()
    {
        MutableInt counter = new MutableInt( STRING_VALUE_SHIFT );
        return () ->
        {
            counter.increment();
            return "a" + counter.intValue();
        };
    }

    private class TrackingProgressMonitor implements MigrationProgressMonitor.Section
    {
        private int progressInvocationCounter;
        private int progressSum;

        @Override
        public void start( long max )
        {
            // empty not invoked here
        }

        @Override
        public void progress( long add )
        {
            progressInvocationCounter++;
            progressSum += add;
        }

        @Override
        public void completed()
        {
            // empty not invoked here
        }

        int getProgressInvocationCounter()
        {
            return progressInvocationCounter;
        }

        int getProgressSum()
        {
            return progressSum;
        }
    }

}
