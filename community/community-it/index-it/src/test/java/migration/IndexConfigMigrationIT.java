/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package migration;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.compress.ZipUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.index.schema.config.CrsConfig;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettingsKeys.ANALYZER;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettingsKeys.EVENTUALLY_CONSISTENT;
import static org.neo4j.test.Unzip.unzip;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.all;
import static org.neo4j.values.storable.Values.COMPARATOR;

/**
 * This test should verify that index configurations from a 3.5 store stay intact when opened again, with migration if needed.
 */
@Neo4jLayoutExtension
class IndexConfigMigrationIT
{
    @Inject
    private DatabaseLayout databaseLayout;

    private enum MinMaxSetting
    {
        wgs84Min( CrsConfig.group( WGS84 ).min, List.of( -1.0, -2.0 ) ),
        wgs84Max( CrsConfig.group( WGS84 ).max, List.of( 3.0, 4.0 ) ),

        wgs84_3DMin( CrsConfig.group( WGS84_3D ).min, List.of( -5.0, -6.0, -7.0 ) ),
        wgs84_3DMax( CrsConfig.group( WGS84_3D ).max, List.of( 8.0, 9.0, 10.0 ) ),

        cartesianMin( CrsConfig.group( Cartesian ).min, List.of( -11.0, -12.0 ) ),
        cartesianMax( CrsConfig.group( Cartesian ).max, List.of( 13.0, 14.0 ) ),

        cartesian_3DMin( CrsConfig.group( Cartesian_3D ).min, List.of( -15.0, -16.0, -17.0 ) ),
        cartesian_3DMax( CrsConfig.group( Cartesian_3D ).max, List.of( 18.0, 19.0, 20.0 ) );

        private final Setting<List<Double>> setting;
        private final List<Double> settingValue;

        MinMaxSetting( Setting<List<Double>> setting, List<Double> settingValue )
        {
            this.setting = setting;
            this.settingValue = settingValue;
        }
    }

    private static final Map<String,Value> staticExpectedIndexConfig = new HashMap<>();

    static
    {
        /* Note on why maxLevels settings are commented out:
        Indexes in 3.5 where created with maxLevels configured. This setting was removed in
        4.0 and actual number was fixated. Therefore we don't migrate this index setting. */

//        staticExpectedIndexConfig.put( "spatial.wgs-84.maxLevels", Values.intValue( 15 ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84.min", Values.doubleArray( new double[]{-1.0, -2.0} ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84.max", Values.doubleArray( new double[]{3.0, 4.0} ) );

//        staticExpectedIndexConfig.put( "spatial.wgs-84-3d.maxLevels", Values.intValue( 10 ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84-3d.min", Values.doubleArray( new double[]{-5.0, -6.0, -7.0} ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84-3d.max", Values.doubleArray( new double[]{8.0, 9.0, 10.0} ) );

//        staticExpectedIndexConfig.put( "spatial.cartesian.maxLevels", Values.intValue( 15 ) );
        staticExpectedIndexConfig.put( "spatial.cartesian.min", Values.doubleArray( new double[]{-11.0, -12.0} ) );
        staticExpectedIndexConfig.put( "spatial.cartesian.max", Values.doubleArray( new double[]{13.0, 14.0} ) );

//        staticExpectedIndexConfig.put( "spatial.cartesian-3d.maxLevels", Values.intValue( 10 ) );
        staticExpectedIndexConfig.put( "spatial.cartesian-3d.min", Values.doubleArray( new double[]{-15.0, -16.0, -17.0} ) );
        staticExpectedIndexConfig.put( "spatial.cartesian-3d.max", Values.doubleArray( new double[]{18.0, 19.0, 20.0} ) );
    }

    private static final String ZIP_FILE_3_5 = "IndexConfigMigrationIT-3_5-db.zip";

    // Schema index
    private static final String propKey = "key";
    private static final Label label1 = Label.label( "label1" );
    private static final Label label2 = Label.label( "label2" );
    private static final Label label3 = Label.label( "label3" );
    private static final Label label4 = Label.label( "label4" );
    private static final Label[] labels = {label1, label2, label3, label4};

    // Fulltext index
    private enum FulltextIndexDescription
    {
        BOTH( "fulltextBoth", true, "fulltextToken1", asConfigMap( "simple", true ) ),
        ANALYZER_ONLY( "fulltextAnalyzer", false, "fulltextToken2", asConfigMap( "russian" ) ),
        EVENTUALLY_CONSISTENT_ONLY( "fulltextEC", true, "fulltextToken3", asConfigMap( true ) );

        private final String indexName;
        private final String indexProcedure;
        private final String tokenName;
        private final Map<String,Value> configMap;

        FulltextIndexDescription( String indexName, boolean nodeIndex, String tokenName, Map<String,Value> configMap )
        {
            this.indexName = indexName;
            this.tokenName = tokenName;
            this.configMap = configMap;
            this.indexProcedure = nodeIndex ? "createNodeIndex" : "createRelationshipIndex";
        }
    }

    @Inject
    private TestDirectory testDirectory;

    private static File tempStoreDirectory() throws IOException
    {
        File file = File.createTempFile( "create-db", "neo4j" );
        File storeDir = new File( file.getAbsoluteFile().getParentFile(), file.getName() );
        FileUtils.deleteFile( file );
        return storeDir;
    }

    @Disabled( "Here as reference for how 3.5 db was created" )
    @Test
    void create3_5Database() throws Exception
    {
        File storeDir = tempStoreDirectory();
        DatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( storeDir );
        setSpatialConfig( builder );

        DatabaseManagementService dbms = builder.build();
        GraphDatabaseService db = dbms.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        createIndex( db, NATIVE_BTREE10.providerName(), label1 );
//        createIndex( db, NATIVE20.providerName(), label2 ); // <- Those index providers are removed in 4.0, but here for reference.
//        createIndex( db, NATIVE10.providerName(), label3 );
//        createIndex( db, LUCENE10.providerName(), label4 );
        createSpatialData( db, label1, label2, label3, label4 );
        for ( FulltextIndexDescription fulltextIndex : FulltextIndexDescription.values() )
        {
            createFulltextIndex( db, fulltextIndex.indexProcedure, fulltextIndex.indexName, fulltextIndex.tokenName, propKey, fulltextIndex.configMap );
        }
        dbms.shutdown();

        File zipFile = new File( storeDir.getParentFile(), storeDir.getName() + ".zip" );
        ZipUtils.zip( new DefaultFileSystemAbstraction(), storeDir, zipFile );
        System.out.println( "Db created in " + zipFile.getAbsolutePath() );
    }

    @Test
    void shouldHaveCorrectDataAndIndexConfiguration() throws IOException, IndexNotFoundKernelException
    {
        File databaseDir = databaseLayout.databaseDirectory();
        unzip( getClass(), ZIP_FILE_3_5, databaseDir );
        // when
        DatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setConfig( GraphDatabaseSettings.allow_upgrade, true );
        DatabaseManagementService dbms = builder.build();
        try
        {
            GraphDatabaseAPI db = (GraphDatabaseAPI) dbms.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
            Set<CoordinateReferenceSystem> allCRS = Iterables.asSet( all() );
            try ( Transaction tx = db.beginTx() )
            {
                hasIndexCount( tx, 7 );
                for ( Node node : tx.getAllNodes() )
                {
                    hasLabels( node, label1, label2, label3, label4 );
                    Object property = node.getProperty( propKey );
                    if ( property instanceof PointValue )
                    {
                        allCRS.remove( ((PointValue) property).getCoordinateReferenceSystem() );
                    }
                }
                assertTrue( allCRS.isEmpty(), "Expected all CRS to be represented in store, but missing " + allCRS );
                assertIndexConfiguration( db, tx );
                assertFulltextIndexConfiguration( db, tx );
                tx.commit();
            }
        }
        finally
        {
            dbms.shutdown();
        }

        // Assert old index files has been removed
        File baseSchemaIndexFolder = IndexDirectoryStructure.baseSchemaIndexFolder( databaseDir );
        Set<File> retiredIndexProviderDirectories = Set.of(
                new File( baseSchemaIndexFolder, "lucene" ),
                new File( baseSchemaIndexFolder, "lucene-1.0" ),
                new File( baseSchemaIndexFolder, "lucene_native-1.0" ),
                new File( baseSchemaIndexFolder, "lucene_native-2.0" )
        );
        for ( File indexProviderDirectory : Objects.requireNonNull( baseSchemaIndexFolder.listFiles() ) )
        {
            assertFalse( retiredIndexProviderDirectories.contains( indexProviderDirectory ),
                    "Expected old index provider directories to be deleted during migration but store still had directory " + indexProviderDirectory );
        }
    }

    private static void assertIndexConfiguration( GraphDatabaseAPI db, Transaction tx ) throws IndexNotFoundKernelException
    {
        for ( Label label : labels )
        {
            Map<String,Value> actualIndexConfig = getIndexConfig( db, tx, label );
            Map<String,Value> expectedIndexConfig = new HashMap<>( staticExpectedIndexConfig );
            for ( Map.Entry<String,Value> entry : actualIndexConfig.entrySet() )
            {
                String actualKey = entry.getKey();
                Value actualValue = entry.getValue();
                Value expectedValue = expectedIndexConfig.remove( actualKey );
                assertNotNull( expectedValue, "Actual index config had map entry that was not among expected " + entry );
                assertEquals( 0, COMPARATOR.compare( expectedValue, actualValue ),
                        format( "Expected and actual index config value differed for %s, expected %s but was %s.", actualKey, expectedValue,
                                actualValue ) );
            }
            assertTrue( expectedIndexConfig.isEmpty(), "Actual index config was missing some values: " + expectedIndexConfig );
        }
    }

    private static void assertFulltextIndexConfiguration( GraphDatabaseAPI db, Transaction tx ) throws IndexNotFoundKernelException
    {
        for ( FulltextIndexDescription fulltextIndex : FulltextIndexDescription.values() )
        {
            Map<String,Value> actualIndexConfig = getIndexConfig( db, tx, fulltextIndex.indexName );
            for ( Map.Entry<String,Value> expectedEntry : fulltextIndex.configMap.entrySet() )
            {
                Value actualValue = actualIndexConfig.get( expectedEntry.getKey() );
                assertEquals( expectedEntry.getValue(), actualValue,
                        format( "Index did not have expected config, %s.%nExpected: %s%nActual: %s ",
                                fulltextIndex.indexName, fulltextIndex.configMap, actualIndexConfig ) );
            }
        }
    }

    private static Map<String,Value> getIndexConfig( GraphDatabaseAPI db, Transaction tx, String indexName ) throws IndexNotFoundKernelException
    {
        IndexingService indexingService = getIndexingService( db );
        IndexDescriptor indexReference = schemaRead( tx ).indexGetForName( indexName );
        IndexProxy indexProxy = indexingService.getIndexProxy( indexReference );
        return indexProxy.indexConfig();
    }

    @SuppressWarnings( "SameParameterValue" )
    private static Map<String,Value> getIndexConfig( GraphDatabaseAPI db, Transaction tx, Label label ) throws IndexNotFoundKernelException
    {
        IndexDefinitionImpl indexDefinition = (IndexDefinitionImpl) single( tx.schema().getIndexes( label ) );
        IndexDescriptor index = indexDefinition.getIndexReference();
        IndexingService indexingService = getIndexingService( db );
        IndexProxy indexProxy = indexingService.getIndexProxy( index );
        return indexProxy.indexConfig();
    }

    @SuppressWarnings( "SameParameterValue" )
    private static void hasIndexCount( Transaction transaction, int expectedIndexCount )
    {
        Iterable<IndexDefinition> indexes = transaction.schema().getIndexes();
        long actualIndexCount = Iterables.count( indexes );
        assertEquals( expectedIndexCount, actualIndexCount, "Expected there to be " + expectedIndexCount + " indexes but was " + actualIndexCount );
    }

    private static void hasLabels( Node node, Label... labels )
    {
        for ( Label label : labels )
        {
            assertTrue( node.hasLabel( label ), "Did not have label " + label );
        }
    }

    private static void createSpatialData( GraphDatabaseService db, Label... labels )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( CoordinateReferenceSystem crs : all() )
            {
                Node node = tx.createNode( labels );
                int dim = crs.getDimension();
                double[] coords = new double[dim];
                node.setProperty( propKey, Values.pointValue( crs, coords ) );
            }
            tx.commit();
        }
    }

    private static void createIndex( GraphDatabaseService db, String providerName, Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            String indexPattern = format( "\":%s(%s)\"", label.name(), propKey );
            String indexProvider = '"' + providerName + '"';
            tx.execute( format( "CALL db.createIndex( %s, %s )", indexPattern, indexProvider ) ).close();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.commit();
        }
    }

    private static void createFulltextIndex( GraphDatabaseService db, String indexProcedure, String fulltextName, String token, String propKey,
            Map<String,Value> configMap )
    {
        try ( Transaction tx = db.beginTx() )
        {
            String labelArray = array( token );
            String propArray = array( propKey );
            String configString = asConfigString( configMap );
            System.out.println( fulltextName + " created with config: " + configString );
            String query = format( "CALL db.index.fulltext." + indexProcedure + "(\"%s\", %s, %s, %s )", fulltextName, labelArray, propArray, configString );
            tx.execute( query ).close();
            tx.commit();
        }
    }

    private static Map<String,Value> asConfigMap( String analyzer, boolean eventuallyConsistent )
    {
        Map<String,Value> map = new HashMap<>();
        map.put( ANALYZER, Values.stringValue( analyzer ) );
        map.put( EVENTUALLY_CONSISTENT, Values.booleanValue( eventuallyConsistent ) );
        return map;
    }

    private static Map<String,Value> asConfigMap( String analyzer )
    {
        Map<String,Value> map = new HashMap<>();
        map.put( ANALYZER, Values.stringValue( analyzer ) );
        return map;
    }

    private static Map<String,Value> asConfigMap( boolean eventuallyConsistent )
    {
        Map<String,Value> map = new HashMap<>();
        map.put( EVENTUALLY_CONSISTENT, Values.booleanValue( eventuallyConsistent ) );
        return map;
    }

    private static String asConfigString( Map<String,Value> configMap )
    {
        StringJoiner joiner = new StringJoiner( ", ", "{", "}" );
        configMap.forEach( ( k, v ) -> joiner.add( k + ": \"" + v.asObject() + '"' ) );
        return joiner.toString();
    }

    private static String array( String... args )
    {
        return Arrays.stream( args ).collect( Collectors.joining( "\", \"", "[\"", "\"]" ) );
    }

    private static void setSpatialConfig( DatabaseManagementServiceBuilder builder )
    {
        for ( MinMaxSetting minMaxSetting : MinMaxSetting.values() )
        {
            builder.setConfig( minMaxSetting.setting, minMaxSetting.settingValue );
        }
    }

    private static IndexingService getIndexingService( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( IndexingService.class );
    }

    private static SchemaRead schemaRead( Transaction tx )
    {
        return ((InternalTransaction) tx).kernelTransaction().schemaRead();
    }
}
