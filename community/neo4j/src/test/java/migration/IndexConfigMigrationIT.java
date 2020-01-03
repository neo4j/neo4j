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
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.io.compress.ZipUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SchemaIndex.LUCENE10;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SchemaIndex.NATIVE10;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SchemaIndex.NATIVE20;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettings.INDEX_CONFIG_ANALYZER;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettings.INDEX_CONFIG_EVENTUALLY_CONSISTENT;
import static org.neo4j.kernel.impl.index.schema.config.SpatialIndexSettings.makeCRSRangeSetting;
import static org.neo4j.kernel.impl.index.schema.config.SpatialIndexSettings.space_filling_curve_max_bits;
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
@ExtendWith( TestDirectoryExtension.class )
class IndexConfigMigrationIT
{
    private enum MinMaxSetting
    {
        wgs84MinX( makeCRSRangeSetting( WGS84, 0, "min" ), "-1" ),
        wgs84MinY( makeCRSRangeSetting( WGS84, 1, "min" ), "-2" ),
        wgs84MaxX( makeCRSRangeSetting( WGS84, 0, "max" ), "3" ),
        wgs84MaxY( makeCRSRangeSetting( WGS84, 1, "max" ), "4" ),
        wgs84_3DMinX( makeCRSRangeSetting( WGS84_3D, 0, "min" ), "-5" ),
        wgs84_3DMinY( makeCRSRangeSetting( WGS84_3D, 1, "min" ), "-6" ),
        wgs84_3DMinZ( makeCRSRangeSetting( WGS84_3D, 2, "min" ), "-7" ),
        wgs84_3DMaxX( makeCRSRangeSetting( WGS84_3D, 0, "max" ), "8" ),
        wgs84_3DMaxY( makeCRSRangeSetting( WGS84_3D, 1, "max" ), "9" ),
        wgs84_3DMaxZ( makeCRSRangeSetting( WGS84_3D, 2, "max" ), "10" ),
        cartesianMinX( makeCRSRangeSetting( Cartesian, 0, "min" ), "-11" ),
        cartesianMinY( makeCRSRangeSetting( Cartesian, 1, "min" ), "-12" ),
        cartesianMaxX( makeCRSRangeSetting( Cartesian, 0, "max" ), "13" ),
        cartesianMaxY( makeCRSRangeSetting( Cartesian, 1, "max" ), "14" ),
        cartesian_3DMinX( makeCRSRangeSetting( Cartesian_3D, 0, "min" ), "-15" ),
        cartesian_3DMinY( makeCRSRangeSetting( Cartesian_3D, 1, "min" ), "-16" ),
        cartesian_3DMinZ( makeCRSRangeSetting( Cartesian_3D, 2, "min" ), "-17" ),
        cartesian_3DMaxX( makeCRSRangeSetting( Cartesian_3D, 0, "max" ), "18" ),
        cartesian_3DMaxY( makeCRSRangeSetting( Cartesian_3D, 1, "max" ), "19" ),
        cartesian_3DMaxZ( makeCRSRangeSetting( Cartesian_3D, 2, "max" ), "20" );

        private final Setting<Double> setting;
        private final String settingValue;

        MinMaxSetting( Setting<Double> setting, String settingValue )
        {
            this.setting = setting;
            this.settingValue = settingValue;
        }
    }

    private static final String space_filling_curve_max_bits_value = "30";
    private static final Map<String,Value> staticExpectedIndexConfig = new HashMap<>();

    static
    {
        staticExpectedIndexConfig.put( "spatial.wgs-84.tableId", Values.intValue( 1 ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84.code", Values.intValue( 4326 ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84.dimensions", Values.intValue( 2 ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84.maxLevels", Values.intValue( 15 ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84.min", Values.doubleArray( new double[]{-1.0, -2.0} ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84.max", Values.doubleArray( new double[]{3.0, 4.0} ) );

        staticExpectedIndexConfig.put( "spatial.wgs-84-3d.tableId", Values.intValue( 1 ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84-3d.code", Values.intValue( 4979 ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84-3d.dimensions", Values.intValue( 3 ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84-3d.maxLevels", Values.intValue( 10 ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84-3d.min", Values.doubleArray( new double[]{-5.0, -6.0, -7.0} ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84-3d.max", Values.doubleArray( new double[]{8.0, 9.0, 10.0} ) );

        staticExpectedIndexConfig.put( "spatial.cartesian.tableId", Values.intValue( 2 ) );
        staticExpectedIndexConfig.put( "spatial.cartesian.code", Values.intValue( 7203 ) );
        staticExpectedIndexConfig.put( "spatial.cartesian.dimensions", Values.intValue( 2 ) );
        staticExpectedIndexConfig.put( "spatial.cartesian.maxLevels", Values.intValue( 15 ) );
        staticExpectedIndexConfig.put( "spatial.cartesian.min", Values.doubleArray( new double[]{-11.0, -12.0} ) );
        staticExpectedIndexConfig.put( "spatial.cartesian.max", Values.doubleArray( new double[]{13.0, 14.0} ) );

        staticExpectedIndexConfig.put( "spatial.cartesian-3d.tableId", Values.intValue( 2 ) );
        staticExpectedIndexConfig.put( "spatial.cartesian-3d.code", Values.intValue( 9157 ) );
        staticExpectedIndexConfig.put( "spatial.cartesian-3d.dimensions", Values.intValue( 3 ) );
        staticExpectedIndexConfig.put( "spatial.cartesian-3d.maxLevels", Values.intValue( 10 ) );
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
        EVENTUALLY_CONSISTENY_ONLY( "fulltextEC", true, "fulltextToken3", asConfigMap( true ) );

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
    private TestDirectory directory;

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
        GraphDatabaseBuilder builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir );
        setSpatialConfig( builder );

        GraphDatabaseService db = builder.newGraphDatabase();
        createIndex( db, NATIVE_BTREE10.providerName(), label1 );
        createIndex( db, NATIVE20.providerName(), label2 );
        createIndex( db, NATIVE10.providerName(), label3 );
        createIndex( db, LUCENE10.providerName(), label4 );
        createSpatialData( db, label1, label2, label3, label4 );
        for ( FulltextIndexDescription fulltextIndex : FulltextIndexDescription.values() )
        {
            createFulltextIndex( db, fulltextIndex.indexProcedure, fulltextIndex.indexName, fulltextIndex.tokenName, propKey, fulltextIndex.configMap );
        }
        db.shutdown();

        File zipFile = new File( storeDir.getParentFile(), storeDir.getName() + ".zip" );
        ZipUtils.zip( new DefaultFileSystemAbstraction(), storeDir, zipFile );
        System.out.println( "Db created in " + zipFile.getAbsolutePath() );
    }

    @Test
    void shouldHaveCorrectDataAndIndexConfiguration() throws IOException, IndexNotFoundKernelException
    {
        File storeDir = directory.databaseDir();
        unzip( getClass(), ZIP_FILE_3_5, storeDir );
        // when
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.allow_upgrade, Settings.TRUE )
                .newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            Set<CoordinateReferenceSystem> allCRS = Iterables.asSet( all() );
            hasIndexCount( db, 7 );
            for ( Node node : db.getAllNodes() )
            {
                hasLabels( node, label1, label2, label3, label4 );
                Object property = node.getProperty( propKey );
                if ( property instanceof PointValue )
                {
                    allCRS.remove( ((PointValue) property).getCoordinateReferenceSystem() );
                }
            }
            assertTrue( allCRS.isEmpty(), "Expected all CRS to be represented in store, but missing " + allCRS );
            assertIndexConfiguration( db );
            assertFulltextIndexConfiguration( db );
            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }

    private static void assertIndexConfiguration( GraphDatabaseAPI db ) throws IndexNotFoundKernelException
    {
        for ( Label label : labels )
        {
            Map<String,Value> actualIndexConfig = getIndexConfig( db, label, propKey );
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

    private static void assertFulltextIndexConfiguration( GraphDatabaseAPI db ) throws IndexNotFoundKernelException
    {
        for ( FulltextIndexDescription fulltextIndex : FulltextIndexDescription.values() )
        {
            Map<String,Value> actualIndexConfig = getFulltextIndexConfig( db, fulltextIndex.indexName );
            for ( Map.Entry<String,Value> expectedEntry : fulltextIndex.configMap.entrySet() )
            {
                Value actualValue = actualIndexConfig.get( expectedEntry.getKey() );
                assertEquals( expectedEntry.getValue(), actualValue,
                        format( "Index did not have expected config, %s.%nExpected: %s%nActual: %s ",
                                fulltextIndex.indexName, fulltextIndex.configMap, actualIndexConfig ) );
            }
        }
    }

    private static Map<String,Value> getFulltextIndexConfig( GraphDatabaseAPI db, String indexName ) throws IndexNotFoundKernelException
    {
        IndexingService indexingService = getIndexingService( db );
        IndexReference indexReference = schemaRead( db ).indexGetForName( indexName );
        IndexProxy indexProxy = indexingService.getIndexProxy( indexReference.schema() );
        return indexProxy.indexConfig();
    }

    @SuppressWarnings( "SameParameterValue" )
    private static Map<String,Value> getIndexConfig( GraphDatabaseAPI db, Label label, String propKey )
            throws IndexNotFoundKernelException
    {
        TokenRead tokenRead = tokenRead( db );
        IndexingService indexingService = getIndexingService( db );
        int labelId = tokenRead.nodeLabel( label.name() );
        int propKeyId = tokenRead.propertyKey( propKey );
        IndexProxy indexProxy = indexingService.getIndexProxy( SchemaDescriptorFactory.forLabel( labelId, propKeyId ) );
        return indexProxy.indexConfig();
    }

    @SuppressWarnings( "SameParameterValue" )
    private static void hasIndexCount( GraphDatabaseAPI db, int expectedIndexCount )
    {
        Iterable<IndexDefinition> indexes = db.schema().getIndexes();
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
                Node node = db.createNode( labels );
                int dim = crs.getDimension();
                double[] coords = new double[dim];
                node.setProperty( propKey, Values.pointValue( crs, coords ) );
            }
            tx.success();
        }
    }

    private static void createIndex( GraphDatabaseService db, String providerName, Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            String indexPattern = format( "\":%s(%s)\"", label.name(), propKey );
            String indexProvider = "\"" + providerName + "\"";
            db.execute( format( "CALL db.createIndex( %s, %s )", indexPattern, indexProvider ) ).close();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
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
            db.execute( query ).close();
            tx.success();
        }
    }

    private static Map<String,Value> asConfigMap( String analyzer, boolean eventuallyConsistent )
    {
        Map<String,Value> map = new HashMap<>();
        map.put( INDEX_CONFIG_ANALYZER, Values.stringValue( analyzer ) );
        map.put( INDEX_CONFIG_EVENTUALLY_CONSISTENT, Values.booleanValue( eventuallyConsistent ) );
        return map;
    }

    private static Map<String,Value> asConfigMap( String analyzer )
    {
        Map<String,Value> map = new HashMap<>();
        map.put( INDEX_CONFIG_ANALYZER, Values.stringValue( analyzer ) );
        return map;
    }

    private static Map<String,Value> asConfigMap( boolean eventuallyConsistent )
    {
        Map<String,Value> map = new HashMap<>();
        map.put( INDEX_CONFIG_EVENTUALLY_CONSISTENT, Values.booleanValue( eventuallyConsistent ) );
        return map;
    }

    private static String asConfigString( Map<String,Value> configMap )
    {
        StringJoiner joiner = new StringJoiner( ", ", "{", "}" );
        configMap.forEach( ( k, v ) -> joiner.add( k + ": \"" + v.asObject() + "\"" ) );
        return joiner.toString();
    }

    private static String array( String... args )
    {
        return Arrays.stream( args ).collect( Collectors.joining( "\", \"", "[\"", "\"]" ) );
    }

    private static void setSpatialConfig( GraphDatabaseBuilder builder )
    {
        builder.setConfig( space_filling_curve_max_bits, space_filling_curve_max_bits_value );
        for ( MinMaxSetting minMaxSetting : MinMaxSetting.values() )
        {
            builder.setConfig( minMaxSetting.setting, minMaxSetting.settingValue );
        }
    }

    private static IndexingService getIndexingService( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( IndexingService.class );
    }

    private static TokenRead tokenRead( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class ).getKernelTransactionBoundToThisThread( false ).tokenRead();
    }

    private static SchemaRead schemaRead( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class ).getKernelTransactionBoundToThisThread( false ).schemaRead();
    }
}
