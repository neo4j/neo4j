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
package migration;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.io.compress.ZipUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.index.schema.config.SpatialIndexSettings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SchemaIndex.LUCENE10;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SchemaIndex.NATIVE10;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SchemaIndex.NATIVE20;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10;
import static org.neo4j.kernel.impl.index.schema.config.SpatialIndexSettings.space_filling_curve_bottom_threshold;
import static org.neo4j.kernel.impl.index.schema.config.SpatialIndexSettings.space_filling_curve_extra_levels;
import static org.neo4j.kernel.impl.index.schema.config.SpatialIndexSettings.space_filling_curve_max_bits;
import static org.neo4j.kernel.impl.index.schema.config.SpatialIndexSettings.space_filling_curve_top_threshold;
import static org.neo4j.test.Unzip.unzip;

/**
 * This test should verify that index configurations from a 3.5 store stay intact when opened again, with migration if needed.
 */
@ExtendWith( TestDirectoryExtension.class )
class IndexConfigMigrationIT
{
    private static final String ZIP_FILE_3_5 = "IndexConfigMigrationIT-3_5-db.zip";
    private static final Setting<Double> wgs84MinX = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.WGS84, 0, "min" );
    private static final Setting<Double> wgs84MinY = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.WGS84, 1, "min" );
    private static final Setting<Double> wgs84MaxX = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.WGS84, 0, "max" );
    private static final Setting<Double> wgs84MaxY = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.WGS84, 1, "max" );
    private static final Setting<Double> wgs84_3DMinX = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.WGS84_3D, 0, "min" );
    private static final Setting<Double> wgs84_3DMinY = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.WGS84_3D, 1, "min" );
    private static final Setting<Double> wgs84_3DMinZ = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.WGS84_3D, 2, "min" );
    private static final Setting<Double> wgs84_3DMaxX = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.WGS84_3D, 0, "max" );
    private static final Setting<Double> wgs84_3DMaxY = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.WGS84_3D, 1, "max" );
    private static final Setting<Double> wgs84_3DMaxZ = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.WGS84_3D, 2, "max" );
    private static final Setting<Double> cartesianMinX = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.Cartesian, 0, "min" );
    private static final Setting<Double> cartesianMinY = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.Cartesian, 1, "min" );
    private static final Setting<Double> cartesianMaxX = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.Cartesian, 0, "max" );
    private static final Setting<Double> cartesianMaxY = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.Cartesian, 1, "max" );
    private static final Setting<Double> cartesian_3DMinX = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.Cartesian_3D, 0, "min" );
    private static final Setting<Double> cartesian_3DMinY = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.Cartesian_3D, 1, "min" );
    private static final Setting<Double> cartesian_3DMinZ = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.Cartesian_3D, 2, "min" );
    private static final Setting<Double> cartesian_3DMaxX = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.Cartesian_3D, 0, "max" );
    private static final Setting<Double> cartesian_3DMaxY = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.Cartesian_3D, 1, "max" );
    private static final Setting<Double> cartesian_3DMaxZ = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.Cartesian_3D, 2, "max" );
    private static final String value_wgs84_min = "-30";
    private static final String value_wgs84_max = "30";
    private static final String value_cartesian_min = "-100";
    private static final String value_cartesian_max = "100";
    private static final String propKey = "key";
    private static final Label label1 = Label.label( "label1" );
    private static final Label label2 = Label.label( "label2" );
    private static final Label label3 = Label.label( "label3" );
    private static final Label label4 = Label.label( "label4" );
    private static final Label[] labels = {label1, label2, label3, label4};

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

        Set<CoordinateReferenceSystem> allCRS = Iterables.asSet( CoordinateReferenceSystem.all() );
        try ( Transaction tx = db.beginTx() )
        {
            hasIndexCount( db, 4 );
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
            tx.success();
        }
    }

    private static void assertIndexConfiguration( GraphDatabaseAPI db ) throws IndexNotFoundKernelException
    {
        // todo implement
    }

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
            for ( CoordinateReferenceSystem crs : CoordinateReferenceSystem.all() )
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
            String indexPattern = String.format( "\":%s(%s)\"", label.name(), propKey );
            String indexProvider = "\"" + providerName + "\"";
            db.execute( String.format( "CALL db.createIndex( %s, %s )", indexPattern, indexProvider ) ).close();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
    }

    private static void setSpatialConfig( GraphDatabaseBuilder builder )
    {
        builder.setConfig( space_filling_curve_extra_levels, "5" );
        builder.setConfig( space_filling_curve_bottom_threshold, "0.5" );
        builder.setConfig( space_filling_curve_top_threshold, "0.99" );
        builder.setConfig( space_filling_curve_max_bits, "30" );
        builder.setConfig( wgs84MinX, value_wgs84_min );
        builder.setConfig( wgs84MinY, value_wgs84_min );
        builder.setConfig( wgs84MaxX, value_wgs84_max );
        builder.setConfig( wgs84MaxY, value_wgs84_max );
        builder.setConfig( wgs84_3DMinX, value_wgs84_min );
        builder.setConfig( wgs84_3DMinY, value_wgs84_min );
        builder.setConfig( wgs84_3DMinZ, value_wgs84_min );
        builder.setConfig( wgs84_3DMaxX, value_wgs84_max );
        builder.setConfig( wgs84_3DMaxY, value_wgs84_max );
        builder.setConfig( wgs84_3DMaxZ, value_wgs84_max );
        builder.setConfig( cartesianMinX, value_cartesian_min );
        builder.setConfig( cartesianMinY, value_cartesian_min );
        builder.setConfig( cartesianMaxX, value_cartesian_max );
        builder.setConfig( cartesianMaxY, value_cartesian_max );
        builder.setConfig( cartesian_3DMinX, value_cartesian_min );
        builder.setConfig( cartesian_3DMinY, value_cartesian_min );
        builder.setConfig( cartesian_3DMinZ, value_cartesian_min );
        builder.setConfig( cartesian_3DMaxX, value_cartesian_max );
        builder.setConfig( cartesian_3DMaxY, value_cartesian_max );
        builder.setConfig( cartesian_3DMaxZ, value_cartesian_max );
    }

    private static IndexingService getIndexingService( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( IndexingService.class );
    }

    private static TokenRead tokenRead( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class ).getKernelTransactionBoundToThisThread( false ).tokenRead();
    }
}
