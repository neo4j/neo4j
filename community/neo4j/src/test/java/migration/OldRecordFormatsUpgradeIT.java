package migration;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_2;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.Values.pointValue;

public class OldRecordFormatsUpgradeIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void skipMigrationIfFormatSpecifiedInConfig() throws Exception
    {
        final File storeDir = testDirectory.graphDbDir();
        final GraphDatabaseService database =
                new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir ).setConfig( GraphDatabaseSettings.record_format,
                        StandardV3_2.NAME ).newGraphDatabase();
        try ( Transaction transaction = database.beginTx() )
        {
            final Node node = database.createNode();
            node.setProperty( "a", "b" );
            transaction.success();
        }
        database.shutdown();

        final GraphDatabaseService nonUpgradedStore =
                new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir ).setConfig( GraphDatabaseSettings.record_format, StandardV3_2.NAME ).setConfig(
                        GraphDatabaseSettings.allow_upgrade, Settings.FALSE ).newGraphDatabase();
        try ( Transaction transaction = nonUpgradedStore.beginTx() )
        {
            assertEquals( 1, Iterables.count( nonUpgradedStore.getAllNodes() ) );
        }
        nonUpgradedStore.shutdown();
    }

    @Test
    public void failToCreatePointOnOldDatabase() throws Exception
    {
        final File storeDir = testDirectory.graphDbDir();
        final GraphDatabaseService database =
                new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir ).setConfig( GraphDatabaseSettings.record_format,
                        StandardV3_2.NAME ).newGraphDatabase();
        try ( Transaction transaction = database.beginTx() )
        {
            final Node node = database.createNode();
            node.setProperty( "a", "b" );
            transaction.success();
        }
        database.shutdown();

        final GraphDatabaseService nonUpgradedStore =
                new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir ).setConfig( GraphDatabaseSettings.record_format, StandardV3_2.NAME ).setConfig(
                        GraphDatabaseSettings.allow_upgrade, Settings.FALSE ).newGraphDatabase();
        try ( Transaction transaction = nonUpgradedStore.beginTx() )
        {
            final Node node = nonUpgradedStore.createNode();
            // TODO this test should fail
            node.setProperty( "a", pointValue( Cartesian, 1.0, 2.0 ) );
            transaction.success();
        }
    }
}
