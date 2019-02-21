package org.neo4j.kernel.impl.index.schema;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.test.TestLabels.LABEL_ONE;

class IndexCleanupIT
{
    private static final String propertyKey = "key";

    @ParameterizedTest
    @EnumSource( GraphDatabaseSettings.SchemaIndex.class )
    void mustClearIndexDirectoryOnDropWhileOnline( GraphDatabaseSettings.SchemaIndex schemaIndex ) throws IOException
    {
        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        GraphDatabaseService db = buildDb( fs, schemaIndex );
        createIndex( db );

        File[] providerDirectories = indexDirectory( fs, db );
        for ( File providerDirectory : providerDirectories )
        {
            assertTrue( fs.listFiles( providerDirectory ).length > 0, "expected there to be at least one index per existing provider map" );
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().getIndexes().forEach( IndexDefinition::drop );
            tx.success();
        }

        for ( File providerDirectory : providerDirectories )
        {
            assertEquals( 0, fs.listFiles( providerDirectory ).length, "expected there to be no indexes" );
        }
    }

    private GraphDatabaseService buildDb( EphemeralFileSystemAbstraction fs, GraphDatabaseSettings.SchemaIndex schemaIndex )
    {
        return new TestGraphDatabaseFactory()
                    .setFileSystem( fs )
                    .newImpermanentDatabaseBuilder()
                    .setConfig( GraphDatabaseSettings.default_schema_provider, schemaIndex.providerName() )
                    .newGraphDatabase();
    }

    private void createIndex( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL_ONE ).on( propertyKey ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
    }

    private File[] indexDirectory( EphemeralFileSystemAbstraction fs, GraphDatabaseService db )
    {
        GraphDatabaseAPI databaseAPI = (GraphDatabaseAPI) db;
        DatabaseLayout databaseLayout = databaseAPI.databaseLayout();
        File dbDir = databaseLayout.databaseDirectory();
        File schemaDir = new File( dbDir, "schema" );
        File indexDir = new File( schemaDir, "index" );
        return fs.listFiles( indexDir );
    }

    private DependencyResolver dependencyResolver( GraphDatabaseService db )
    {
        return ((GraphDatabaseAPI)db).getDependencyResolver();
    }

    //todo
    // mustClearIndexDirectoryOnDropWhilePopulating
    // mustClearIndexDirectoryOnDropWhileFailed
}
