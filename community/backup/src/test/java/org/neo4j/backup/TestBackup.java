package org.neo4j.backup;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.test.DbRepresentation;

public class TestBackup
{
    private String serverPath = "target/var/serverdb";
    private String backupPath = "target/var/backuedup-serverdb";
    
    @Before
    public void before() throws Exception
    {
        FileUtils.deleteDirectory( new File( serverPath ) );
        FileUtils.deleteDirectory( new File( backupPath ) );
    }
    
    // TODO MP: What happens if the server database keeps growing, virtually making the files endless?
    
    @Test
    public void fullThenIncremental() throws Exception
    {
        DbRepresentation initialDataSetRepresentation = createInitialDataSet();
        ServerInterface server = startServer();
        OnlineBackup backup = OnlineBackup.from( "localhost" );
        backup.full( backupPath );
        assertEquals( initialDataSetRepresentation, DbRepresentation.of( backupPath ) );
        shutdownServer( server );
        
        DbRepresentation furtherRepresentation = addMoreData();
        server = startServer();
        backup.incremental( backupPath );
        assertEquals( furtherRepresentation, DbRepresentation.of( backupPath ) );
        shutdownServer( server );
    }

    private ServerInterface startServer()
    {
        ServerInterface server = new ServerProcess().start( serverPath );
        server.awaitStarted();
        return server;
    }

    private void shutdownServer( ServerInterface server ) throws Exception
    {
        server.shutdown();
        Thread.sleep( 1000 );
    }

    private DbRepresentation addMoreData()
    {
        GraphDatabaseService db = startGraphDatabase( serverPath );
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.setProperty( "backup", "Is great" );
        db.getReferenceNode().createRelationshipTo( node, DynamicRelationshipType.withName( "LOVES" ) );
        tx.success();
        tx.finish();
        DbRepresentation result = DbRepresentation.of( db );
        db.shutdown();
        return result;
    }

    private GraphDatabaseService startGraphDatabase( String path )
    {
        return new EmbeddedGraphDatabase( path, stringMap( Config.KEEP_LOGICAL_LOGS, "true" ) );
    }

    private DbRepresentation createInitialDataSet()
    {
        GraphDatabaseService db = startGraphDatabase( serverPath );
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.setProperty( "myKey", "myValue" );
        db.getReferenceNode().createRelationshipTo( node, DynamicRelationshipType.withName( "KNOWS" ) );
        tx.success();
        tx.finish();
        DbRepresentation result = DbRepresentation.of( db );
        db.shutdown();
        return result;
    }
}
