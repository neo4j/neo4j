/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

package org.neo4j.backup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FilenameFilter;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.com.ComException;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.subprocess.SubProcess;

public class TestBackup
{
    private String serverPath;
    private String otherServerPath;
    private String backupPath;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() throws Exception
    {
        File base = TargetDirectory.forTest( getClass() ).directory( testName.getMethodName(), true );
        serverPath = new File( base, "server" ).getAbsolutePath();
        otherServerPath = new File( base, "server2" ).getAbsolutePath();
        backupPath = new File( base, "backuedup-serverdb" ).getAbsolutePath();
    }

    // TODO MP: What happens if the server database keeps growing, virtually making the files endless?

    @Test
    public void makeSureFullFailsWhenDbExists() throws Exception
    {
        createInitialDataSet( serverPath );
        ServerInterface server = startServer( serverPath );
        OnlineBackup backup = OnlineBackup.from( "localhost" );
        createInitialDataSet( backupPath );
        try
        {
            backup.full( backupPath );
            fail( "Shouldn't be able to do full backup into existing db" );
        }
        catch ( Exception e )
        {
            // good
        }
        shutdownServer( server );
    }

    @Test
    public void makeSureIncrementalFailsWhenNoDb() throws Exception
    {
        createInitialDataSet( serverPath );
        ServerInterface server = startServer( serverPath );
        OnlineBackup backup = OnlineBackup.from( "localhost" );
        try
        {
            backup.incremental( backupPath );
            fail( "Shouldn't be able to do incremental backup into non-existing db" );
        }
        catch ( Exception e )
        {
            // Good
        }
        shutdownServer( server );
    }

    @Test
    public void backupLeavesLastTxInLog() throws Exception
    {
        GraphDatabaseAPI db = null;
        ServerInterface server = null;
        try
        {
            createInitialDataSet( serverPath );
            server = startServer( serverPath );
            OnlineBackup backup = OnlineBackup.from( "localhost" );
            backup.full( backupPath );
            shutdownServer( server );
            server = null;

            db = new EmbeddedGraphDatabase( backupPath );
            for ( XaDataSource ds : db.getXaDataSourceManager().getAllRegisteredDataSources() )
            {
                ds.getMasterForCommittedTx( ds.getLastCommittedTxId() );
            }
            db.shutdown();

            addMoreData( serverPath );
            server = startServer( serverPath );
            backup.incremental( backupPath );
            shutdownServer( server );
            server = null;

            db = new EmbeddedGraphDatabase( backupPath );
            for ( XaDataSource ds : db.getXaDataSourceManager().getAllRegisteredDataSources() )
            {
                ds.getMasterForCommittedTx( ds.getLastCommittedTxId() );
            }
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
            if ( server != null )
            {
                shutdownServer( server );
            }
        }
    }

    @Test
    public void incrementalBackupLeavesOnlyLastTxInLog() throws Exception
    {
        GraphDatabaseAPI db = null;
        ServerInterface server = null;
        try
        {
            createInitialDataSet( serverPath );
            server = startServer( serverPath );
            OnlineBackup backup = OnlineBackup.from( "localhost" );
            backup.full( backupPath );
            shutdownServer( server );
            server = null;

            addMoreData( serverPath );
            server = startServer( serverPath );
            backup.incremental( backupPath );
            shutdownServer( server );
            server = null;

            // do 2 rotations, add two empty logs
            new EmbeddedGraphDatabase( backupPath ).shutdown();
            new EmbeddedGraphDatabase( backupPath ).shutdown();

            addMoreData( serverPath );
            server = startServer( serverPath );
            backup.incremental( backupPath );
            shutdownServer( server );
            server = null;

            int logsFound = new File( backupPath ).listFiles( new FilenameFilter()
            {
                @Override
                public boolean accept( File dir, String name )
                {
                    return name.startsWith( "nioneo_logical.log" )
                           && !name.endsWith( "active" );
                }
            } ).length;

            // 2 one the real and the other from the rotation of shutdown
            assertEquals( 2, logsFound );

            db = new EmbeddedGraphDatabase( backupPath );

            for ( XaDataSource ds : db.getXaDataSourceManager().getAllRegisteredDataSources() )
            {
                ds.getMasterForCommittedTx( ds.getLastCommittedTxId() );
            }
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
            if ( server != null )
            {
                shutdownServer( server );
            }
        }
    }

    @Test
    public void fullThenIncremental() throws Exception
    {
        DbRepresentation initialDataSetRepresentation = createInitialDataSet( serverPath );
        ServerInterface server = startServer( serverPath );

        // START SNIPPET: onlineBackup
        OnlineBackup backup = OnlineBackup.from( "localhost" );
        backup.full( backupPath );
        // END SNIPPET: onlineBackup
        assertEquals( initialDataSetRepresentation, DbRepresentation.of( backupPath ) );
        shutdownServer( server );

        DbRepresentation furtherRepresentation = addMoreData( serverPath );
        server = startServer( serverPath );
        // START SNIPPET: onlineBackup
        backup.incremental( backupPath );
        // END SNIPPET: onlineBackup
        assertEquals( furtherRepresentation, DbRepresentation.of( backupPath ) );
        shutdownServer( server );
    }

    @Test
    public void makeSureNoLogFileRemains() throws Exception
    {
        createInitialDataSet( serverPath );
        ServerInterface server = startServer( serverPath );
        OnlineBackup backup = OnlineBackup.from( "localhost" );

        // First check full
        backup.full( backupPath );
        assertFalse( checkLogFileExistence( backupPath ) );
        // Then check empty incremental
        backup.incremental( backupPath );
        assertFalse( checkLogFileExistence( backupPath ) );
        // Then check real incremental
        shutdownServer( server );
        addMoreData( serverPath );
        server = startServer( serverPath );
        backup.incremental( backupPath );
        assertFalse( checkLogFileExistence( backupPath ) );
        shutdownServer( server );
    }

    @Test
    public void makeSureStoreIdIsEnforced() throws Exception
    {
        // Create data set X on server A
        DbRepresentation initialDataSetRepresentation = createInitialDataSet( serverPath );
        ServerInterface server = startServer( serverPath );

        // Grab initial backup from server A
        OnlineBackup backup = OnlineBackup.from( "localhost" );
        backup.full( backupPath );
        assertEquals( initialDataSetRepresentation, DbRepresentation.of( backupPath ) );
        shutdownServer( server );

        // Create data set X+Y on server B
        createInitialDataSet( otherServerPath );
        addMoreData( otherServerPath );
        server = startServer( otherServerPath );

        // Try to grab incremental backup from server B.
        // Data should be OK, but store id check should prevent that.
        try
        {
            backup.incremental( backupPath );
            fail( "Shouldn't work" );
        }
        catch ( ComException e )
        { // Good
        }
        shutdownServer( server );
        // Just make sure incremental backup can be received properly from
        // server A, even after a failed attempt from server B
        DbRepresentation furtherRepresentation = addMoreData( serverPath );
        server = startServer( serverPath );
        backup.incremental( backupPath );
        assertEquals( furtherRepresentation, DbRepresentation.of( backupPath ) );
        shutdownServer( server );
    }

    private ServerInterface startServer( String path ) throws Exception
    {
        /*
        ServerProcess server = new ServerProcess();
        try
        {
            server.startup( Pair.of( path, "true" ) );
        }
        catch ( Throwable e )
        {
            // TODO Auto-generated catch block
            throw new RuntimeException( e );
        }
        */
        ServerInterface server = new EmbeddedServer( path );
        server.awaitStarted();
        return server;
    }

    private void shutdownServer( ServerInterface server ) throws Exception
    {
        server.shutdown();
        Thread.sleep( 1000 );
    }

    private DbRepresentation addMoreData( String path )
    {
        GraphDatabaseService db = startGraphDatabase( path );
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.setProperty( "backup", "Is great" );
        db.getReferenceNode().createRelationshipTo( node,
                DynamicRelationshipType.withName( "LOVES" ) );
        tx.success();
        tx.finish();
        DbRepresentation result = DbRepresentation.of( db );
        db.shutdown();
        return result;
    }

    private GraphDatabaseService startGraphDatabase( String path )
    {
        return new GraphDatabaseFactory().
            newEmbeddedDatabaseBuilder( path ).
            setConfig( GraphDatabaseSettings.keep_logical_logs, GraphDatabaseSetting.TRUE ).
            newGraphDatabase();
    }

    private DbRepresentation createInitialDataSet( String path )
    {
        GraphDatabaseService db = startGraphDatabase( path );
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.setProperty( "myKey", "myValue" );
        Index<Node> nodeIndex = db.index().forNodes( "db-index" );
        nodeIndex.add( node, "myKey", "myValue" );
        db.getReferenceNode().createRelationshipTo( node,
                DynamicRelationshipType.withName( "KNOWS" ) );
        tx.success();
        tx.finish();
        DbRepresentation result = DbRepresentation.of( db );
        db.shutdown();
        return result;
    }

    @Test
    public void multipleIncrementals() throws Exception
    {
        GraphDatabaseService db = null;
        try
        {
            db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( serverPath ).
                setConfig( OnlineBackupSettings.online_backup_enabled, GraphDatabaseSetting.TRUE ).
                newGraphDatabase();

            Transaction tx = db.beginTx();
            Index<Node> index = db.index().forNodes( "yo" );
            index.add( db.createNode(), "justTo", "commitATx" );
            tx.success();
            tx.finish();

            OnlineBackup backup = OnlineBackup.from( "localhost" );
            backup.full( backupPath );
            long lastCommittedTxForLucene = getLastCommittedTx( backupPath );

            for ( int i = 0; i < 5; i++ )
            {
                tx = db.beginTx();
                Node node = db.createNode();
                index.add( node, "key", "value" + i );
                tx.success();
                tx.finish();
                backup.incremental( backupPath );
                assertEquals( lastCommittedTxForLucene + i + 1,
                        getLastCommittedTx( backupPath ) );
            }
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }

    @Test
    public void backupIndexWithNoCommits() throws Exception
    {
        GraphDatabaseService db = null;
        try
        {
            db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( serverPath ).
                setConfig( OnlineBackupSettings.online_backup_enabled, GraphDatabaseSetting.TRUE ).
                newGraphDatabase();

            db.index().forNodes( "created-no-commits" );

            OnlineBackup backup = OnlineBackup.from( "localhost" );
            backup.full( backupPath );
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }

    private long getLastCommittedTx( String path )
    {
        GraphDatabaseService db = new EmbeddedGraphDatabase( path );
        try
        {
            XaDataSource ds = ((InternalAbstractGraphDatabase)db).getXaDataSourceManager().getNeoStoreDataSource();
            return ds.getLastCommittedTxId();
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void backupEmptyIndex() throws Exception
    {
        String key = "name";
        String value = "Neo";
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( serverPath ).
            setConfig( OnlineBackupSettings.online_backup_enabled, GraphDatabaseSetting.TRUE ).
            newGraphDatabase();

        Index<Node> index = db.index().forNodes( key );
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.setProperty( key, value );
        tx.success();
        tx.finish();
        OnlineBackup.from( "localhost" ).full( backupPath );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( backupPath ) );
        FileUtils.deleteDirectory( new File( backupPath ) );
        OnlineBackup.from( "localhost" ).full( backupPath );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( backupPath ) );

        tx = db.beginTx();
        index.add( node, key, value );
        tx.success();
        tx.finish();
        FileUtils.deleteDirectory( new File( backupPath ) );
        OnlineBackup.from( "localhost" ).full( backupPath );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( backupPath ) );
        db.shutdown();
    }

    @Test
    public void shouldRetainFileLocksAfterFullBackupOnLiveDatabase() throws Exception
    {
        String sourcePath = "target/var/serverdb-lock";
        FileUtils.deleteDirectory( new File( sourcePath ) );

        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( sourcePath ).
            setConfig( OnlineBackupSettings.online_backup_enabled, GraphDatabaseSetting.TRUE ).
            newGraphDatabase();
        try
        {
            assertStoreIsLocked( sourcePath );
            OnlineBackup.from( "localhost" ).full( backupPath );
            assertStoreIsLocked( sourcePath );
        }
        finally
        {
            db.shutdown();
        }
    }

    private static void assertStoreIsLocked( String path )
    {
        try
        {
            new EmbeddedGraphDatabase( path ).shutdown();
            fail( "Could start up database in same process, store not locked" );
        }
        catch ( IllegalStateException ex )
        {
            // expected
        }
        StartupChecker proc = new LockProcess().start( path );
        try
        {
            assertFalse( "Could start up database in subprocess, store is not locked", proc.startupOk() );
        }
        finally
        {
            SubProcess.stop( proc );
        }
    }

    public interface StartupChecker
    {
        boolean startupOk();
    }

    @SuppressWarnings( "serial" )
    private static class LockProcess extends SubProcess<StartupChecker, String> implements StartupChecker
    {
        private volatile Object state;

        @Override
        public boolean startupOk()
        {
            Object result;
            do
            {
                result = state;
            }
            while ( result == null );
            return !( state instanceof Exception );
        }

        @Override
        protected void startup( String path ) throws Throwable
        {
            GraphDatabaseService db;
            try
            {
                db = new EmbeddedGraphDatabase( path );
            }
            catch ( IllegalStateException ex )
            {
                state = ex;
                return;
            }
            state = new Object();
            db.shutdown();
        }
    }

    private static boolean checkLogFileExistence( String directory )
    {
        return new File( directory, StringLogger.DEFAULT_NAME ).exists();
    }
}
