/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.File;
import java.io.FilenameFilter;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.impl.nioneo.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.subprocess.SubProcess;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestBackup
{
    private File serverPath;
    private File otherServerPath;
    private File backupPath;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() throws Exception
    {
        File base = TargetDirectory.forTest( getClass() ).directory( testName.getMethodName(), true );
        serverPath = new File( base, "server" );
        otherServerPath = new File( base, "server2" );
        backupPath = new File( base, "backuedup-serverdb" );
    }

    // TODO MP: What happens if the server database keeps growing, virtually making the files endless?

    @Test
    public void makeSureFullFailsWhenDbExists() throws Exception
    {
        createInitialDataSet( serverPath );
        ServerInterface server = startServer( serverPath );
        OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );
        createInitialDataSet( backupPath );
        try
        {
            backup.full( backupPath.getPath() );
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
        OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );
        try
        {
            backup.incremental( backupPath.getPath() );
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
            OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );
            backup.full( backupPath.getPath() );
            shutdownServer( server );
            server = null;

            db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( backupPath.getPath() );
            for ( XaDataSource ds : db.getDependencyResolver().resolveDependency( XaDataSourceManager.class ).getAllRegisteredDataSources() )
            {
                ds.getMasterForCommittedTx( ds.getLastCommittedTxId() );
            }
            db.shutdown();

            addMoreData( serverPath );
            server = startServer( serverPath );
            backup.incremental( backupPath.getPath() );
            shutdownServer( server );
            server = null;

            db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( backupPath.getPath() );
            for ( XaDataSource ds : db.getDependencyResolver().resolveDependency( XaDataSourceManager.class).getAllRegisteredDataSources() )
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
            OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );
            backup.full( backupPath.getPath() );
            shutdownServer( server );
            server = null;

            addMoreData( serverPath );
            server = startServer( serverPath );
            backup.incremental( backupPath.getPath() );
            shutdownServer( server );
            server = null;

            // do 2 rotations, add two empty logs
            new GraphDatabaseFactory().newEmbeddedDatabase( backupPath.getPath() ).shutdown();
            new GraphDatabaseFactory().newEmbeddedDatabase( backupPath.getPath() ).shutdown();

            addMoreData( serverPath );
            server = startServer( serverPath );
            backup.incremental( backupPath.getPath() );
            shutdownServer( server );
            server = null;

            int logsFound = backupPath.listFiles( new FilenameFilter()
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

            db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( backupPath.getPath() );

            for ( XaDataSource ds : db.getDependencyResolver().resolveDependency( XaDataSourceManager.class ).getAllRegisteredDataSources() )
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
        OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );
        backup.full( backupPath.getPath() );
        // END SNIPPET: onlineBackup
        assertEquals( initialDataSetRepresentation, DbRepresentation.of( backupPath ) );
        shutdownServer( server );

        DbRepresentation furtherRepresentation = addMoreData( serverPath );
        server = startServer( serverPath );
        // START SNIPPET: onlineBackup
        backup.incremental( backupPath.getPath() );
        // END SNIPPET: onlineBackup
        assertEquals( furtherRepresentation, DbRepresentation.of( backupPath ) );
        shutdownServer( server );
    }

    @Test
    public void makeSureNoLogFileRemains() throws Exception
    {
        createInitialDataSet( serverPath );
        ServerInterface server = startServer( serverPath );
        OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );

        // First check full
        backup.full( backupPath.getPath() );
        assertFalse( checkLogFileExistence( backupPath.getPath() ) );
        // Then check empty incremental
        backup.incremental( backupPath.getPath() );
        assertFalse( checkLogFileExistence( backupPath.getPath() ) );
        // Then check real incremental
        shutdownServer( server );
        addMoreData( serverPath );
        server = startServer( serverPath );
        backup.incremental( backupPath.getPath() );
        assertFalse( checkLogFileExistence( backupPath.getPath() ) );
        shutdownServer( server );
    }

    @Test
    public void makeSureStoreIdIsEnforced() throws Exception
    {
        // Create data set X on server A
        DbRepresentation initialDataSetRepresentation = createInitialDataSet( serverPath );
        ServerInterface server = startServer( serverPath );

        // Grab initial backup from server A
        OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );
        backup.full( backupPath.getPath() );
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
            backup.incremental( backupPath.getPath() );
            fail( "Shouldn't work" );
        }
        catch ( MismatchingStoreIdException e )
        { // Good
        }
        shutdownServer( server );
        // Just make sure incremental backup can be received properly from
        // server A, even after a failed attempt from server B
        DbRepresentation furtherRepresentation = addMoreData( serverPath );
        server = startServer( serverPath );
        backup.incremental( backupPath.getPath() );
        assertEquals( furtherRepresentation, DbRepresentation.of( backupPath ) );
        shutdownServer( server );
    }

    private ServerInterface startServer( File path ) throws Exception
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
        ServerInterface server = new EmbeddedServer( path.getPath(), "127.0.0.1:6362" );
        server.awaitStarted();
        return server;
    }

    private void shutdownServer( ServerInterface server ) throws Exception
    {
        server.shutdown();
        Thread.sleep( 1000 );
    }

    private DbRepresentation addMoreData( File path )
    {
        GraphDatabaseService db = startGraphDatabase( path );
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.setProperty( "backup", "Is great" );
        db.createNode().createRelationshipTo( node,
                DynamicRelationshipType.withName( "LOVES" ) );
        tx.success();
        tx.finish();
        DbRepresentation result = DbRepresentation.of( db );
        db.shutdown();
        return result;
    }

    private GraphDatabaseService startGraphDatabase( File path )
    {
        return new GraphDatabaseFactory().
            newEmbeddedDatabaseBuilder( path.getPath() ).
            setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE ).
            setConfig( GraphDatabaseSettings.keep_logical_logs, Settings.TRUE ).
            newGraphDatabase();
    }

    private DbRepresentation createInitialDataSet( File path )
    {
        GraphDatabaseService db = startGraphDatabase( path );
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.setProperty( "myKey", "myValue" );
        Index<Node> nodeIndex = db.index().forNodes( "db-index" );
        nodeIndex.add( node, "myKey", "myValue" );
        db.createNode().createRelationshipTo( node,
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
            db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( serverPath.getPath() ).
                setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE ).
                newGraphDatabase();

            Transaction tx = db.beginTx();
            Index<Node> index = db.index().forNodes( "yo" );
            index.add( db.createNode(), "justTo", "commitATx" );
            tx.success();
            tx.finish();

            OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );
            backup.full( backupPath.getPath() );
            long lastCommittedTxForLucene = getLastCommittedTx( backupPath.getPath() );

            for ( int i = 0; i < 5; i++ )
            {
                tx = db.beginTx();
                Node node = db.createNode();
                index.add( node, "key", "value" + i );
                tx.success();
                tx.finish();
                backup = backup.incremental( backupPath.getPath() );
                assertTrue( "Should be consistent", backup.isConsistent() );
                assertEquals( lastCommittedTxForLucene + i + 1,
                        getLastCommittedTx( backupPath.getPath() ) );
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
            db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( serverPath.getPath() ).
                setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE ).
                newGraphDatabase();

            Transaction transaction = db.beginTx();
            try
            {
                db.index().forNodes( "created-no-commits" );
                transaction.success();
            }
            finally
            {
                transaction.finish();
            }

            OnlineBackup backup = OnlineBackup.from( "127.0.0.1" );
            backup.full( backupPath.getPath() );
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }

    @SuppressWarnings("deprecation")
    private long getLastCommittedTx( String path )
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( path );
        try
        {
            XaDataSource ds = ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( XaDataSourceManager.class ).getNeoStoreDataSource();
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
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( serverPath.getPath() ).
            setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE ).
            newGraphDatabase();

        Transaction tx = db.beginTx();
        Index<Node> index;
        Node node;
        try
        {
            index = db.index().forNodes( key );
            node = db.createNode();
            node.setProperty( key, value );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        OnlineBackup.from( "127.0.0.1" ).full( backupPath.getPath() );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( backupPath ) );
        FileUtils.deleteDirectory( new File( backupPath.getPath() ) );
        OnlineBackup.from( "127.0.0.1" ).full( backupPath.getPath() );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( backupPath ) );

        tx = db.beginTx();
        try
        {
            index.add( node, key, value );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        FileUtils.deleteDirectory( new File( backupPath.getPath() ) );
        OnlineBackup backup = OnlineBackup.from( "127.0.0.1" ).full( backupPath.getPath() );
        assertTrue( "Should be consistent", backup.isConsistent() );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( backupPath ) );
        db.shutdown();
    }

    @Test
    public void shouldRetainFileLocksAfterFullBackupOnLiveDatabase() throws Exception
    {
        String sourcePath = "target/var/serverdb-lock";
        FileUtils.deleteDirectory( new File( sourcePath ) );

        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( sourcePath ).
            setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE ).
            newGraphDatabase();
        try
        {
            assertStoreIsLocked( sourcePath );
            OnlineBackup.from( "127.0.0.1" ).full( backupPath.getPath() );
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
            new GraphDatabaseFactory().newEmbeddedDatabase( path).shutdown();
            fail( "Could start up database in same process, store not locked" );
        }
        catch ( RuntimeException ex )
        {
            assertThat( ex.getCause().getCause(), instanceOf( StoreLockException.class ) );
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
            GraphDatabaseService db = null;
            try
            {
                db = new GraphDatabaseFactory().newEmbeddedDatabase( path );
            }
            catch ( RuntimeException ex )
            {
                if (ex.getCause().getCause() instanceof StoreLockException )
                {
                    state = ex;
                    return;
                }
            }
            state = new Object();
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }

    private static boolean checkLogFileExistence( String directory )
    {
        return new File( directory, StringLogger.DEFAULT_NAME ).exists();
    }
}
