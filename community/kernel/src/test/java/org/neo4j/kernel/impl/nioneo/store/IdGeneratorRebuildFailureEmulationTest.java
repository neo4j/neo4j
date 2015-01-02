/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.BreakpointHandler;
import org.neo4j.test.subprocess.BreakpointTrigger;
import org.neo4j.test.subprocess.DebugInterface;
import org.neo4j.test.subprocess.EnabledBreakpoints;
import org.neo4j.test.subprocess.ForeignBreakpoints;
import org.neo4j.test.subprocess.SubProcessTestRunner;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Suite.class)
@SuiteClasses({IdGeneratorRebuildFailureEmulationTest.FailureBeforeRebuild.class,
        IdGeneratorRebuildFailureEmulationTest.FailureDuringRebuild.class})
public class IdGeneratorRebuildFailureEmulationTest
{
    @RunWith(JUnit4.class)
    public static final class FailureBeforeRebuild extends IdGeneratorRebuildFailureEmulationTest
    {
        @Override
        protected void emulateFailureOnRebuildOf( NeoStore neostore )
        {
            // emulate a failure during rebuild by not issuing this call:
            // neostore.makeStoreOk();
        }
    }

    @RunWith(SubProcessTestRunner.class)
    @ForeignBreakpoints(@ForeignBreakpoints.BreakpointDef(
            type = "org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl", method = "setHighId"))
    public static final class FailureDuringRebuild extends IdGeneratorRebuildFailureEmulationTest
    {
        @Override
        protected void emulateFailureOnRebuildOf( NeoStore neostore )
        {
            // emulate a failure (Id capacity exceeded) during rebuild by breakpoints in this method:
            neostore.makeStoreOk();
            fail( "makeStoreOk should have thrown UnderlyingStorageException" );
        }

        @BreakpointHandler("performTest")
        public static void bootstrapTest( @BreakpointHandler("setHighId") BreakPoint setHighId )
        {
            setHighId.enable();
        }

        @SuppressWarnings("boxing")
        @BreakpointHandler("setHighId")
        public static void on_setHighId( DebugInterface di, BreakPoint setHighId )
        {
            if ( setHighId.invocationCount() > 1
                    || RelationshipTypeTokenStore.class.getName().equals( di.thread().getStackTrace()
                    [2].getClassName() ) )
            {
                setHighId.disable();
                // emulate a failure in recovery by changing the id parameter to setHighId(id) to an invalid value,
                // causing an exception to be thrown.
                di.setLocalVariable( "id", -1 );
            }
        }
    }

    @BreakpointTrigger
    private void performTest() throws Exception
    {
        String file = prefix + File.separator + Thread.currentThread().getStackTrace()[2].getMethodName().replace(
                '_', '.' );
        // emulate the need for rebuilding id generators by deleting it
        fs.deleteFile( new File( file + ".id") );
        NeoStore neostore = null;
        try
        {
            neostore = factory.newNeoStore( new File( prefix + File.separator + "neostore") );
            // emulate a failure during rebuild:
            emulateFailureOnRebuildOf( neostore );
        }
        catch ( UnderlyingStorageException expected )
        {
            assertEquals( "Id capacity exceeded", expected.getMessage() );
        }
        finally
        {
            // we want close to not misbehave
            // (and for example truncate the file based on the wrong highId)
            if ( neostore != null )
            {
                neostore.close();
            }
        }
    }

    void emulateFailureOnRebuildOf( NeoStore neostore )
    {
        fail( "emulateFailureOnRebuildOf(NeoStore) must be overridden" );
    }

    private FileSystem fs;
    private StoreFactory factory;
    private String prefix;

    @Before
    public void initialize()
    {
        fs = new FileSystem();
        InternalAbstractGraphDatabase graphdb = new Database();
        prefix = graphdb.getStoreDir();
        createInitialData( graphdb );
        graphdb.shutdown();
        Map<String, String> config = new HashMap<String, String>();
        config.put( GraphDatabaseSettings.rebuild_idgenerators_fast.name(), Settings.FALSE );
        config.put( GraphDatabaseSettings.store_dir.name(), prefix );
        factory = new StoreFactory( new Config( config, GraphDatabaseSettings.class ),
                new DefaultIdGeneratorFactory(), new DefaultWindowPoolFactory(), fs, StringLogger.DEV_NULL, null );
    }

    @After
    public void verifyAndDispose() throws Exception
    {
        try
        {
            InternalAbstractGraphDatabase graphdb = new Database();
            verifyData( graphdb );
            graphdb.shutdown();
        }
        finally
        {
            if ( fs != null )
            {
                fs.disposeAndAssertNoOpenFiles();
            }
            fs = null;
        }
    }

    private void verifyData( GraphDatabaseService graphdb )
    {
        Transaction tx = graphdb.beginTx();
        try
        {
            int nodecount = 0;
            for ( Node node : GlobalGraphOperations.at( graphdb ).getAllNodes() )
            {
                int propcount = readProperties( node );
                int relcount = 0;
                for ( Relationship rel : node.getRelationships() )
                {
                    assertEquals( "all relationships should have 3 properties.", 3, readProperties( rel ) );
                    relcount++;
                }
                assertEquals( "all created nodes should have 3 properties.", 3, propcount );
                assertEquals( "all created nodes should have 2 relationships.", 2, relcount );

                nodecount++;
            }
            assertEquals( "The database should have 2 nodes.", 2, nodecount );
        }
        finally
        {
            tx.finish();
        }
    }

    private void createInitialData( GraphDatabaseService graphdb )
    {
        Transaction tx = graphdb.beginTx();
        try
        {
            Node first = properties( graphdb.createNode() );
            Node other = properties( graphdb.createNode() );
            properties( first.createRelationshipTo( other, DynamicRelationshipType.withName( "KNOWS" ) ) );
            properties( other.createRelationshipTo( first, DynamicRelationshipType.withName( "DISTRUSTS" ) ) );

            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private <E extends PropertyContainer> E properties( E entity )
    {
        entity.setProperty( "short thing", "short" );
        entity.setProperty( "long thing",
                "this is quite a long string, don't you think, it sure is long enough at least" );
        entity.setProperty( "string array", new String[]{"these are a few", "cool strings",
                "for your viewing pleasure"} );
        return entity;
    }

    private int readProperties( PropertyContainer entity )
    {
        int count = 0;
        for ( String key : entity.getPropertyKeys() )
        {
            entity.getProperty( key );
            count++;
        }
        return count;
    }

    private static class FileSystem extends EphemeralFileSystemAbstraction
    {
        void disposeAndAssertNoOpenFiles() throws Exception
        {
            //Collection<String> open = openFiles();
            //assertTrue( "Open files: " + open, open.isEmpty() );
            assertNoOpenFiles();
            super.shutdown();
        }

        @Override
        public void shutdown()
        {
            // no-op, it's pretty odd to have EphemeralFileSystemAbstraction implement Lifecycle by default
        }
    }

    @SuppressWarnings("deprecation")
    private class Database extends ImpermanentGraphDatabase
    {
        @Override
        protected FileSystemAbstraction createFileSystemAbstraction()
        {
            return fs;
        }

        @Override
        protected IdGeneratorFactory createIdGeneratorFactory()
        {
            return new DefaultIdGeneratorFactory();
        }
    }

    @EnabledBreakpoints("performTest")
    @Test
    public void neostore() throws Exception
    {
        performTest();
    }

    @EnabledBreakpoints("performTest")
    @Test
    public void neostore_nodestore_db() throws Exception
    {
        performTest();
    }

    @EnabledBreakpoints("performTest")
    @Test
    public void neostore_propertystore_db_arrays() throws Exception
    {
        performTest();
    }

    @EnabledBreakpoints("performTest")
    @Test
    public void neostore_propertystore_db() throws Exception
    {
        performTest();
    }

    @EnabledBreakpoints("performTest")
    @Test
    public void neostore_propertystore_db_index() throws Exception
    {
        performTest();
    }

    @EnabledBreakpoints("performTest")
    @Test
    public void neostore_propertystore_db_index_keys() throws Exception
    {
        performTest();
    }

    @EnabledBreakpoints("performTest")
    @Test
    public void neostore_propertystore_db_strings() throws Exception
    {
        performTest();
    }

    @EnabledBreakpoints("performTest")
    @Test
    public void neostore_relationshipstore_db() throws Exception
    {
        performTest();
    }

    @EnabledBreakpoints("performTest")
    @Test
    public void neostore_relationshiptypestore_db() throws Exception
    {
        performTest();
    }

    @EnabledBreakpoints("performTest")
    @Test
    public void neostore_relationshiptypestore_db_names() throws Exception
    {
        performTest();
    }

    private IdGeneratorRebuildFailureEmulationTest()
    {
        if ( IdGeneratorRebuildFailureEmulationTest.class == getClass() )
        {
            throw new UnsupportedOperationException( "This class is effectively abstract" );
        }
    }
}
