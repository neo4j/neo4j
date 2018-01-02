/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.test.TestGraphDatabaseFactory;

@AbstractNeo4jTestCase.RequiresPersistentGraphDatabase( false )
public abstract class AbstractNeo4jTestCase
{
    @Retention( RetentionPolicy.RUNTIME )
    @Target( ElementType.TYPE )
    @Inherited
    public @interface RequiresPersistentGraphDatabase
    {
        boolean value() default true;
    }

    protected static final File NEO4J_BASE_DIR = new File( "target", "var" );

    public static final @ClassRule TestRule START_GRAPHDB = new TestRule()
    {
        @Override
        public Statement apply( Statement base, Description description )
        {
            tearDownDb();
            setupGraphDatabase( description.getTestClass().getName(),
                    description.getTestClass().getAnnotation( RequiresPersistentGraphDatabase.class ).value() );
            return base;
        }
    };

    private static ThreadLocal<GraphDatabaseAPI> threadLocalGraphDb = new ThreadLocal<>();
    private static ThreadLocal<String> currentTestClassName = new ThreadLocal<>();
    private static ThreadLocal<Boolean> requiresPersistentGraphDatabase = new ThreadLocal<>();

    private GraphDatabaseAPI graphDb;

    private Transaction tx;

    protected AbstractNeo4jTestCase()
    {
        graphDb = threadLocalGraphDb.get();
    }

    public GraphDatabaseService getGraphDb()
    {
        return graphDb;
    }

    private static void setupGraphDatabase( String testClassName, boolean requiresPersistentGraphDatabase )
    {
        AbstractNeo4jTestCase.requiresPersistentGraphDatabase.set( requiresPersistentGraphDatabase );
        AbstractNeo4jTestCase.currentTestClassName.set( testClassName );
        if ( requiresPersistentGraphDatabase )
        {
            try
            {
                FileUtils.deleteRecursively( getStorePath( "neo-test" ) );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }

        threadLocalGraphDb.set( (GraphDatabaseAPI) (requiresPersistentGraphDatabase ?
                                                    new TestGraphDatabaseFactory().newEmbeddedDatabase( getStorePath(
                                                            "neo-test" ) ) :
                                                    new TestGraphDatabaseFactory().newImpermanentDatabase()) );
    }

    public GraphDatabaseAPI getGraphDbAPI()
    {
        return graphDb;
    }

    protected boolean restartGraphDbBetweenTests()
    {
        return false;
    }

    public Transaction getTransaction()
    {
        return tx;
    }

    public static File getStorePath( String endPath )
    {
        return new File( NEO4J_BASE_DIR, currentTestClassName.get() + "-" + endPath ).getAbsoluteFile();
    }

    @Before
    public void setUpTest()
    {
        if ( restartGraphDbBetweenTests() && graphDb == null )
        {
            setupGraphDatabase( currentTestClassName.get(), requiresPersistentGraphDatabase.get() );
            graphDb = threadLocalGraphDb.get();
        }
        tx = graphDb.beginTx();
    }

    @After
    public void tearDownTest()
    {
        if ( tx != null )
        {
            tx.close();
        }

        if ( restartGraphDbBetweenTests() )
        {
            tearDownDb();
        }
    }

    @AfterClass
    public static void tearDownDb()
    {
        try
        {
            if ( threadLocalGraphDb.get() != null )
            {
                threadLocalGraphDb.get().shutdown();
            }
        }
        finally
        {
            threadLocalGraphDb.remove();
        }
    }

    public void setTransaction( Transaction tx )
    {
        this.tx = tx;
    }

    public Transaction newTransaction()
    {
        if ( tx != null )
        {
            tx.success();
            tx.close();
        }
        tx = graphDb.beginTx();
        return tx;
    }

    public void commit()
    {
        if ( tx != null )
        {
            try
            {
                tx.success();
                tx.close();
            }
            finally
            {
                tx = null;
            }
        }
    }

    public void finish()
    {
        if ( tx != null )
        {
            try
            {
                tx.close();
            }
            finally
            {
                tx = null;
            }
        }
    }

    public void rollback()
    {
        if ( tx != null )
        {
            try
            {
                tx.failure();
                tx.close();
            }
            finally
            {
                tx = null;
            }
        }
    }

    public NodeManager getNodeManager()
    {
        return graphDb.getDependencyResolver().resolveDependency( NodeManager.class );
    }

    public IdGenerator getIdGenerator( IdType idType )
    {
        return graphDb.getDependencyResolver().resolveDependency( IdGeneratorFactory.class ).get( idType );
    }

    public static void deleteFileOrDirectory( String dir )
    {
        deleteFileOrDirectory( new File( dir ) );
    }

    public static void deleteFileOrDirectory( File file )
    {
        if ( !file.exists() )
        {
            return;
        }

        if ( file.isDirectory() )
        {
            for ( File child : file.listFiles() )
            {
                deleteFileOrDirectory( child );
            }
        }
        else
        {
            file.delete();
        }
    }

    protected long propertyRecordsInUse()
    {
        return numberOfRecordsInUse( propertyStore() );
    }

    public static <RECORD extends AbstractBaseRecord> int numberOfRecordsInUse( RecordStore<RECORD> store )
    {
        int inUse = 0;
        for ( long id = store.getNumberOfReservedLowIds(); id < store.getHighId(); id++ )
        {
            RECORD record = store.forceGetRecord( id );
            if ( record.inUse() )
            {
                inUse++;
            }
        }
        return inUse;
    }

    protected long dynamicStringRecordsInUse()
    {
        return numberOfRecordsInUse( propertyStore().getStringStore() );
    }

    protected long dynamicArrayRecordsInUse()
    {
        return numberOfRecordsInUse( propertyStore().getArrayStore() );
    }

    protected PropertyStore propertyStore()
    {
        NeoStores neoStores = graphDb.getDependencyResolver().resolveDependency( NeoStoresSupplier.class ).get();
        return neoStores.getPropertyStore();
    }

    public static File unzip( File targetDir, Class<?> testClass, String resource ) throws IOException
    {
        try ( InputStream source = testClass.getResourceAsStream( resource ) )
        {
            if ( source == null )
            {
                throw new FileNotFoundException( "Could not find resource '" + resource + "' to unzip" );
            }
            ZipInputStream zipStream = new ZipInputStream( source );
            ZipEntry entry;
            byte[] scratch = new byte[8096];
            while ( (entry = zipStream.getNextEntry()) != null )
            {
                if ( entry.isDirectory() )
                {
                    new File( targetDir, entry.getName() ).mkdirs();
                }
                else
                {
                    try ( OutputStream file =
                                  new BufferedOutputStream(
                                          new FileOutputStream( new File( targetDir, entry.getName() ) ) ) )
                    {
                        long toCopy = entry.getSize();
                        while ( toCopy > 0 )
                        {
                            int read = zipStream.read( scratch );
                            file.write( scratch, 0, read );
                            toCopy -= read;
                        }
                    }
                }
                zipStream.closeEntry();
            }
        }
        return targetDir;
    }
}
