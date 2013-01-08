/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.nioneo.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.util.FileUtils;
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
            setupGraphDatabase(description.getTestClass().getAnnotation( RequiresPersistentGraphDatabase.class ).value());
            return base;
        }
    };

    private static GraphDatabaseAPI graphDb;
    private Transaction tx;

    private static boolean requiresPersistentGraphDatabase = false;

    public GraphDatabaseService getGraphDb()
    {
        return graphDb;
    }

    private static void setupGraphDatabase( boolean requiresPersistentGraphDatabase )
    {
        AbstractNeo4jTestCase.requiresPersistentGraphDatabase  = requiresPersistentGraphDatabase;
        if ( requiresPersistentGraphDatabase )
        {
            try
            {
                FileUtils.deleteRecursively( new File( getStorePath( "neo-test" ) ) );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
        
        graphDb = (GraphDatabaseAPI) (requiresPersistentGraphDatabase ?
                                      new TestGraphDatabaseFactory().newEmbeddedDatabase( getStorePath( "neo-test" ) ) :
                                      new TestGraphDatabaseFactory().newImpermanentDatabase());
    }

    public GraphDatabaseAPI getEmbeddedGraphDb()
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

    public static String getStorePath( String endPath )
    {
        return new File( NEO4J_BASE_DIR, endPath ).getAbsolutePath();
    }

    @Before
    public void setUpTest()
    {
        if ( restartGraphDbBetweenTests() && graphDb == null )
        {
            setupGraphDatabase(requiresPersistentGraphDatabase);
        }
        tx = graphDb.beginTx();
    }

    @After
    public void tearDownTest()
    {
        if ( tx != null )
        {
            tx.finish();
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
            if ( graphDb != null ) graphDb.shutdown();
        }
        finally
        {
            graphDb = null;
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
            tx.finish();
        }
        tx = graphDb.beginTx();
        return tx;
    }

    public void commit()
    {
        if ( tx != null )
        {
            tx.success();
            tx.finish();
            tx = null;
        }
    }

    public void rollback()
    {
        if ( tx != null )
        {
            tx.failure();
            tx.finish();
            tx = null;
        }
    }

    public NodeManager getNodeManager()
    {
        return graphDb.getNodeManager();
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

    protected void clearCache()
    {
        getEmbeddedGraphDb().getNodeManager().clearCache();
    }

    protected long propertyRecordsInUse()
    {
        return propertyStore().getNumberOfIdsInUse();
    }

    protected long dynamicStringRecordsInUse()
    {
        return dynamicRecordsInUse( "stringPropertyStore" );
    }

    protected long dynamicArrayRecordsInUse()
    {
        return dynamicRecordsInUse( "arrayPropertyStore" );
    }
    
    private long dynamicRecordsInUse( String fieldName )
    {
        try
        {
            Field storeField = PropertyStore.class.getDeclaredField( fieldName );
            storeField.setAccessible( true );
            return ( (AbstractDynamicStore) storeField.get( propertyStore() ) ).getNumberOfIdsInUse();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
    
    protected PropertyStore propertyStore()
    {
        XaDataSourceManager dsMgr = graphDb.getXaDataSourceManager();
        return dsMgr.getNeoStoreDataSource().getXaConnection().getPropertyStore();
    }
}