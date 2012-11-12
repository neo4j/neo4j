/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import java.lang.reflect.Field;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.nioneo.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaConnection;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;

public abstract class AbstractNeo4jTestCase
{
    protected static final File NEO4J_BASE_DIR = new File( "target", "var" );

    private static GraphDatabaseService graphDb;
    private Transaction tx;

    public GraphDatabaseService getGraphDb()
    {
        return graphDb;
    }

    public EmbeddedGraphDatabase getEmbeddedGraphDb()
    {
        return (EmbeddedGraphDatabase) graphDb;
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

    @BeforeClass
    public static void setUpDb()
    {
        deleteFileOrDirectory( new File( getStorePath( "neo-test" ) ) );
        graphDb = new EmbeddedGraphDatabase( getStorePath( "neo-test" ) );
    }

    @Before
    public void setUpTest()
    {
        if ( restartGraphDbBetweenTests() && graphDb == null )
        {
            setUpDb();
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
            graphDb.shutdown();
            graphDb = null;
        }
    }

    @AfterClass
    public static void tearDownDb()
    {
        if ( graphDb != null )
        {
            graphDb.shutdown();
        }
    }

    public void setTransaction( Transaction tx )
    {
        this.tx = tx;
    }

    public void newTransaction()
    {
        if ( tx != null )
        {
            tx.success();
            tx.finish();
        }
        tx = graphDb.beginTx();
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
        return ((EmbeddedGraphDatabase) graphDb).getConfig().getGraphDbModule().getNodeManager();
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
        getEmbeddedGraphDb().getConfig().getGraphDbModule()
            .getNodeManager().clearCache();
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
        XaDataSourceManager dsMgr = ((AbstractGraphDatabase)graphDb).getConfig().getTxModule().getXaDataSourceManager();
        return ( (NeoStoreXaConnection) dsMgr.getXaDataSource( "nioneodb" ).getXaConnection() ).getPropertyStore();
    }
}