/*
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
package org.neo4j.index.impl.lucene;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterAccessor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexEntityType;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LuceneDataSourceTest
{
    private IndexConfigStore indexStore;
    private LuceneDataSource dataSource;
    File dbPath = getDbPath();

    private File getDbPath()
    {
        return new File("target/var/datasource" + System.currentTimeMillis());
    }

    @Before
    public void setup()
    {
        dbPath.mkdirs();
        indexStore = new IndexConfigStore( dbPath, new DefaultFileSystemAbstraction() );
        addIndex( "foo" );
    }

    private void addIndex( String name )
    {
        indexStore.set( Node.class, name, MapUtil.stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" ) );
    }

    private IndexIdentifier identifier( String name )
    {
        return new IndexIdentifier( IndexEntityType.Node, name );
    }

    @After
    public void tearDown() throws IOException
    {
        dataSource.stop();
        FileUtils.deleteRecursively( dbPath );
    }

    @Test
    public void testShouldReturnIndexWriterFromLRUCache() throws InstantiationException
    {
        Config config = new Config( config(), GraphDatabaseSettings.class );
        dataSource = new LuceneDataSource( config, indexStore, new DefaultFileSystemAbstraction());
        dataSource.start();
        IndexIdentifier identifier = identifier( "foo" );
        IndexWriter writer = dataSource.getIndexSearcher( identifier ).getWriter();
        assertSame( writer, dataSource.getIndexSearcher( identifier ).getWriter() );
    }

    @Test
    public void testShouldReturnIndexSearcherFromLRUCache() throws InstantiationException, IOException
    {
        Config config = new Config( config(), GraphDatabaseSettings.class );
        dataSource = new LuceneDataSource( config, indexStore, new DefaultFileSystemAbstraction() );
        dataSource.start();
        IndexIdentifier identifier = identifier( "foo" );
        IndexReference searcher = dataSource.getIndexSearcher( identifier );
        assertSame( searcher, dataSource.getIndexSearcher( identifier ) );
        searcher.close();
    }

    @Test
    public void testClosesOldestIndexWriterWhenCacheSizeIsExceeded() throws InstantiationException
    {
        addIndex( "bar" );
        addIndex( "baz" );
        Map<String, String> configMap = config();
        configMap.put( GraphDatabaseSettings.lucene_searcher_cache_size.name(), "2" );
        Config config = new Config( configMap, GraphDatabaseSettings.class );
        dataSource = new LuceneDataSource( config, indexStore, new DefaultFileSystemAbstraction() );
        dataSource.start();
        IndexIdentifier fooIdentifier = identifier( "foo" );
        IndexIdentifier barIdentifier = identifier( "bar" );
        IndexIdentifier bazIdentifier = identifier( "baz" );
        IndexWriter fooIndexWriter = dataSource.getIndexSearcher( fooIdentifier ).getWriter();
        dataSource.getIndexSearcher( barIdentifier );
        assertFalse( IndexWriterAccessor.isClosed( fooIndexWriter ) );
        dataSource.getIndexSearcher( bazIdentifier );
        assertTrue( IndexWriterAccessor.isClosed( fooIndexWriter ) );
    }

    @Test
    public void testClosesOldestIndexSearcherWhenCacheSizeIsExceeded() throws InstantiationException, IOException
    {
        addIndex( "bar" );
        addIndex( "baz" );
        Map<String, String> configMap = config();
        configMap.put( GraphDatabaseSettings.lucene_searcher_cache_size.name(), "2" );
        Config config = new Config( configMap, GraphDatabaseSettings.class );
        dataSource = new LuceneDataSource( config, indexStore, new DefaultFileSystemAbstraction() );
        dataSource.start();
        IndexIdentifier fooIdentifier = identifier( "foo" );
        IndexIdentifier barIdentifier = identifier( "bar" );
        IndexIdentifier bazIdentifier = identifier( "baz" );
        IndexReference fooSearcher = dataSource.getIndexSearcher( fooIdentifier );
        IndexReference barSearcher = dataSource.getIndexSearcher( barIdentifier );
        assertFalse( fooSearcher.isClosed() );
        IndexReference bazSearcher = dataSource.getIndexSearcher( bazIdentifier );
        assertTrue( fooSearcher.isClosed() );
        barSearcher.close();
        bazSearcher.close();
    }

    @Test
    public void testRecreatesSearcherWhenRequestedAgain() throws InstantiationException, IOException
    {
        addIndex( "bar" );
        addIndex( "baz" );
        Map<String, String> configMap = config();
        configMap.put( GraphDatabaseSettings.lucene_searcher_cache_size.name(), "2" );
        Config config = new Config( configMap, GraphDatabaseSettings.class );
        dataSource = new LuceneDataSource( config, indexStore, new DefaultFileSystemAbstraction() );
        dataSource.start();
        IndexIdentifier fooIdentifier = identifier( "foo" );
        IndexIdentifier barIdentifier = identifier( "bar" );
        IndexIdentifier bazIdentifier = identifier( "baz" );
        IndexReference oldFooSearcher = dataSource.getIndexSearcher( fooIdentifier );
        IndexReference barSearcher = dataSource.getIndexSearcher( barIdentifier );
        IndexReference bazSearcher = dataSource.getIndexSearcher( bazIdentifier );
        IndexReference newFooSearcher = dataSource.getIndexSearcher( bazIdentifier );
        assertNotSame( oldFooSearcher, newFooSearcher );
        assertFalse( newFooSearcher.isClosed() );
        oldFooSearcher.close();
        barSearcher.close();
        bazSearcher.close();
        newFooSearcher.close();
    }

    @Test
    public void testRecreatesWriterWhenRequestedAgainAfterCacheEviction() throws InstantiationException
    {
        addIndex( "bar" );
        addIndex( "baz" );
        Map<String, String> configMap = config();
        configMap.put( GraphDatabaseSettings.lucene_searcher_cache_size.name(), "2" );
        Config config = new Config( configMap, GraphDatabaseSettings.class );
        dataSource = new LuceneDataSource( config, indexStore, new DefaultFileSystemAbstraction() );
        dataSource.start();
        IndexIdentifier fooIdentifier = identifier( "foo" );
        IndexIdentifier barIdentifier = identifier( "bar" );
        IndexIdentifier bazIdentifier = identifier( "baz" );
        IndexWriter oldFooIndexWriter = dataSource.getIndexSearcher( fooIdentifier ).getWriter();
        dataSource.getIndexSearcher( barIdentifier );
        dataSource.getIndexSearcher( bazIdentifier );
        IndexWriter newFooIndexWriter = dataSource.getIndexSearcher( fooIdentifier ).getWriter();
        assertNotSame( oldFooIndexWriter, newFooIndexWriter );
        assertFalse( IndexWriterAccessor.isClosed( newFooIndexWriter ) );
    }

    private Map<String, String> config()
    {
        return MapUtil.stringMap("store_dir", getDbPath().getPath() );
    }
}
