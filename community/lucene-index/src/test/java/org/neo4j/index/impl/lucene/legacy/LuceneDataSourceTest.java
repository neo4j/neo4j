/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.impl.lucene.legacy;

import org.apache.lucene.index.IndexWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LuceneDataSourceTest
{
    private final LifeRule life = new LifeRule( true );
    private final TestDirectory directory = TargetDirectory.testDirForTest( getClass() );

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( directory )
            .around( life );

    private IndexConfigStore indexStore;
    private LuceneDataSource dataSource;

    @Before
    public void setup()
    {
        indexStore = new IndexConfigStore( directory.directory(), new DefaultFileSystemAbstraction() );
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

    @Test
    public void testShouldReturnIndexWriterFromLRUCache() throws Throwable
    {
        Config config = new Config( config(), GraphDatabaseSettings.class );
        dataSource = life.add( new LuceneDataSource( directory.graphDbDir(), config, indexStore, new DefaultFileSystemAbstraction() ) );
        IndexIdentifier identifier = identifier( "foo" );
        IndexWriter writer = dataSource.getIndexSearcher( identifier ).getWriter();
        assertSame( writer, dataSource.getIndexSearcher( identifier ).getWriter() );
    }

    @Test
    public void testShouldReturnIndexSearcherFromLRUCache() throws Throwable
    {
        Config config = new Config( config(), GraphDatabaseSettings.class );
        dataSource = life.add( new LuceneDataSource( directory.graphDbDir(), config, indexStore, new DefaultFileSystemAbstraction() ) );
        IndexIdentifier identifier = identifier( "foo" );
        IndexReference searcher = dataSource.getIndexSearcher( identifier );
        assertSame( searcher, dataSource.getIndexSearcher( identifier ) );
        searcher.close();
    }

    @Test
    public void testClosesOldestIndexWriterWhenCacheSizeIsExceeded() throws Throwable
    {
        addIndex( "bar" );
        addIndex( "baz" );
        Map<String, String> configMap = config();
        configMap.put( GraphDatabaseSettings.lucene_searcher_cache_size.name(), "2" );
        Config config = new Config( configMap, GraphDatabaseSettings.class );
        dataSource = life.add( new LuceneDataSource( directory.graphDbDir(), config, indexStore, new DefaultFileSystemAbstraction() ) );
        IndexIdentifier fooIdentifier = identifier( "foo" );
        IndexIdentifier barIdentifier = identifier( "bar" );
        IndexIdentifier bazIdentifier = identifier( "baz" );
        IndexWriter fooIndexWriter = dataSource.getIndexSearcher( fooIdentifier ).getWriter();
        dataSource.getIndexSearcher( barIdentifier );
        assertTrue( fooIndexWriter.isOpen() );
        dataSource.getIndexSearcher( bazIdentifier );
        assertFalse( fooIndexWriter.isOpen() );
    }

    @Test
    public void testClosesOldestIndexSearcherWhenCacheSizeIsExceeded() throws Throwable
    {
        addIndex( "bar" );
        addIndex( "baz" );
        Map<String, String> configMap = config();
        configMap.put( GraphDatabaseSettings.lucene_searcher_cache_size.name(), "2" );
        Config config = new Config( configMap, GraphDatabaseSettings.class );
        dataSource = life.add( new LuceneDataSource( directory.graphDbDir(), config, indexStore, new DefaultFileSystemAbstraction() ) );
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
    public void testRecreatesSearcherWhenRequestedAgain() throws Throwable
    {
        addIndex( "bar" );
        addIndex( "baz" );
        Map<String, String> configMap = config();
        configMap.put( GraphDatabaseSettings.lucene_searcher_cache_size.name(), "2" );
        Config config = new Config( configMap, GraphDatabaseSettings.class );
        dataSource = life.add( new LuceneDataSource( directory.graphDbDir(), config, indexStore, new DefaultFileSystemAbstraction() ) );
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
    public void testRecreatesWriterWhenRequestedAgainAfterCacheEviction() throws Throwable
    {
        addIndex( "bar" );
        addIndex( "baz" );
        Map<String, String> configMap = config();
        configMap.put( GraphDatabaseSettings.lucene_searcher_cache_size.name(), "2" );
        Config config = new Config( configMap, GraphDatabaseSettings.class );
        dataSource = life.add( new LuceneDataSource( directory.graphDbDir(), config, indexStore, new DefaultFileSystemAbstraction() ) );
        IndexIdentifier fooIdentifier = identifier( "foo" );
        IndexIdentifier barIdentifier = identifier( "bar" );
        IndexIdentifier bazIdentifier = identifier( "baz" );
        IndexWriter oldFooIndexWriter = dataSource.getIndexSearcher( fooIdentifier ).getWriter();
        dataSource.getIndexSearcher( barIdentifier );
        dataSource.getIndexSearcher( bazIdentifier );
        IndexWriter newFooIndexWriter = dataSource.getIndexSearcher( fooIdentifier ).getWriter();
        assertNotSame( oldFooIndexWriter, newFooIndexWriter );
        assertTrue( newFooIndexWriter.isOpen() );
    }

    private Map<String, String> config()
    {
        return MapUtil.stringMap( "store_dir", directory.directory().getPath() );
    }
}
