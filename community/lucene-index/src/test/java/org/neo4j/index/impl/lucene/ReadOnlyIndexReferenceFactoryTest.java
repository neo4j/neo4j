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
package org.neo4j.index.impl.lucene;


import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertSame;

public class ReadOnlyIndexReferenceFactoryTest
{
    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Rule
    public CleanupRule cleanupRule = new CleanupRule();

    private static final String INDEX_NAME = "testIndex";

    private LuceneDataSource.LuceneFilesystemFacade filesystemFacade = LuceneDataSource.LuceneFilesystemFacade.FS;
    private IndexIdentifier indexIdentifier = new IndexIdentifier( IndexEntityType.Node, INDEX_NAME );
    private IndexConfigStore indexStore;

    @Before
    public void setUp() throws IOException
    {
        setupIndexInfrastructure();
    }

    @Test
    public void createReadOnlyIndexReference() throws Exception
    {
        ReadOnlyIndexReferenceFactory indexReferenceFactory = getReadOnlyIndexReferenceFactory();
        IndexReference indexReference = indexReferenceFactory.createIndexReference( indexIdentifier );
        cleanupRule.add( indexReference );

        expectedException.expect( UnsupportedOperationException.class );
        indexReference.getWriter();
    }

    @Test
    public void refreshReadOnlyIndexReference() throws IOException
    {
        ReadOnlyIndexReferenceFactory indexReferenceFactory = getReadOnlyIndexReferenceFactory();
        IndexReference indexReference = indexReferenceFactory.createIndexReference( indexIdentifier );
        cleanupRule.add( indexReference );

        IndexReference refreshedIndex = indexReferenceFactory.refresh( indexReference );
        assertSame("Refreshed instance should be the same.", indexReference, refreshedIndex);
    }

    private void setupIndexInfrastructure() throws IOException
    {
        DefaultFileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();
        File storeDir = getStoreDir();
        indexStore = new IndexConfigStore( storeDir, fileSystemAbstraction );
        indexStore.set( Node.class, INDEX_NAME, MapUtil.stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" ) );
        LuceneDataSource luceneDataSource = new LuceneDataSource( storeDir, new Config( MapUtil.stringMap() ),
                indexStore, fileSystemAbstraction );
        try
        {
            luceneDataSource.init();
            luceneDataSource.getIndexSearcher( indexIdentifier );
        }
        finally
        {
            luceneDataSource.shutdown();
        }
    }

    private ReadOnlyIndexReferenceFactory getReadOnlyIndexReferenceFactory()
    {
        return new ReadOnlyIndexReferenceFactory( filesystemFacade, new File( getStoreDir(), "index"),
                new IndexTypeCache( indexStore ) );
    }

    private File getStoreDir()
    {
        return testDirectory.directory();
    }
}