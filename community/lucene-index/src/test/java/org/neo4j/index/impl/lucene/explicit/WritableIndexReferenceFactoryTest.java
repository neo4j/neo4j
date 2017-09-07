/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.index.impl.lucene.explicit;


import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class WritableIndexReferenceFactoryTest
{

    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final CleanupRule cleanupRule = new CleanupRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( testDirectory )
            .around( cleanupRule ).around( fileSystemRule );

    private static final String INDEX_NAME = "testIndex";

    private LuceneDataSource.LuceneFilesystemFacade filesystemFacade = LuceneDataSource.LuceneFilesystemFacade.MEMORY;
    private IndexIdentifier indexIdentifier = new IndexIdentifier( IndexEntityType.Node, INDEX_NAME );
    private IndexConfigStore indexStore;

    @Before
    public void setUp() throws IOException
    {
        setupIndexInfrastructure();
    }

    @Test
    public void createWritableIndexReference() throws Exception
    {
        WritableIndexReferenceFactory indexReferenceFactory = createFactory();
        IndexReference indexReference = createIndexReference( indexReferenceFactory );

        assertNotNull( "Index should have writer.", indexReference.getWriter() );
    }

    @Test
    public void refreshNotChangedWritableIndexReference() throws Exception
    {
        WritableIndexReferenceFactory indexReferenceFactory = createFactory();
        IndexReference indexReference = createIndexReference( indexReferenceFactory );

        IndexReference refreshedInstance = indexReferenceFactory.refresh( indexReference );
        assertSame( indexReference, refreshedInstance );
    }

    @Test
    public void refreshChangedWritableIndexReference() throws Exception
    {
        WritableIndexReferenceFactory indexReferenceFactory = createFactory();
        IndexReference indexReference = createIndexReference( indexReferenceFactory );

        writeSomething( indexReference );

        IndexReference refreshedIndexReference = indexReferenceFactory.refresh( indexReference );
        cleanupRule.add( refreshedIndexReference );

        assertNotSame( "Should return new refreshed index reference.", indexReference, refreshedIndexReference );
    }

    private void writeSomething( IndexReference indexReference ) throws IOException
    {
        IndexWriter writer = indexReference.getWriter();
        writer.addDocument( new Document() );
        writer.commit();
    }

    private IndexReference createIndexReference( WritableIndexReferenceFactory indexReferenceFactory ) throws IOException
    {
        IndexReference indexReference = indexReferenceFactory.createIndexReference( indexIdentifier );
        cleanupRule.add( indexReference );
        return indexReference;
    }

    private WritableIndexReferenceFactory createFactory()
    {
        return new WritableIndexReferenceFactory( filesystemFacade, new File( getStoreDir(), "index" ),
                new IndexTypeCache( indexStore ) );
    }

    private void setupIndexInfrastructure() throws IOException
    {
        File storeDir = getStoreDir();
        indexStore = new IndexConfigStore( storeDir, fileSystemRule.get() );
        indexStore.set( Node.class, INDEX_NAME, MapUtil.stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" ) );
    }

    private File getStoreDir()
    {
        return testDirectory.directory();
    }

}
