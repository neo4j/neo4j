/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.io.File;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertSame;

public class ReadOnlyIndexReferenceFactoryTest
{
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final ExpectedException expectedException = ExpectedException.none();
    private final CleanupRule cleanupRule = new CleanupRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( cleanupRule ).around( expectedException )
                                          .around( testDirectory ).around( fileSystemRule );

    private static final String INDEX_NAME = "testIndex";
    private LuceneDataSource.LuceneFilesystemFacade filesystemFacade = LuceneDataSource.LuceneFilesystemFacade.FS;
    private IndexIdentifier indexIdentifier = new IndexIdentifier( IndexEntityType.Node, INDEX_NAME );
    private IndexConfigStore indexStore;

    @Before
    public void setUp() throws Exception
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
    public void refreshReadOnlyIndexReference() throws Exception
    {
        ReadOnlyIndexReferenceFactory indexReferenceFactory = getReadOnlyIndexReferenceFactory();
        IndexReference indexReference = indexReferenceFactory.createIndexReference( indexIdentifier );
        cleanupRule.add( indexReference );

        IndexReference refreshedIndex = indexReferenceFactory.refresh( indexReference );
        assertSame("Refreshed instance should be the same.", indexReference, refreshedIndex);
    }

    private void setupIndexInfrastructure() throws Exception
    {
        File storeDir = getStoreDir();
        indexStore = new IndexConfigStore( storeDir, fileSystemRule.get() );
        indexStore.set( Node.class, INDEX_NAME, MapUtil.stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" ) );
        LuceneDataSource luceneDataSource = new LuceneDataSource( storeDir, Config.defaults(),
                indexStore, fileSystemRule.get(), OperationalMode.single );
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
        return new ReadOnlyIndexReferenceFactory( filesystemFacade, new File( getStoreDir(), "index" ),
                new IndexTypeCache( indexStore ) );
    }

    private File getStoreDir()
    {
        return testDirectory.directory();
    }
}
