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
package org.neo4j.kernel.api.impl.schema;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.io.IOException;

import org.neo4j.kernel.api.impl.index.partition.ReadOnlyIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.logging.NullLog;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertTrue;

public class ReadOnlyLuceneSchemaIndexTest
{
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final ExpectedException expectedException = ExpectedException.none();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDirectory )
            .around( expectedException ).around( fileSystemRule ).around( pageCacheRule );

    private ReadOnlyDatabaseSchemaIndex luceneSchemaIndex;

    @Before
    public void setUp()
    {
        Config config = Config.defaults();
        DirectoryFactory dirFactory =
                DirectoryFactory.newDirectoryFactory( pageCacheRule.getPageCache( fileSystemRule ), config,
                        NullLog.getInstance() );
        PartitionedIndexStorage indexStorage = new PartitionedIndexStorage(
                dirFactory,
                fileSystemRule.get(), testDirectory.directory(), false );
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig( config );
        luceneSchemaIndex = new ReadOnlyDatabaseSchemaIndex( indexStorage, IndexDescriptorFactory.forLabel( 0, 0 ),
                samplingConfig, new ReadOnlyIndexPartitionFactory() );
    }

    @After
    public void tearDown() throws IOException
    {
        luceneSchemaIndex.close();
    }

    @Test
    public void indexDeletionIndReadOnlyModeIsNotSupported()
    {
        expectedException.expect( UnsupportedOperationException.class );
        luceneSchemaIndex.drop();
    }

    @Test
    public void indexCreationInReadOnlyModeIsNotSupported()
    {
        expectedException.expect( UnsupportedOperationException.class );
        luceneSchemaIndex.create();
    }

    @Test
    public void readOnlyIndexMarkingIsNotSupported()
    {
        expectedException.expect( UnsupportedOperationException.class );
        luceneSchemaIndex.markAsOnline();
    }

    @Test
    public void readOnlyIndexMode()
    {
        assertTrue( luceneSchemaIndex.isReadOnly() );
    }

    @Test
    public void writerIsNotAccessibleInReadOnlyMode()
    {
        expectedException.expect( UnsupportedOperationException.class );
        luceneSchemaIndex.getIndexWriter();
    }
}
