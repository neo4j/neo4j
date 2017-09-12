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
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertTrue;

public class ReadOnlyLuceneSchemaIndexTest
{
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final ExpectedException expectedException = ExpectedException.none();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDirectory )
            .around( expectedException ).around( fileSystemRule );

    private ReadOnlyDatabaseSchemaIndex luceneSchemaIndex;

    @Before
    public void setUp()
    {
        PartitionedIndexStorage indexStorage = new PartitionedIndexStorage( DirectoryFactory.PERSISTENT,
                fileSystemRule.get(), testDirectory.directory(), false );
        Config config = Config.defaults();
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
    public void indexDeletionIndReadOnlyModeIsNotSupported() throws Exception
    {
        expectedException.expect( UnsupportedOperationException.class );
        luceneSchemaIndex.drop();
    }

    @Test
    public void indexCreationInReadOnlyModeIsNotSupported() throws Exception
    {
        expectedException.expect( UnsupportedOperationException.class );
        luceneSchemaIndex.create();
    }

    @Test
    public void readOnlyIndexMarkingIsNotSupported() throws Exception
    {
        expectedException.expect( UnsupportedOperationException.class );
        luceneSchemaIndex.markAsOnline();
    }

    @Test
    public void readOnlyIndexMode() throws Exception
    {
        assertTrue( luceneSchemaIndex.isReadOnly() );
    }

    @Test
    public void writerIsNotAccessibleInReadOnlyMode() throws Exception
    {
        expectedException.expect( UnsupportedOperationException.class );
        luceneSchemaIndex.getIndexWriter();
    }
}
