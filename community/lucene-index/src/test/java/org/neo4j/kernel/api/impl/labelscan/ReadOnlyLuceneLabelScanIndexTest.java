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
package org.neo4j.kernel.api.impl.labelscan;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.io.IOException;

import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.labelscan.storestrategy.BitmapDocumentFormat;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertTrue;

public class ReadOnlyLuceneLabelScanIndexTest
{
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final ExpectedException expectedException = ExpectedException.none();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( testDirectory ).around( expectedException )
            .around( fileSystemRule );

    private ReadOnlyDatabaseLabelScanIndex luceneLabelScanIndex;

    @Before
    public void setUp()
    {
        PartitionedIndexStorage indexStorage = new PartitionedIndexStorage( DirectoryFactory.PERSISTENT,
                fileSystemRule.get(), testDirectory.directory(), "1", false );
        luceneLabelScanIndex = new ReadOnlyDatabaseLabelScanIndex( BitmapDocumentFormat._32, indexStorage );
    }

    @After
    public void tearDown() throws IOException
    {
        luceneLabelScanIndex.close();
    }

    @Test
    public void readOnlyIndexMode() throws Exception
    {
        assertTrue( luceneLabelScanIndex.isReadOnly() );
    }

    @Test
    public void writerIsNotAccessibleInReadOnlyMode() throws Exception
    {
        expectedException.expect( UnsupportedOperationException.class );
        luceneLabelScanIndex.getLabelScanWriter();
    }

    @Test
    public void indexCreationInReadOnlyModeIsNotSupported() throws Exception
    {
        expectedException.expect( UnsupportedOperationException.class );
        luceneLabelScanIndex.create();
    }

    @Test
    public void indexDeletionInReadOnlyModeIsNotSupported() throws Exception
    {
        expectedException.expect( UnsupportedOperationException.class );
        luceneLabelScanIndex.drop();
    }
}
