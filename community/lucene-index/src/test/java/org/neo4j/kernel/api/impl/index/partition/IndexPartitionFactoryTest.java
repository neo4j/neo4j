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
package org.neo4j.kernel.api.impl.index.partition;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import javax.annotation.Resource;

import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;

@EnableRuleMigrationSupport
@ExtendWith( TestDirectoryExtension.class )
public class IndexPartitionFactoryTest
{
    @Resource
    public TestDirectory testDirectory;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private Directory directory;

    @BeforeEach
    public void setUp() throws IOException
    {
        directory = DirectoryFactory.PERSISTENT.open( testDirectory.directory() );
    }

    @Test
    public void createReadOnlyPartition() throws Exception
    {
        prepareIndex();
        try ( AbstractIndexPartition indexPartition =
                      new ReadOnlyIndexPartitionFactory().createPartition( testDirectory.directory(), directory ) )
        {
            expectedException.expect( UnsupportedOperationException.class );

            indexPartition.getIndexWriter();
        }
    }

    @Test
    public void createWritablePartition() throws Exception
    {
        try ( AbstractIndexPartition indexPartition =
                      new WritableIndexPartitionFactory( IndexWriterConfigs::standard )
                              .createPartition( testDirectory.directory(), directory ) )
        {

            try ( IndexWriter indexWriter = indexPartition.getIndexWriter() )
            {
                indexWriter.addDocument( new Document() );
                indexWriter.commit();
                indexPartition.maybeRefreshBlocking();
                try ( PartitionSearcher searcher = indexPartition.acquireSearcher() )
                {
                    assertEquals( 1, searcher.getIndexSearcher().getIndexReader().numDocs(),
                            "We should be able to see newly added document " );
                }
            }
        }
    }

    private void prepareIndex() throws IOException
    {
        File location = testDirectory.directory();
        try ( AbstractIndexPartition ignored =
                      new WritableIndexPartitionFactory( IndexWriterConfigs::standard )
                              .createPartition( location, DirectoryFactory.PERSISTENT.open( location ) ) )
        {
            // empty
        }
    }
}
