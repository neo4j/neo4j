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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertTrue;

public class LuceneSchemaIndexTest
{
    @Rule
    public final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();

    private final DirectoryFactory dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();
    private SchemaIndex index;
    private final IndexDescriptor descriptor = IndexDescriptorFactory.forLabel( 3, 5 );

    @After
    public void closeIndex() throws Exception
    {
        IOUtils.closeAll( index, dirFactory );
    }

    @Test
    public void markAsOnline() throws IOException
    {
        index = createIndex();
        index.getIndexWriter().addDocument( newDocument() );
        index.markAsOnline();

        assertTrue( "Should have had online status set", index.isOnline() );
    }

    @Test
    public void markAsOnlineAndClose() throws IOException
    {
        index = createIndex();
        index.getIndexWriter().addDocument( newDocument() );
        index.markAsOnline();

        index.close();

        index = openIndex();
        assertTrue( "Should have had online status set", index.isOnline() );
    }

    @Test
    public void markAsOnlineTwice() throws IOException
    {
        index = createIndex();
        index.markAsOnline();

        index.getIndexWriter().addDocument( newDocument() );
        index.markAsOnline();

        assertTrue( "Should have had online status set", index.isOnline() );
    }

    @Test
    public void markAsOnlineTwiceAndClose() throws IOException
    {
        index = createIndex();
        index.markAsOnline();

        index.getIndexWriter().addDocument( newDocument() );
        index.markAsOnline();
        index.close();

        index = openIndex();
        assertTrue( "Should have had online status set", index.isOnline() );
    }

    @Test
    public void markAsOnlineIsRespectedByOtherWriter() throws IOException
    {
        index = createIndex();
        index.markAsOnline();
        index.close();

        index = openIndex();
        index.getIndexWriter().addDocument( newDocument() );
        index.close();

        index = openIndex();
        assertTrue( "Should have had online status set", index.isOnline() );
    }

    private SchemaIndex createIndex() throws IOException
    {
        SchemaIndex schemaIndex = newSchemaIndex();
        schemaIndex.create();
        schemaIndex.open();
        return schemaIndex;
    }

    private SchemaIndex openIndex() throws IOException
    {
        SchemaIndex schemaIndex = newSchemaIndex();
        schemaIndex.open();
        return schemaIndex;
    }

    private SchemaIndex newSchemaIndex()
    {
        LuceneSchemaIndexBuilder builder = LuceneSchemaIndexBuilder.create( descriptor );
        return builder
                .withIndexRootFolder( new File( testDir.directory( "index" ), "testIndex" ) )
                .withDirectoryFactory( dirFactory )
                .withFileSystem( fs.get() )
                .build();
    }

    private static Document newDocument()
    {
        Document doc = new Document();
        doc.add( new StringField( "test", UUID.randomUUID().toString(), Field.Store.YES ) );
        return doc;
    }
}
