/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.api.impl.schema;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class LuceneSchemaIndexTest
{
    @Inject
    private DefaultFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDir;

    private final DirectoryFactory dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();
    private SchemaIndex index;
    private final IndexDescriptor descriptor = TestIndexDescriptorFactory.forLabel( 3, 5 );

    @AfterEach
    void closeIndex() throws Exception
    {
        IOUtils.closeAll( index, dirFactory );
    }

    @Test
    void markAsOnline() throws IOException
    {
        index = createIndex();
        index.getIndexWriter().addDocument( newDocument() );
        index.markAsOnline();

        assertTrue( index.isOnline(), "Should have had online status set" );
    }

    @Test
    void markAsOnlineAndClose() throws IOException
    {
        index = createIndex();
        index.getIndexWriter().addDocument( newDocument() );
        index.markAsOnline();

        index.close();

        index = openIndex();
        assertTrue( index.isOnline(), "Should have had online status set" );
    }

    @Test
    void markAsOnlineTwice() throws IOException
    {
        index = createIndex();
        index.markAsOnline();

        index.getIndexWriter().addDocument( newDocument() );
        index.markAsOnline();

        assertTrue( index.isOnline(), "Should have had online status set" );
    }

    @Test
    void markAsOnlineTwiceAndClose() throws IOException
    {
        index = createIndex();
        index.markAsOnline();

        index.getIndexWriter().addDocument( newDocument() );
        index.markAsOnline();
        index.close();

        index = openIndex();
        assertTrue( index.isOnline(), "Should have had online status set" );
    }

    @Test
    void markAsOnlineIsRespectedByOtherWriter() throws IOException
    {
        index = createIndex();
        index.markAsOnline();
        index.close();

        index = openIndex();
        index.getIndexWriter().addDocument( newDocument() );
        index.close();

        index = openIndex();
        assertTrue( index.isOnline(), "Should have had online status set" );
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
        LuceneSchemaIndexBuilder builder = LuceneSchemaIndexBuilder.create( descriptor, Config.defaults() );
        return builder
                .withIndexRootFolder( new File( testDir.directory( "index" ), "testIndex" ) )
                .withDirectoryFactory( dirFactory )
                .withFileSystem( fs )
                .build();
    }

    private static Document newDocument()
    {
        Document doc = new Document();
        doc.add( new StringField( "test", UUID.randomUUID().toString(), Field.Store.YES ) );
        return doc;
    }
}
