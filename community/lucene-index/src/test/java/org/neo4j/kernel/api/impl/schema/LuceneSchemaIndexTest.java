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
package org.neo4j.kernel.api.impl.schema;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.test.DefaultFileSystemRule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertTrue;

public class LuceneSchemaIndexTest
{
    @Rule
    public final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    private final DirectoryFactory dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();
    private LuceneSchemaIndex index;

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

    private LuceneSchemaIndex createIndex() throws IOException
    {
        LuceneSchemaIndex schemaIndex = newSchemaIndex( false );
        schemaIndex.create();
        schemaIndex.open();
        return schemaIndex;
    }

    private LuceneSchemaIndex openIndex() throws IOException
    {
        LuceneSchemaIndex schemaIndex = newSchemaIndex( false );
        schemaIndex.open();
        return schemaIndex;
    }

    private LuceneSchemaIndex newSchemaIndex( boolean unique )
    {
        LuceneSchemaIndexBuilder builder = LuceneSchemaIndexBuilder.create();
        if ( unique )
        {
            builder = builder.uniqueIndex();
        }
        return builder
                .withIndexRootFolder( testDir.directory( "index" ) )
                .withDirectoryFactory( dirFactory )
                .withFileSystem( fs.get() )
                .withIndexIdentifier( "testIndex" )
                .build();
    }

    private static Document newDocument()
    {
        Document doc = new Document();
        doc.add( new StringField( "test", UUID.randomUUID().toString(), Field.Store.YES ) );
        return doc;
    }
}
