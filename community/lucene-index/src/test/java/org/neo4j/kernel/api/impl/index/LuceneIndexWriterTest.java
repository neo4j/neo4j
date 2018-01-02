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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.neo4j.index.impl.lucene.LuceneDataSource.KEYWORD_ANALYZER;

public class LuceneIndexWriterTest
{
    @Test
    public void forceShouldSetOnlineStatus() throws Exception
    {
        // GIVEN
        LuceneIndexWriter writer = newWriter();
        writer.addDocument( newDocument() );
        writer.commitAsOnline();

        // WHEN
        writer.close();

        // THEN
        assertTrue( "Should have had online status set", LuceneIndexWriter.isOnline( directory ) );
    }

    @Test
    public void forceShouldKeepOnlineStatus() throws Exception
    {
        // GIVEN
        LuceneIndexWriter writer = newWriter();
        writer.commitAsOnline();

        // WHEN
        writer.addDocument( newDocument() );
        writer.commitAsOnline();
        writer.close();

        // THEN
        assertTrue( "Should have had online status set", LuceneIndexWriter.isOnline( directory ) );
    }

    @Test
    public void otherWriterSessionShouldKeepOnlineStatusEvenIfNotForcingBeforeClosing() throws Exception
    {
        // GIVEN
        LuceneIndexWriter writer = newWriter();
        writer.commitAsOnline();
        writer.close();

        // WHEN
        writer = newWriter();
        writer.addDocument( newDocument() );
        writer.close();

        // THEN
        assertTrue( "Should have had online status set", LuceneIndexWriter.isOnline( directory ) );
    }

    private Directory directory;
    private DirectoryFactory.InMemoryDirectoryFactory dirFactory;

    @Before
    public void before() throws Exception
    {
        dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        directory = dirFactory.open( new File( "dir" ) );
    }

    @After
    public void after()
    {
        dirFactory.close();
    }

    private LuceneIndexWriter newWriter() throws IOException
    {
        return new LuceneIndexWriter( directory, new IndexWriterConfig( Version.LUCENE_36, KEYWORD_ANALYZER ) );
    }

    private Document newDocument()
    {
        Document doc = new Document();
        doc.add( new Field( "test", "test", Store.NO, Index.NOT_ANALYZED ) );
        return doc;
    }
}
