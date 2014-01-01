/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

import static org.neo4j.index.impl.lucene.LuceneDataSource.KEYWORD_ANALYZER;

public class WriterLogicTest
{

    @Test
    public void forceShouldSetOnlineStatus() throws Exception
    {
        // GIVEN
        IndexWriter writer = newWriter();
        writer.addDocument( newDocument() );
        logic.commitAsOnline( writer );
        
        // WHEN
        writer.close( true );

        // THEN
        assertTrue( "Should have had online status set", logic.isOnline( directory ) );
    }
    
    @Test
    public void forceShouldKeepOnlineStatus() throws Exception
    {
        // GIVEN
        IndexWriter writer = newWriter();
        logic.commitAsOnline( writer );
        
        // WHEN
        writer.addDocument( newDocument() );
        logic.commitAsOnline( writer );
        writer.close( true );

        // THEN
        assertTrue( "Should have had online status set", logic.isOnline( directory ) );
    }
    
    @Test
    public void otherWriterSessionShouldKeepOnlineStatusEvenIfNotForcingBeforeClosing() throws Exception
    {
        // GIVEN
        IndexWriter writer = newWriter();
        logic.commitAsOnline( writer );
        writer.close( true );
        
        // WHEN
        writer = newWriter();
        writer.addDocument( newDocument() );
        writer.close( true );

        // THEN
        assertTrue( "Should have had online status set", logic.isOnline( directory ) );
    }
    
    private final IndexWriterStatus logic = new IndexWriterStatus();
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
    
    private IndexWriter newWriter() throws IOException
    {
        return new IndexWriter( directory, new IndexWriterConfig( Version.LUCENE_35, KEYWORD_ANALYZER ) );
    }
    
    private Document newDocument()
    {
        Document doc = new Document();
        doc.add( new Field( "test", "test", Store.NO, Index.NOT_ANALYZED ) );
        return doc;
    }
}
