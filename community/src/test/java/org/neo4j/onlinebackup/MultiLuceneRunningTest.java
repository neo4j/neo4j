/*
 * Copyright (c) 2009-2010 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.onlinebackup;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneFulltextIndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * Backup Neo4j and a Lucene data source to another running Neo4j+Lucene,
 * including a lucene fulltext data source.
 */
public class MultiLuceneRunningTest
{
    private static enum MyRels implements RelationshipType
    {
        TEST
    }

    private static final String FILE_SEP = System
        .getProperty( "file.separator" );
    private static final String TARGET_DIR = "target";
    private static final String VAR = TARGET_DIR + FILE_SEP + "var";
    private static final String STORE_LOCATION_DIR = VAR + FILE_SEP + "neo-db";
    private static final String BACKUP_LOCATION_DIR = VAR + FILE_SEP
        + "neo-backup";
    private HashMap<String, String> config = null;

    @Before
    public void setup()
    {
        Util.deleteDir( new File( VAR ) );

        System.out.println( "setting up database and backup-copy "
                            + "including Lucene and lucene-fulltext" );

        EmbeddedGraphDatabase graphDb = Util.startGraphDbInstance(
                STORE_LOCATION_DIR, getConfiguration() );
        IndexService index = new LuceneIndexService( graphDb );
        LuceneFulltextIndexService fulltextIndex = new LuceneFulltextIndexService(
                graphDb );

        Transaction tx = graphDb.beginTx();
        try
        {
            Node node = addNode( graphDb );
            index.index( node, "number", 1 );
            fulltextIndex.index( node, "text", "abc def" );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        Util.stopGraphDb( graphDb, index, fulltextIndex );

        Util.copyDir( STORE_LOCATION_DIR, BACKUP_LOCATION_DIR );
    }

    protected Map<String, String> getConfiguration()
    {
        if ( config != null )
        {
            return config;
        }
        config = new HashMap<String, String>();
        config.put( "keep_logical_logs", "nioneodb,lucene,lucene-fulltext" );
        return config;
    }

    @Test
    public void backup() throws IOException
    {
        System.out.println( "starting tests" );
        EmbeddedGraphDatabase graphDb = Util.startGraphDbInstance(
                STORE_LOCATION_DIR, getConfiguration() );
        IndexService indexService = new LuceneIndexService( graphDb );
        LuceneFulltextIndexService fulltextIndex = new LuceneFulltextIndexService(
                graphDb );

        System.out.println( "backing up original db without any changes" );
        tryBackup( graphDb, BACKUP_LOCATION_DIR, 1 );

        Transaction tx = graphDb.beginTx();
        try
        {
            indexService.index( addNode( graphDb ), "number", 2 );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        System.out.println( "one node added" );
        tryBackup( graphDb, BACKUP_LOCATION_DIR, 2 );

        tx = graphDb.beginTx();
        try
        {
            indexService.index( addNode( graphDb ), "number", 3 );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        System.out.println( "one node added" );

        tx = graphDb.beginTx();
        try
        {
            indexService.index( addNode( graphDb ), "number", 4 );
            System.out.println( "one node added, not commited" );
            tryBackup( graphDb, BACKUP_LOCATION_DIR, 3 );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        System.out.println( "previous add commited" );
        tryBackup( graphDb, BACKUP_LOCATION_DIR, 4 );

        Util.stopGraphDb( graphDb, indexService, fulltextIndex );
    }

    protected void tryBackup( EmbeddedGraphDatabase graphDb, String location, int relCount )
        throws IOException
    {
        setupBackup( graphDb, location );

        EmbeddedGraphDatabase bDb = Util.startGraphDbInstance( location );
        IndexService bIndexService = new LuceneIndexService( bDb );
        LuceneFulltextIndexService bFulltextIndex = new LuceneFulltextIndexService(
                bDb );

        Transaction bTx = bDb.beginTx();
        try
        {
            List<Relationship> rels = new ArrayList<Relationship>();
            for ( Relationship rel : bDb.getReferenceNode().getRelationships() )
            {
                rels.add( rel );
            }
            assertEquals( relCount, rels.size() );
            Node node = bIndexService.getSingleNode( "number", relCount );
            assertEquals( true, node != null );
            assertEquals( node.getId(), (long) (Long) node.getProperty(
                "theId", -1L ) );
            node = bFulltextIndex.getSingleNode( "text", "abc" );
            assertEquals( true, node != null );
            bTx.success();
        }
        finally
        {
            bTx.finish();
        }
        Util.stopGraphDb( bDb, bIndexService, bFulltextIndex );
    }

    @SuppressWarnings( "serial" )
    protected void setupBackup( EmbeddedGraphDatabase graphDb, String location )
        throws IOException
    {
        EmbeddedGraphDatabase bDb = Util.startGraphDbInstance( location );
        IndexService bIndexService = new LuceneIndexService( bDb );
        LuceneFulltextIndexService bFulltextIndex = new LuceneFulltextIndexService(
                bDb );

        Backup backupComp = Neo4jBackup.allDataSources( graphDb, bDb );
        backupComp.enableFileLogger();
        backupComp.doBackup();
        Util.stopGraphDb( bDb, bIndexService, bFulltextIndex );
    }

    private Node addNode( EmbeddedGraphDatabase graphDb )
    {
        Node referenceNode = graphDb.getReferenceNode();
        Node node = graphDb.createNode();
        node.setProperty( "theId", node.getId() );
        referenceNode.createRelationshipTo( node, MyRels.TEST );
        return node;
    }
}
