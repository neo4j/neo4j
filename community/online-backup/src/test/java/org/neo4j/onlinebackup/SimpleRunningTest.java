/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.persistence.PersistenceSource;

/**
 * Try to backup Neo4j to another running Neo4j instance.
 */
public class SimpleRunningTest
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

    @Before
    public void clean()
    {
        Util.deleteDir( new File( VAR ) );

        System.out.println( "setting up simple database and backup-copy" );

        EmbeddedGraphDatabase graphDb = Util.startGraphDbInstance( STORE_LOCATION_DIR );
        configureSourceDb( graphDb );

        Transaction tx = graphDb.beginTx();
        try
        {
            addNode( graphDb );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        Util.stopGraphDb( graphDb );

        Util.copyDir( STORE_LOCATION_DIR, BACKUP_LOCATION_DIR );
    }

    protected void configureSourceDb( final EmbeddedGraphDatabase graphDb )
    {
        PersistenceSource persistenceSource = graphDb.getConfig().getPersistenceModule().getPersistenceManager().getPersistenceSource();
        ( (NeoStoreXaDataSource) persistenceSource.getXaDataSource() ).keepLogicalLogs( true );
    }

    @Test
    public void backup() throws IOException
    {
        EmbeddedGraphDatabase graphDb = Util.startGraphDbInstance( STORE_LOCATION_DIR );
        configureSourceDb( graphDb );

        System.out.println( "backing up original db without any changes" );
        tryBackup( graphDb, BACKUP_LOCATION_DIR, 1 );

        Transaction tx = graphDb.beginTx();
        try
        {
            addNode( graphDb );
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
            addNode( graphDb );
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
            addNode( graphDb );
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

        Util.stopGraphDb( graphDb );
    }

    protected void tryBackup( EmbeddedGraphDatabase graphDb, String location,
            int relCount )
        throws IOException
    {
        System.out.println( "backing up to running EmbeddedGraphDatabase instance" );
        EmbeddedGraphDatabase bDb = Util.startGraphDbInstance( location );
        Backup backupComp = Neo4jBackup.neo4jDataSource( graphDb, bDb );
        configureBackup( backupComp );
        backupComp.doBackup();
        Util.stopGraphDb( bDb );
        bDb = Util.startGraphDbInstance( location );
        Transaction bTx = bDb.beginTx();
        try
        {
            List<Relationship> rels = new ArrayList<Relationship>();
            for ( Relationship rel : bDb.getReferenceNode().getRelationships() )
            {
                rels.add( rel );
            }
            assertEquals( relCount, rels.size() );
            bTx.success();
        }
        finally
        {
            bTx.finish();
        }
        Util.stopGraphDb( bDb );
    }

    protected void configureBackup( Backup backupComp ) throws IOException
    {
        backupComp.enableFileLogger();
    }

    private void addNode( EmbeddedGraphDatabase graphDb )
    {
        Node referenceNode = graphDb.getReferenceNode();
        Node node = graphDb.createNode();
        referenceNode.createRelationshipTo( node, MyRels.TEST );
    }
}
