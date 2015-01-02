/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.LogTestUtils.CountingLogHook;

import static org.junit.Assert.assertEquals;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.test.LogTestUtils.filterNeostoreLogicalLog;

/**
 * Asserts that pure read operations does not write records to logical or transaction logs.
 */
public class ReadTransactionLogWritingTest
{
    @Test
    public void shouldNotWriteAnyLogCommandInPureReadTransaction() throws Exception
    {
        // WHEN
        executeTransaction( getRelationships() );
        executeTransaction( getProperties() );
        executeTransaction( getById() );
        executeTransaction( getNodesFromRelationship() );
        
        // THEN
        int actualCount = countLogEntries();
        assertEquals( "There were " + (actualCount-logEntriesWrittenBeforeReadOperations) +
                " log entries written during one or more pure read transactions",
                logEntriesWrittenBeforeReadOperations, actualCount );
    }
    
    public final @Rule DatabaseRule dbr = new ImpermanentDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( GraphDatabaseSettings.cache_type, "none" );
        };
    };
    
    private final Label label = label( "Test" );
    private Node node;
    private Relationship relationship;
    private int logEntriesWrittenBeforeReadOperations;
    
    @Before
    public void createDataset()
    {
        GraphDatabaseAPI db = dbr.getGraphDatabaseAPI();
        long nodeId, relationshipId;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode( label );
            node.setProperty( "short", 123 );
            node.setProperty( "long", longString( 300 ) );
            nodeId = node.getId();
            relationship = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            relationship.setProperty( "short", 123 );
            relationship.setProperty( "long", longString( 300 ) );
            relationshipId = relationship.getId();
            tx.success();
        }
        db.getDependencyResolver().resolveDependency( XaDataSourceManager.class ).rotateLogicalLogs();
        logEntriesWrittenBeforeReadOperations = countLogEntries();
    }
    
    private int countLogEntries()
    {
        GraphDatabaseAPI db = dbr.getGraphDatabaseAPI();
        FileSystemAbstraction fs = db.getDependencyResolver().resolveDependency( FileSystemAbstraction.class );
        File storeDir = new File( db.getStoreDir() );
        try
        {
            CountingLogHook<LogEntry> logicalLogCounter = new CountingLogHook<>();
            filterNeostoreLogicalLog( fs, storeDir.getPath(), logicalLogCounter );
            
            // Not so nice, but there's no other way. We cannot look at the file since log records in the txlog
            // are buffered and they get flushed for 2PC (at least up until the commit record).
            // If we're going for a restart instead then we can count them but they will however disappear
            // for the next session.
            int txLogRecordCount = db.getDependencyResolver()
                    .resolveDependency( TxManager.class ).getTxLog().getRecordCount();
            
            return logicalLogCounter.getCount() + txLogRecordCount;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private String longString( int length )
    {
        char[] characters = new char[length];
        for ( int i = 0; i < length; i++ )
        {
            characters[i] = (char) ('a' + i%10);
        }
        return new String( characters );
    }
    
    private void executeTransaction( Runnable runnable )
    {
        executeTransaction( runnable, true );
        executeTransaction( runnable, false );
    }

    private void executeTransaction( Runnable runnable, boolean success )
    {
        try ( Transaction tx = dbr.getGraphDatabaseService().beginTx() )
        {
            runnable.run();
            if ( success )
            {
                tx.success();
            }
        }
    }

    private Runnable getRelationships()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                assertEquals( 1, count( node.getRelationships() ) );
            }
        };
    }
    
    private Runnable getNodesFromRelationship()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                relationship.getEndNode();
                relationship.getStartNode();
                relationship.getNodes();
                relationship.getOtherNode( node );
            }
        };
    }

    private Runnable getById()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                dbr.getGraphDatabaseService().getNodeById( node.getId() );
                dbr.getGraphDatabaseService().getRelationshipById( relationship.getId() );
            }
        };
    }

    private Runnable getProperties()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                getAllProperties( node );
                getAllProperties( relationship );
            }

            private void getAllProperties( PropertyContainer entity )
            {
                for ( String key : entity.getPropertyKeys() )
                {
                    entity.getProperty( key );
                }
            }
        };
    }
}
