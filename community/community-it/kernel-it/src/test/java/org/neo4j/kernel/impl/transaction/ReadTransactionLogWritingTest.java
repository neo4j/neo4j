/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.LogTestUtils.CountingLogHook;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.LogTestUtils.filterNeostoreLogicalLog;

/**
 * Asserts that pure read operations does not write records to logical or transaction logs.
 */
public class ReadTransactionLogWritingTest
{
    @Rule
    public final DatabaseRule dbr = new ImpermanentDatabaseRule();

    private final Label label = label( "Test" );
    private Node node;
    private Relationship relationship;
    private long logEntriesWrittenBeforeReadOperations;

    @Before
    public void createDataset()
    {
        GraphDatabaseAPI db = dbr.getGraphDatabaseAPI();
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode( label );
            node.setProperty( "short", 123 );
            node.setProperty( "long", longString( 300 ) );
            relationship = node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            relationship.setProperty( "short", 123 );
            relationship.setProperty( "long", longString( 300 ) );
            tx.success();
        }
        logEntriesWrittenBeforeReadOperations = countLogEntries();
    }

    @Test
    public void shouldNotWriteAnyLogCommandInPureReadTransaction()
    {
        // WHEN
        executeTransaction( getRelationships() );
        executeTransaction( getProperties() );
        executeTransaction( getById() );
        executeTransaction( getNodesFromRelationship() );

        // THEN
        long actualCount = countLogEntries();
        assertEquals( "There were " + (actualCount - logEntriesWrittenBeforeReadOperations) +
                        " log entries written during one or more pure read transactions", logEntriesWrittenBeforeReadOperations,
                actualCount );
    }

    private long countLogEntries()
    {
        GraphDatabaseAPI db = dbr.getGraphDatabaseAPI();
        FileSystemAbstraction fs = db.getDependencyResolver().resolveDependency( FileSystemAbstraction.class );
        LogFiles logFiles = db.getDependencyResolver().resolveDependency( LogFiles.class );
        try
        {
            CountingLogHook<LogEntry> logicalLogCounter = new CountingLogHook<>();
            filterNeostoreLogicalLog( logFiles, fs, logicalLogCounter );

            long txLogRecordCount = logFiles.getLogFileInformation().getLastEntryId();

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
            characters[i] = (char) ('a' + i % 10);
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
        try ( Transaction tx = dbr.getGraphDatabaseAPI().beginTx() )
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
        return () -> assertEquals( 1, Iterables.count( node.getRelationships() ) );
    }

    private Runnable getNodesFromRelationship()
    {
        return () ->
        {
            relationship.getEndNode();
            relationship.getStartNode();
            relationship.getNodes();
            relationship.getOtherNode( node );
        };
    }

    private Runnable getById()
    {
        return () ->
        {
            dbr.getGraphDatabaseAPI().getNodeById( node.getId() );
            dbr.getGraphDatabaseAPI().getRelationshipById( relationship.getId() );
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
