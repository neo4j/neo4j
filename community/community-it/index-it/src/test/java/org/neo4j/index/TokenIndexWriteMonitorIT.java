/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.index.schema.TokenScanWriteMonitor;
import org.neo4j.kernel.internal.event.InternalTransactionEventListener;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@DbmsExtension( configurationCallback = "configure" )
class TokenIndexWriteMonitorIT
{
    @Inject
    private GraphDatabaseService db;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private DatabaseManagementService databaseManagementService;

    private final List<Long> txIds = new ArrayList<>();

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseInternalSettings.token_scan_write_log_enabled, true );
    }

    @BeforeEach
    void beforeEach()
    {
        // Let's create all tokens up front ...
        try ( var tx = db.beginTx() )
        {
            var node = tx.createNode( Label.label( "l1" ), Label.label( "l2" ), Label.label( "l3" ) );
            node.createRelationshipTo( node, RelationshipType.withName( "t1" ) );
            tx.commit();
        }

        try ( var tx = db.beginTx() )
        {
            tx.getRelationshipById( 0 ).delete();
            tx.getNodeById( 0 ).delete();
            tx.commit();
        }

        databaseManagementService.registerTransactionEventListener( db.databaseName(), new InternalTransactionEventListener.Adapter<>()
        {

            public void afterCommit( TransactionData data, Object state, GraphDatabaseService databaseService )
            {
                txIds.add( data.getTransactionId() );
            }
        } );
    }

    @Test
    void testLabelChangeLogging() throws IOException
    {
        long nodeId;
        try ( var tx = db.beginTx() )
        {
            nodeId = tx.createNode( Label.label( "l1" ), Label.label( "l2" ) ).getId();
            tx.commit();
        }

        try ( var tx = db.beginTx() )
        {
            var node = tx.getNodeById( nodeId );
            node.removeLabel( Label.label( "l2" ) );
            node.addLabel( Label.label( "l3" ) );
            tx.commit();
        }

        var labelIndex = databaseLayout.labelScanStore();
        var logContent = getLogContent( labelIndex );
        assertThat( logContent )
                .contains( "+tx:" + txIds.get( 0 ) + ",entity:" + nodeId + ",token:0" )
                .contains( "+tx:" + txIds.get( 0 ) + ",entity:" + nodeId + ",token:1" )
                .contains( "-tx:" + txIds.get( 1 ) + ",entity:" + nodeId + ",token:1" )
                .contains( "+tx:" + txIds.get( 1 ) + ",entity:" + nodeId + ",token:2" );
    }

    @Test
    void testRelationshipTypeChangeLogging() throws IOException
    {
        long relId;
        try ( var tx = db.beginTx() )
        {
            var node = tx.createNode();
            relId = node.createRelationshipTo( node, RelationshipType.withName( "t1" ) ).getId();
            tx.commit();
        }

        try ( var tx = db.beginTx() )
        {
            tx.getRelationshipById( relId ).delete();
            tx.commit();
        }

        var relIndex = databaseLayout.relationshipTypeScanStore();
        var logContent = getLogContent( relIndex );
        assertThat( logContent )
                .contains( "+tx:" + txIds.get( 0 ) + ",entity:" + relId + ",token:0" )
                .contains( "-tx:" + txIds.get( 1 ) + ",entity:" + relId + ",token:0" );
    }

    private String getLogContent( Path index ) throws IOException
    {
        // shutdown is the easiest way how to flush the log
        databaseManagementService.shutdown();

        TokenScanWriteMonitor.main( new String[]{"--tofile", databaseLayout.databaseDirectory().toString()} );
        var logFile = index.resolveSibling( index.getFileName() + ".writelog.txt" );
        return Files.readString( logFile );
    }
}
