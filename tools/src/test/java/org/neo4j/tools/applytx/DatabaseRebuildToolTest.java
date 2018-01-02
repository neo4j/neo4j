/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.tools.applytx;

import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static java.lang.String.format;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.tools.console.input.ConsoleUtil.NULL_PRINT_STREAM;

public class DatabaseRebuildToolTest
{
    @Rule
    public final TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldRebuildDbFromTransactions() throws Exception
    {
        // This tests the basic functionality of this tool, there are more things, but it's not as important
        // to test as the functionality of applying transactions.

        // GIVEN
        File from = directory.directory( "from" );
        File to = directory.directory( "to" );
        databaseWithSomeTransactions( from );
        DatabaseRebuildTool tool = new DatabaseRebuildTool( System.in, NULL_PRINT_STREAM, NULL_PRINT_STREAM );

        // WHEN
        tool.run( "--from", from.getAbsolutePath(), "--to", to.getAbsolutePath(), "apply last" );

        // THEN
        assertEquals( DbRepresentation.of( from ), DbRepresentation.of( to ) );
    }

    @Test
    public void shouldApplySomeTransactions() throws Exception
    {
        // This tests the basic functionality of this tool, there are more things, but it's not as important
        // to test as the functionality of applying transactions.

        // GIVEN
        File from = directory.directory( "from" );
        File to = directory.directory( "to" );
        databaseWithSomeTransactions( from );
        DatabaseRebuildTool tool = new DatabaseRebuildTool( input( "apply next", "apply next", "cc", "exit" ),
                NULL_PRINT_STREAM, NULL_PRINT_STREAM );

        // WHEN
        tool.run( "--from", from.getAbsolutePath(), "--to", to.getAbsolutePath(), "-i" );

        // THEN
        assertEquals( TransactionIdStore.BASE_TX_ID + 2, lastAppliedTx( to ) );
    }

    @Test
    public void shouldDumpNodePropertyChain() throws Exception
    {
        shouldPrint( "dump node properties 0",
                "Property",
                "name0" );
    }

    @Test
    public void shouldDumpRelationshipPropertyChain() throws Exception
    {
        shouldPrint( "dump relationship properties 0",
                "Property",
                "name0" );
    }

    @Test
    public void shouldDumpRelationships() throws Exception
    {
        shouldPrint( "dump node relationships 0",
                "Relationship[0,",
                "Relationship[10," );
    }

    @Test
    public void shouldDumpRelationshipTypeTokens() throws Exception
    {
        shouldPrint( "dump tokens relationship-type",
                "TYPE_0",
                "TYPE_1" );
    }

    @Test
    public void shouldDumpLabelTokens() throws Exception
    {
        shouldPrint( "dump tokens label",
                "Label_0",
                "Label_1" );
    }

    @Test
    public void shouldDumpPropertyKeyTokens() throws Exception
    {
        shouldPrint( "dump tokens property-key",
                "name" );
    }

    private void shouldPrint( String command, String... expectedResultContaining ) throws Exception
    {
        // GIVEN
        File from = directory.directory( "from" );
        File to = directory.directory( "to" );
        databaseWithSomeTransactions( to );
        ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( byteArrayOut );
        DatabaseRebuildTool tool = new DatabaseRebuildTool( input( command, "exit" ),
                out, NULL_PRINT_STREAM );

        // WHEN
        tool.run( "--from", from.getAbsolutePath(), "--to", to.getAbsolutePath(), "-i" );

        // THEN
        out.flush();
        String dump = new String( byteArrayOut.toByteArray() );
        for ( String string : expectedResultContaining )
        {
            assertThat( dump, containsString( string ) );
        }
    }

    private long lastAppliedTx( File storeDir )
    {
        try
        {
            return MetaDataStore.getRecord( createPageCache( new DefaultFileSystemAbstraction() ),
                    new File( storeDir, MetaDataStore.DEFAULT_NAME ),
                    MetaDataStore.Position.LAST_TRANSACTION_ID );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private InputStream input( String... strings )
    {
        StringBuilder all = new StringBuilder();
        for ( String string : strings )
        {
            all.append( string ).append( format( "%n" ) );
        }
        return new ByteArrayInputStream( all.toString().getBytes() );
    }

    private void databaseWithSomeTransactions( File dir )
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( dir.getAbsolutePath() );
        Node[] nodes = new Node[10];
        for ( int i = 0; i < nodes.length; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( label( "Label_" + (i%2) ) );
                setProperties( node, i );
                nodes[i] = node;
                tx.success();
            }
        }
        for ( int i = 0; i < 40; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Relationship relationship =
                        nodes[i%nodes.length].createRelationshipTo( nodes[(i+1)%nodes.length], withName( "TYPE_" + (i%3) ) );
                setProperties( relationship, i );
                tx.success();
            }
        }
        try ( Transaction tx = db.beginTx() )
        {
            Node node = nodes[nodes.length-1];
            for ( Relationship relationship : node.getRelationships() )
            {
                relationship.delete();
            }
            node.delete();
            tx.success();
        }
        db.shutdown();
    }

    private void setProperties( PropertyContainer entity, int i )
    {
        entity.setProperty( "key", "name" + i );
    }
}
