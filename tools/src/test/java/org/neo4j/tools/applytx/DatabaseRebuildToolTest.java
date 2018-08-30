/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.tools.applytx;

import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.tools.console.input.ConsoleUtil.NULL_PRINT_STREAM;

public class DatabaseRebuildToolTest
{
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory();

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
        tool.run( "--from", databaseDirectory( from ).getAbsolutePath(), "--to", to.getAbsolutePath(), "apply last" );

        // THEN
        assertEquals( DbRepresentation.of( databaseDirectory( from ) ), DbRepresentation.of( databaseDirectory( to ) ) );
    }

    @Test
    public void shouldApplySomeTransactions() throws Exception
    {
        // This tests the basic functionality of this tool, there are more things, but it's not as important
        // to test as the functionality of applying transactions.

        // GIVEN
        File from = directory.directory( "from" );
        DatabaseLayout to = directory.databaseLayout( "to" );
        databaseWithSomeTransactions( from );
        DatabaseRebuildTool tool = new DatabaseRebuildTool( input( "apply next", "apply next", "cc", "exit" ),
                NULL_PRINT_STREAM, NULL_PRINT_STREAM );

        // WHEN
        tool.run( "--from", from.getAbsolutePath(), "--to", to.databaseDirectory().getPath(), "-i" );

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
            assertThat( "dump from command '" + command + "'", dump, containsString( string ) );
        }
    }

    private File databaseDirectory( File storeDir )
    {
        return directory.databaseDir( storeDir );
    }

    private static long lastAppliedTx( DatabaseLayout databaseLayout )
    {
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              JobScheduler scheduler = createInitialisedScheduler();
              PageCache pageCache = createPageCache( fileSystem, scheduler ) )
        {
            return MetaDataStore.getRecord( pageCache, databaseLayout.metadataStore(),
                    MetaDataStore.Position.LAST_TRANSACTION_ID );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private static InputStream input( String... strings )
    {
        StringBuilder all = new StringBuilder();
        for ( String string : strings )
        {
            all.append( string ).append( format( "%n" ) );
        }
        return new ByteArrayInputStream( all.toString().getBytes() );
    }

    private static void databaseWithSomeTransactions( File storeDir )
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.record_id_batch_size, "1" )
                .newGraphDatabase();
        Node[] nodes = new Node[10];
        for ( int i = 0; i < nodes.length; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( label( "Label_" + (i % 2) ) );
                setProperties( node, i );
                nodes[i] = node;
                tx.success();
            }
        }
        for ( int i = 0; i < 40; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Relationship relationship = nodes[i % nodes.length]
                        .createRelationshipTo( nodes[(i + 1) % nodes.length], withName( "TYPE_" + (i % 3) ) );
                setProperties( relationship, i );
                tx.success();
            }
        }
        try ( Transaction tx = db.beginTx() )
        {
            Node node = nodes[nodes.length - 1];
            for ( Relationship relationship : node.getRelationships() )
            {
                relationship.delete();
            }
            node.delete();
            tx.success();
        }
        db.shutdown();
    }

    private static void setProperties( PropertyContainer entity, int i )
    {
        entity.setProperty( "key", "name" + i );
    }
}
