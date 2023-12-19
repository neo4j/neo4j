/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.consistency;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HalfCreatedConstraintIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void uniqueIndexWithoutOwningConstraintIsIgnoredDuringCheck() throws ConsistencyCheckTool.ToolFailureException, IOException
    {
        File storeDir = testDirectory.graphDbDir();
        Label marker = Label.label( "MARKER" );
        String property = "property";

        GraphDatabaseService database = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try
        {
            createNodes( marker, property, database );
            addIndex( database );
            waitForIndexPopulationFailure( database );
        }
        finally
        {
            database.shutdown();
        }

        ConsistencyCheckService.Result checkResult =
                ConsistencyCheckTool.runConsistencyCheckTool( new String[]{storeDir.getAbsolutePath()}, emptyPrintStream(), emptyPrintStream() );
        assertTrue( String.join( System.lineSeparator(), Files.readAllLines( checkResult.reportFile().toPath() ) ), checkResult.isSuccessful() );
    }

    private void waitForIndexPopulationFailure( GraphDatabaseService database )
    {
        try ( Transaction ignored = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 10, TimeUnit.MINUTES );
            fail( "Unique index population should fail." );
        }
        catch ( IllegalStateException e )
        {
            assertEquals( "Index entered a FAILED state. Please see database logs.", e.getMessage() );
        }
    }

    private void addIndex( GraphDatabaseService database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            ThreadToStatementContextBridge statementBridge =
                    ((GraphDatabaseAPI) database).getDependencyResolver().provideDependency( ThreadToStatementContextBridge.class ).get();
            KernelTransaction kernelTransaction = statementBridge.getKernelTransactionBoundToThisThread( true );
            LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( 0, 0 );
            SchemaIndexDescriptor index = SchemaIndexDescriptorFactory.uniqueForSchema( descriptor );
            ((KernelTransactionImplementation) kernelTransaction).txState().indexRuleDoAdd( index, null );
            transaction.success();
        }
    }

    private void createNodes( Label marker, String property, GraphDatabaseService database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                Node node = database.createNode( marker );
                node.setProperty( property, "a" );
            }
            transaction.success();
        }
    }

    private static PrintStream emptyPrintStream()
    {
        return new PrintStream( org.neo4j.io.NullOutputStream.NULL_OUTPUT_STREAM );
    }
}
