/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.consistency;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0_7;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.test.EmbeddedDatabaseRule;
import org.neo4j.test.SuppressOutput;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertTrue;
import static org.neo4j.test.TargetDirectory.testDirForTest;

@RunWith( Parameterized.class )
public class ConsistencyCheckServiceRecordFormatIT
{
    @ClassRule
    public static final TargetDirectory.TestDirectory dir =
            testDirForTest( ConsistencyCheckServiceRecordFormatIT.class );

    private final File storeDir = dir.directory( "db" );
    private final EmbeddedDatabaseRule db = new EmbeddedDatabaseRule( storeDir ).startLazily();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( SuppressOutput.suppressAll() ).around( db );

    @Parameter
    public String recordFormat;

    @Parameters( name = "{0}" )
    public static List<String> recordFormats()
    {
        return Arrays.asList( StandardV3_0_7.NAME, HighLimit.NAME );
    }

    @Before
    public void configureRecordFormat() throws Exception
    {
        db.setConfig( GraphDatabaseSettings.record_format, recordFormat );
    }

    @Test
    public void checkTinyConsistentStore() throws Exception
    {
        db.ensureStarted();
        createLinkedList( db, 1_000 );
        db.shutdownAndKeepStore();

        assertConsistentStore( db );
    }

    private static void createLinkedList( GraphDatabaseService db, int size )
    {
        Node previous = null;
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < size; i++ )
            {
                Label label = (i % 2 == 0) ? TestLabel.FOO : TestLabel.BAR;
                Node current = db.createNode( label );
                current.setProperty( "value", ThreadLocalRandom.current().nextLong() );

                if ( previous != null )
                {
                    previous.createRelationshipTo( current, TestRelType.FORWARD );
                    current.createRelationshipTo( previous, TestRelType.BACKWARD );
                }
                previous = current;
            }
            tx.success();
        }
    }

    private static void assertConsistentStore( GraphDatabaseAPI db ) throws Exception
    {
        ConsistencyCheckService service = new ConsistencyCheckService();

        File storeDir = new File( db.getStoreDir() );
        ConsistencyCheckService.Result result = service.runFullConsistencyCheck( storeDir, Config.empty(),
                ProgressMonitorFactory.textual( System.out ), FormattedLogProvider.toOutputStream( System.out ), true );

        assertTrue( "Store is inconsistent", result.isSuccessful() );
    }

    private enum TestLabel implements Label
    {
        FOO,
        BAR
    }

    private enum TestRelType implements RelationshipType
    {
        FORWARD,
        BACKWARD
    }
}
