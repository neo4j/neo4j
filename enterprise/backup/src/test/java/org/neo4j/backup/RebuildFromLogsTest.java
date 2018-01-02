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
package org.neo4j.backup;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.test.TargetDirectory.testDirForTest;

@RunWith(Parameterized.class)
public class RebuildFromLogsTest
{
    @Rule
    public final TargetDirectory.TestDirectory dir = testDirForTest( RebuildFromLogsTest.class );
    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Test
    public void shouldRebuildFromLog() throws Exception
    {
        // given
        File prototypePath = new File( dir.graphDbDir(), "prototype" );
        GraphDatabaseService prototype = db( prototypePath );
        try
        {
            for ( Transaction transaction : work )
            {
                transaction.applyTo( prototype );
            }
        }
        finally
        {
            prototype.shutdown();
        }

        // when
        File rebuildPath = new File( dir.graphDbDir(), "rebuild" );
        new RebuildFromLogs( fs ).rebuild( prototypePath, rebuildPath, BASE_TX_ID );

        // then
        assertEquals( DbRepresentation.of( prototypePath ), DbRepresentation.of( rebuildPath ) );
    }

    @Test
    public void shouldRebuildFromLogUpToATx() throws Exception
    {
        // given
        File prototypePath = new File( dir.graphDbDir(), "prototype" );
        GraphDatabaseAPI prototype = db( prototypePath );
        long txId;
        try
        {
            for ( Transaction transaction : work )
            {
                transaction.applyTo( prototype );
            }
        }
        finally
        {
            txId = prototype.getDependencyResolver()
                    .resolveDependency( MetaDataStore.class ).getLastCommittedTransactionId();
            prototype.shutdown();
        }

        File copy = new File( dir.graphDbDir(), "copy" );
        FileUtils.copyRecursively( prototypePath, copy );
        GraphDatabaseAPI db = db( copy );
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
        finally
        {
            db.shutdown();
        }

        // when
        File rebuildPath = new File( dir.graphDbDir(), "rebuild" );
        new RebuildFromLogs( fs ).rebuild( copy, rebuildPath, txId );

        // then
        assertEquals( DbRepresentation.of( prototypePath ), DbRepresentation.of( rebuildPath ) );
    }

    private GraphDatabaseAPI db( File rebuiltPath )
    {
        return (GraphDatabaseAPI) new TestGraphDatabaseFactory().newEmbeddedDatabase( rebuiltPath.getAbsolutePath() );
    }

    enum Transaction
    {
        CREATE_NODE
                {
                    @Override
                    void applyTx( GraphDatabaseService graphDb )
                    {
                        graphDb.createNode();
                    }
                },
        CREATE_NODE_WITH_PROPERTY
                {
                    @Override
                    void applyTx( GraphDatabaseService graphDb )
                    {
                        graphDb.createNode().setProperty( name(), "value" );
                    }
                },
        SET_PROPERTY( CREATE_NODE )
                {
                    @Override
                    void applyTx( GraphDatabaseService graphDb )
                    {
                        firstNode( graphDb ).setProperty( name(), "value" );
                    }
                },
        CHANGE_PROPERTY( CREATE_NODE_WITH_PROPERTY )
                {
                    @Override
                    void applyTx( GraphDatabaseService graphDb )
                    {
                        for ( Node node : GlobalGraphOperations.at( graphDb ).getAllNodes() )
                        {
                            if ( "value".equals( node.getProperty( CREATE_NODE_WITH_PROPERTY.name(), null ) ) )
                            {
                                node.setProperty( CREATE_NODE_WITH_PROPERTY.name(), "other" );
                                break;
                            }
                        }
                    }
                },
        LEGACY_INDEX_NODE( CREATE_NODE )
                {
                    @Override
                    void applyTx( GraphDatabaseService graphDb )
                    {
                        Node node = firstNode( graphDb );
                        graphDb.index().forNodes( name() ).add( node, "foo", "bar" );
                    }
                };

        private static Node firstNode( GraphDatabaseService graphDb )
        {
            return Iterables.first( GlobalGraphOperations.at( graphDb ).getAllNodes() );
        }

        private final Transaction[] dependencies;

        private Transaction( Transaction... dependencies )
        {
            this.dependencies = dependencies;
        }

        void applyTo( GraphDatabaseService graphDb )
        {
            try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
            {
                applyTx( graphDb );

                tx.success();
            }
        }

        void applyTx( GraphDatabaseService graphDb )
        {
        }
    }

    static class WorkLog
    {
        static final WorkLog BASE = new WorkLog( EnumSet.noneOf( Transaction.class ) );
        final EnumSet<Transaction> transactions;

        WorkLog( EnumSet<Transaction> transactions )
        {
            this.transactions = transactions;
        }

        @Override
        public boolean equals( Object that )
        {
            return this == that ||
                   that instanceof WorkLog &&
                   transactions.equals( ((WorkLog) that).transactions );
        }

        @Override
        public int hashCode()
        {
            return transactions.hashCode();
        }

        @Override
        public String toString()
        {
            return transactions.toString();
        }

        Transaction[] transactions()
        {
            return transactions.toArray( new Transaction[transactions.size()] );
        }

        static Set<WorkLog> combinations()
        {
            Set<WorkLog> combinations = Collections.newSetFromMap( new LinkedHashMap<WorkLog, Boolean>() );
            for ( Transaction transaction : Transaction.values() )
            {
                combinations.add( BASE.extend( transaction ) );
            }
            for ( Transaction transaction : Transaction.values() )
            {
                for ( WorkLog combination : new ArrayList<>( combinations ) )
                {
                    combinations.add( combination.extend( transaction ) );
                }
            }
            return combinations;
        }

        private WorkLog extend( Transaction transaction )
        {
            EnumSet<Transaction> base = EnumSet.copyOf( transactions );
            Collections.addAll( base, transaction.dependencies );
            base.add( transaction );
            return new WorkLog( base );
        }
    }

    private final Transaction[] work;

    public RebuildFromLogsTest( WorkLog work )
    {
        this.work = work.transactions();
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> commands()
    {
        List<Object[]> commands = new ArrayList<>();
        for ( WorkLog combination : WorkLog.combinations() )
        {
            commands.add( new Object[]{combination} );
        }
        return commands;
    }
}
