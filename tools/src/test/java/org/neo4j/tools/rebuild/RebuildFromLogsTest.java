/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.tools.rebuild;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Set;

import org.neo4j.consistency.checking.InconsistentStoreException;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

@RunWith( Parameterized.class )
public class RebuildFromLogsTest
{
    private final TestDirectory dir = TestDirectory.testDirectory();
    private final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final ExpectedException expectedException = ExpectedException.none();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( dir )
            .around( suppressOutput ).around( fileSystemRule ).around( expectedException );

    private final Transaction[] work;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<WorkLog> commands()
    {
        return WorkLog.combinations();
    }

    @Test
    public void shouldRebuildFromLog() throws Exception, InconsistentStoreException
    {
        // given
        File prototypePath = new File( dir.graphDbDir(), "prototype" );
        populatePrototype( prototypePath );

        // when
        File rebuildPath = new File( dir.graphDbDir(), "rebuild" );
        new RebuildFromLogs( fileSystemRule.get() ).rebuild( prototypePath, rebuildPath, BASE_TX_ID );

        // then
        assertEquals( getDbRepresentation( prototypePath ), getDbRepresentation( rebuildPath ) );
    }

    @Test
    public void failRebuildFromLogIfStoreIsInconsistentAfterRebuild() throws InconsistentStoreException, Exception
    {
        File prototypePath = new File( dir.graphDbDir(), "prototype" );
        populatePrototype( prototypePath );

        // when
        File rebuildPath = new File( dir.graphDbDir(), "rebuild" );
        expectedException.expect( InconsistentStoreException.class );
        RebuildFromLogs rebuildFromLogs = new TestRebuildFromLogs( fileSystemRule.get() );
        rebuildFromLogs.rebuild( prototypePath, rebuildPath, BASE_TX_ID );
    }

    @Test
    public void shouldRebuildFromLogUpToATx() throws Exception, InconsistentStoreException
    {
        // given
        File prototypePath = new File( dir.graphDbDir(), "prototype" );
        long txId = populatePrototype( prototypePath );

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
        new RebuildFromLogs( fileSystemRule.get() ).rebuild( copy, rebuildPath, txId );

        // then
        assertEquals( getDbRepresentation( prototypePath ), getDbRepresentation( rebuildPath ) );
    }

    private long populatePrototype( File prototypePath )
    {
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
            txId = prototype.getDependencyResolver().resolveDependency( MetaDataStore.class ).getLastCommittedTransactionId();
            prototype.shutdown();
        }
        return txId;
    }

    private DbRepresentation getDbRepresentation( File path )
    {
        return DbRepresentation.of( path );
    }

    private GraphDatabaseAPI db( File rebuiltPath )
    {
        return (GraphDatabaseAPI) new TestGraphDatabaseFactory().newEmbeddedDatabase( rebuiltPath );
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
                        ResourceIterable<Node> nodes = graphDb.getAllNodes();
                        try ( ResourceIterator<Node> iterator = nodes.iterator() )
                        {
                            while ( iterator.hasNext() )
                            {
                                Node node = iterator.next();
                                if ( "value".equals( node.getProperty( CREATE_NODE_WITH_PROPERTY.name(), null ) ) )
                                {
                                    node.setProperty( CREATE_NODE_WITH_PROPERTY.name(), "other" );
                                    break;
                                }
                            }
                        }
                    }
                },
        EXPLICIT_INDEX_NODE( CREATE_NODE )
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
            return Iterables.firstOrNull( graphDb.getAllNodes() );
        }

        private final Transaction[] dependencies;

        Transaction( Transaction... dependencies )
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

    public RebuildFromLogsTest( WorkLog work )
    {
        this.work = work.transactions();
    }

    private class TestRebuildFromLogs extends RebuildFromLogs
    {
        TestRebuildFromLogs( FileSystemAbstraction fs )
        {
            super( fs );
        }

        @Override
        void checkConsistency( File target, PageCache pageCache ) throws InconsistentStoreException
        {
            throw new InconsistentStoreException( new ConsistencySummaryStatistics() );
        }
    }
}
