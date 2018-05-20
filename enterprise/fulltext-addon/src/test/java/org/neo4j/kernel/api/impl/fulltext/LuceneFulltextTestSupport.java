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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.Before;
import org.junit.Rule;

import java.io.File;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class LuceneFulltextTestSupport
{
    protected static final String ANALYZER = StandardAnalyzer.class.getCanonicalName();
    protected static final Log LOG = NullLog.getInstance();

    @Rule
    public DatabaseRule dbRule = new EmbeddedDatabaseRule().startLazily();

    protected static final RelationshipType RELTYPE = RelationshipType.withName( "type" );

    protected String analyzer = ANALYZER;
    protected AvailabilityGuard availabilityGuard = new AvailabilityGuard( Clock.systemDefaultZone(), LOG );
    protected GraphDatabaseAPI db;
    protected JobScheduler scheduler;
    protected FileSystemAbstraction fs;
    protected File storeDir;
    private TransactionIdStore transactionIdStore;

    @Before
    public void setUp()
    {
        db = dbRule.getGraphDatabaseAPI();
        scheduler = dbRule.resolveDependency( JobScheduler.class );
        fs = dbRule.resolveDependency( FileSystemAbstraction.class );
        storeDir = dbRule.getStoreDir();
        transactionIdStore = dbRule.resolveDependency( TransactionIdStore.class );
    }

    protected FulltextProviderImpl createProvider()
    {
        return new FulltextProviderImpl( db, LOG, availabilityGuard, scheduler, transactionIdStore,
                fs, storeDir, analyzer );
    }

    protected long createNodeIndexableByPropertyValue( Object propertyValue )
    {
        return createNodeWithProperty( "prop", propertyValue );
    }

    protected long createNodeWithProperty( String propertyKey, Object propertyValue )
    {
        Node node = db.createNode();
        node.setProperty( propertyKey, propertyValue );
        return node.getId();
    }

    protected long createRelationshipIndexableByPropertyValue( long firstNodeId, long secondNodeId, Object propertyValue )
    {
        return createRelationshipWithProperty( firstNodeId, secondNodeId, "prop", propertyValue );
    }

    protected long createRelationshipWithProperty( long firstNodeId, long secondNodeId, String propertyKey,
                                                 Object propertyValue )
    {
        Node first = db.getNodeById( firstNodeId );
        Node second = db.getNodeById( secondNodeId );
        Relationship relationship = first.createRelationshipTo( second, RELTYPE );
        relationship.setProperty( propertyKey, propertyValue );
        return relationship.getId();
    }

    protected void assertExactQueryFindsNothing( ReadOnlyFulltext reader, String query )
    {
        assertExactQueryFindsIds( reader, query, false );
    }

    protected void assertExactQueryFindsIds( ReadOnlyFulltext reader, Collection<String> query, boolean matchAll, long... ids )
    {
        ScoreEntityIterator result = reader.query( query, matchAll );
        assertQueryResultsMatch( result, ids );
    }

    protected void assertExactQueryFindsIdsInOrder( ReadOnlyFulltext reader, Collection<String> query, boolean matchAll, long... ids )
    {
        ScoreEntityIterator result = reader.query( query, matchAll );
        assertQueryResultsMatchInOrder( result, ids );
    }

    protected void assertExactQueryFindsIds( ReadOnlyFulltext reader, String query, boolean matchAll, long... ids )
    {
        assertExactQueryFindsIds( reader, Arrays.asList( query ), matchAll, ids );
    }

    protected void assertFuzzyQueryFindsIds( ReadOnlyFulltext reader, String query, boolean matchAll, long... ids )
    {
        assertFuzzyQueryFindsIds( reader, Arrays.asList( query ), matchAll, ids );
    }

    protected void assertFuzzyQueryFindsIds( ReadOnlyFulltext reader, Collection<String> query, boolean matchAll, long... ids )
    {
        ScoreEntityIterator result = reader.fuzzyQuery( query, matchAll );
        assertQueryResultsMatch( result, ids );
    }

    protected void assertFuzzyQueryFindsIdsInOrder( ReadOnlyFulltext reader, String query, boolean matchAll, long... ids )
    {
        ScoreEntityIterator result = reader.fuzzyQuery( Arrays.asList( query ), matchAll );
        assertQueryResultsMatchInOrder( result, ids );
    }

    protected void assertQueryResultsMatch( ScoreEntityIterator result, long[] ids )
    {
        final MutableLongSet set = LongHashSet.newSetWith( ids );
        while ( result.hasNext() )
        {
            long next = result.next().entityId();
            assertTrue( String.format( "Result returned node id %d, expected one of %s", next, Arrays.toString( ids ) ), set.remove( next ) );
        }
        assertTrue( "Number of results differ from expected", set.isEmpty() );
    }

    protected void assertQueryResultsMatchInOrder( ScoreEntityIterator result, long[] ids )
    {
        int num = 0;
        float score = Float.MAX_VALUE;
        while ( result.hasNext() )
        {
            ScoreEntityIterator.ScoreEntry scoredResult = result.next();
            long nextId = scoredResult.entityId();
            float nextScore = scoredResult.score();
            assertThat( nextScore, lessThanOrEqualTo( score ) );
            score = nextScore;
            assertEquals( String.format( "Result returned node id %d, expected %d", nextId, ids[num] ), ids[num], nextId );
            num++;
        }
        assertEquals( "Number of results differ from expected", ids.length, num );
    }

    protected void setNodeProp( long nodeId, String value )
    {
        setNodeProp( nodeId, "prop", value );
    }

    protected void setNodeProp( long nodeId, String propertyKey, String value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( nodeId );
            node.setProperty( propertyKey, value );
            tx.success();
        }
    }
}
