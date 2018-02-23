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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import javax.annotation.Resource;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongSet;
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
import org.neo4j.test.extension.EmbeddedDatabaseExtension;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( EmbeddedDatabaseExtension.class )
public class LuceneFulltextTestSupport
{
    protected static final String ANALYZER = StandardAnalyzer.class.getCanonicalName();
    protected static final Log LOG = NullLog.getInstance();

    @Resource
    public EmbeddedDatabaseRule dbRule;

    protected static final RelationshipType RELTYPE = RelationshipType.withName( "type" );

    protected String analyzer = ANALYZER;
    protected AvailabilityGuard availabilityGuard = new AvailabilityGuard( Clock.systemDefaultZone(), LOG );
    protected GraphDatabaseAPI db;
    protected JobScheduler scheduler;
    protected FileSystemAbstraction fs;
    protected File storeDir;
    private TransactionIdStore transactionIdStore;

    @BeforeEach
    public void setUp() throws Throwable
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
        PrimitiveLongSet set = PrimitiveLongCollections.setOf( ids );
        while ( result.hasNext() )
        {
            long next = result.next().entityId();
            assertTrue( set.remove( next ),
                    format( "Result returned node id %d, expected one of %s", next, Arrays.toString( ids ) ) );
        }
        assertTrue( set.isEmpty(), "Number of results differ from expected" );
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
            assertEquals( ids[num], nextId, format( "Result returned node id %d, expected %d", nextId, ids[num] ) );
            num++;
        }
        assertEquals( ids.length, num, "Number of results differ from expected" );
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
