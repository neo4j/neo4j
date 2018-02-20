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
package org.neo4j.kernel.api.impl.fulltext.lucene;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
import org.junit.Rule;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.impl.fulltext.integrations.kernel.FulltextAccessor;
import org.neo4j.kernel.api.impl.fulltext.integrations.kernel.FulltextIndexProviderFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LuceneFulltextTestSupport
{
    protected static final String ANALYZER = StandardAnalyzer.class.getCanonicalName();
    protected static final Log LOG = NullLog.getInstance();
    public static final String PROP = "prop";

    @Rule
    public DatabaseRule db = new EmbeddedDatabaseRule();

    protected static final RelationshipType RELTYPE = RelationshipType.withName( "type" );

    protected String analyzer = ANALYZER;
    protected AvailabilityGuard availabilityGuard = new AvailabilityGuard( Clock.systemDefaultZone(), LOG );
//    protected GraphDatabaseAPI db;
    protected JobScheduler scheduler;
    protected FileSystemAbstraction fs;
    protected File storeDir;
    private TransactionIdStore transactionIdStore;
    protected FulltextAccessor fulltextAccessor;

//    @Before
//    public void setUp() throws Throwable
//    {
//        db = dbRule.getGraphDatabaseAPI();
//        scheduler = dbRule.resolveDependency( JobScheduler.class );
//        fs = dbRule.resolveDependency( FileSystemAbstraction.class );
//        storeDir = dbRule.getStoreDir();
//        transactionIdStore = dbRule.resolveDependency( TransactionIdStore.class );
//    }

    @Before
    public void setUp() throws Throwable
    {
        fulltextAccessor = getAccessor();
    }

    protected FulltextProviderImpl createProvider() throws IOException
    {
        return new FulltextProviderImpl( db, LOG, availabilityGuard, scheduler, transactionIdStore,
                fs, storeDir, analyzer );
    }

    private FulltextAccessor getAccessor() throws IOException
    {
        return (FulltextAccessor) db.resolveDependency( IndexProviderMap.class ).apply( FulltextIndexProviderFactory.DESCRIPTOR );
    }

    protected long createNodeIndexableByPropertyValue( Object propertyValue )
    {
        return createNodeWithProperty( PROP, propertyValue );
    }

    protected long createNodeWithProperty( String propertyKey, Object propertyValue )
    {
        Node node = db.createNode();
        node.setProperty( propertyKey, propertyValue );
        return node.getId();
    }

    protected long createRelationshipIndexableByPropertyValue( long firstNodeId, long secondNodeId, Object propertyValue )
    {
        return createRelationshipWithProperty( firstNodeId, secondNodeId, PROP, propertyValue );
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

    protected void assertExactQueryFindsNothing( String indexName, String query ) throws IOException
    {
        assertExactQueryFindsIds( indexName, query, false );
    }

    protected void assertExactQueryFindsIds( String indexName, Collection<String> query, boolean matchAll, long... ids ) throws IOException
    {
        String queryString = FulltextQueryHelper.createQuery(query, false, matchAll);
        PrimitiveLongIterator result = fulltextAccessor.query( indexName, queryString );
        assertQueryResultsMatch( result, ids );
    }

    protected void assertExactQueryFindsIdsInOrder( String indexName, Collection<String> query, boolean matchAll, long... ids ) throws IOException
    {
        String queryString = FulltextQueryHelper.createQuery(query, false, matchAll);
        PrimitiveLongIterator result = fulltextAccessor.query( indexName, queryString );
        assertQueryResultsMatchInOrder( result, ids );
    }

    protected void assertExactQueryFindsIds( String indexName, String query, boolean matchAll, long... ids ) throws IOException
    {
        assertExactQueryFindsIds( indexName, Arrays.asList( query ), matchAll, ids );
    }

    protected void assertFuzzyQueryFindsIds( String indexName, String query, boolean matchAll, long... ids ) throws IOException
    {
        assertFuzzyQueryFindsIds( indexName, Arrays.asList( query ), matchAll, ids );
    }

    protected void assertFuzzyQueryFindsIds( String indexName, Collection<String> query, boolean matchAll, long... ids ) throws IOException
    {
        String queryString = FulltextQueryHelper.createQuery(query, true, matchAll);
        PrimitiveLongIterator result = fulltextAccessor.query( indexName, queryString );
        assertQueryResultsMatch( result, ids );
    }

    protected void assertFuzzyQueryFindsIdsInOrder( String indexName, String query, boolean matchAll, long... ids ) throws IOException
    {
        String queryString = FulltextQueryHelper.createQuery(Arrays.asList( query ), true, matchAll);
        PrimitiveLongIterator result = fulltextAccessor.query( indexName, queryString );
        assertQueryResultsMatchInOrder( result, ids );
    }

    protected void assertQueryResultsMatch( PrimitiveLongIterator result, long[] ids )
    {
        PrimitiveLongSet set = PrimitiveLongCollections.setOf( ids );
        while ( result.hasNext() )
        {
            long next = result.next();
            assertTrue( String.format( "Result returned node id %d, expected one of %s", next, Arrays.toString( ids ) ), set.remove( next ) );
        }
        assertTrue( "Number of results differ from expected", set.isEmpty() );
    }

    protected void assertQueryResultsMatchInOrder( PrimitiveLongIterator result, long[] ids )
    {
        int num = 0;
        while ( result.hasNext() )
        {
            long next = result.next();
            assertEquals( String.format( "Result returned node id %d, expected %d", next, ids[num] ), ids[num], next );
            num++;
        }
        assertEquals( "Number of results differ from expected", ids.length, num );
    }

    protected void setNodeProp( long nodeId, String value )
    {
        setNodeProp( nodeId, PROP, value );
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
    protected void await( IndexDescriptor fulltextIndexDescriptor ) throws IndexNotFoundKernelException
    {
        //TODO real await
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            //noinspection StatementWithEmptyBody
            while ( stmt.readOperations().indexGetState( fulltextIndexDescriptor ) != InternalIndexState.ONLINE )
            {
                ;
            }
        }
    }
}
