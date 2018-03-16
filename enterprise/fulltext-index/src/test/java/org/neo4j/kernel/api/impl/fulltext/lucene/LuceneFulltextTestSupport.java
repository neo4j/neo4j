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

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.impl.fulltext.FulltextAdapter;
import org.neo4j.kernel.api.impl.fulltext.FulltextIndexProviderFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.RepeatRule;

import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LuceneFulltextTestSupport
{

    protected static final RelationshipType RELTYPE = RelationshipType.withName( "type" );
    public static final String PROP = "prop";

    public DatabaseRule db = new EmbeddedDatabaseRule();
    public RepeatRule repeatRule = createRepeatRule();
    @Rule
    public RuleChain rules = RuleChain.outerRule( repeatRule ).around( db );

    protected FulltextAdapter fulltextAdapter;

    protected RepeatRule createRepeatRule()
    {
        return new RepeatRule( false, 1 );
    }

    @Before
    public void setUp()
    {
        fulltextAdapter = getAccessor();
    }

    public void applySetting( Setting<String> setting, String value ) throws IOException
    {
        db.restartDatabase( setting.name(), value );
        db.ensureStarted();
        fulltextAdapter = getAccessor();
    }

    private FulltextAdapter getAccessor()
    {
        return (FulltextAdapter) db.resolveDependency( IndexProviderMap.class ).get( FulltextIndexProviderFactory.DESCRIPTOR );
    }

    long createNodeIndexableByPropertyValue( Object propertyValue )
    {
        return createNodeWithProperty( PROP, propertyValue );
    }

    long createNodeWithProperty( String propertyKey, Object propertyValue )
    {
        Node node = db.createNode();
        node.setProperty( propertyKey, propertyValue );
        return node.getId();
    }

    long createRelationshipIndexableByPropertyValue( long firstNodeId, long secondNodeId, Object propertyValue )
    {
        return createRelationshipWithProperty( firstNodeId, secondNodeId, PROP, propertyValue );
    }

    long createRelationshipWithProperty( long firstNodeId, long secondNodeId, String propertyKey, Object propertyValue )
    {
        Node first = db.getNodeById( firstNodeId );
        Node second = db.getNodeById( secondNodeId );
        Relationship relationship = first.createRelationshipTo( second, RELTYPE );
        relationship.setProperty( propertyKey, propertyValue );
        return relationship.getId();
    }

    void assertQueryFindsNothing( String indexName, String query ) throws IOException, IndexNotFoundKernelException
    {
        assertQueryFindsIds( indexName, query );
    }

    void assertQueryFindsIds( String indexName, String query, long... ids )
            throws IOException, IndexNotFoundKernelException
    {
        ScoreEntityIterator result = fulltextAdapter.query( indexName, query );
        assertQueryResultsMatch( result, ids );
    }

    void assertQueryFindsIdsInOrder( String indexName, String query, long... ids )
            throws IOException, IndexNotFoundKernelException
    {
        ScoreEntityIterator result = fulltextAdapter.query( indexName, query );
        assertQueryResultsMatchInOrder( result, ids );
    }

    private void assertQueryResultsMatch( ScoreEntityIterator result, long[] ids )
    {
        PrimitiveLongSet set = PrimitiveLongCollections.setOf( ids );
        while ( result.hasNext() )
        {
            long next = result.next().entityId();
            assertTrue( String.format( "Result returned node id %d, expected one of %s", next, Arrays.toString( ids ) ), set.remove( next ) );
        }
        if ( !set.isEmpty() )
        {
            List<Long> list = new ArrayList<>();
            set.visitKeys( k -> !list.add( k ) );
            fail( "Number of results differ from expected. " + set.size() +
                  " IDs were not found in the result: " + list );
        }
    }

    private void assertQueryResultsMatchInOrder( ScoreEntityIterator result, long[] ids )
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

    void setNodeProp( long nodeId, String value )
    {
        setNodeProp( nodeId, PROP, value );
    }

    void setNodeProp( long nodeId, String propertyKey, String value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( nodeId );
            node.setProperty( propertyKey, value );
            tx.success();
        }
    }

    void await( IndexDescriptor fulltextIndexDescriptor ) throws IndexNotFoundKernelException
    {
        //TODO change this once you can use normal index await with multi-token indexes
        try ( Transaction ignore = db.beginTx(); Statement stmt = db.statement() )
        {
            //noinspection StatementWithEmptyBody
            while ( stmt.readOperations().indexGetState( fulltextIndexDescriptor ) != InternalIndexState.ONLINE )
            {
                continue;
            }
        }
    }
}
