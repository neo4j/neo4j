/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.kernel.api;

import org.junit.Test;

import java.util.HashMap;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.IndexHits;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings( "Duplicates" )
public abstract class ExplicitIndexCursorWritesTestBase<G extends KernelAPIWriteTestSupport>
        extends KernelAPIWriteTestBase<G>
{

    private static final String INDEX_NAME = "foo";
    private static final String KEY = "bar";
    private static final String VALUE = "this is it";

    @Test
    public void shouldCreateExplicitNodeIndexEagerly() throws Exception
    {
        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            ExplicitIndexWrite indexWrite = tx.indexWrite();
            HashMap<String,String> config = new HashMap<>();
            config.put( "type", "exact" );
            config.put( "provider", "lucene" );
            indexWrite.nodeExplicitIndexCreate( INDEX_NAME, config );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            assertTrue( graphDb.index().existsForNodes( INDEX_NAME ) );
            ctx.success();
        }
    }

    @Test
    public void shouldCreateExplicitNodeIndexLazily() throws Exception
    {
        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            ExplicitIndexWrite indexWrite = tx.indexWrite();
            HashMap<String,String> config = new HashMap<>();
            config.put( "type", "exact" );
            config.put( "provider", "lucene" );
            indexWrite.nodeExplicitIndexCreateLazily( INDEX_NAME, config );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            assertTrue( graphDb.index().existsForNodes( INDEX_NAME ) );
            ctx.success();
        }
    }

    @Test
    public void shouldAddNodeToExplicitIndex() throws Exception
    {
        long nodeId;
        try ( Transaction tx = session.beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            ExplicitIndexWrite indexWrite = tx.indexWrite();
            indexWrite.nodeAddToExplicitIndex( INDEX_NAME, nodeId, KEY, VALUE );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            IndexHits<Node> hits = graphDb.index().forNodes( INDEX_NAME ).get( KEY, VALUE );
            assertThat( hits.next().getId(), equalTo( nodeId ) );
            hits.close();
            ctx.success();
        }
    }

    @Test
    public void shouldRemoveNodeFromExplicitIndex() throws Exception
    {
        // Given
        long nodeId = addNodeToExplicitIndex();

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            ExplicitIndexWrite indexWrite = tx.indexWrite();
            indexWrite.nodeRemoveFromExplicitIndex( INDEX_NAME, nodeId );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            IndexHits<Node> hits = graphDb.index().forNodes( INDEX_NAME ).get( KEY, VALUE );
            assertFalse( hits.hasNext() );
            hits.close();
            ctx.success();
        }
    }

    @Test
    public void shouldHandleRemoveNodeFromExplicitIndexTwice() throws Exception
    {
        // Given
        long nodeId = addNodeToExplicitIndex();

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            ExplicitIndexWrite indexWrite = tx.indexWrite();
            indexWrite.nodeRemoveFromExplicitIndex( INDEX_NAME, nodeId );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            ExplicitIndexWrite indexWrite = tx.indexWrite();
            indexWrite.nodeRemoveFromExplicitIndex( INDEX_NAME, nodeId );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            IndexHits<Node> hits = graphDb.index().forNodes( INDEX_NAME ).get( KEY, VALUE );
            assertFalse( hits.hasNext() );
            hits.close();
            ctx.success();
        }
    }

    @Test
    public void shouldRemoveNonExistingNodeFromExplicitIndex() throws Exception
    {
        // Given
        long nodeId = addNodeToExplicitIndex();

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            ExplicitIndexWrite indexWrite = tx.indexWrite();
            indexWrite.nodeRemoveFromExplicitIndex( INDEX_NAME, nodeId + 1 );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            IndexHits<Node> hits = graphDb.index().forNodes( INDEX_NAME ).get( KEY, VALUE );
            assertThat( hits.next().getId(), equalTo( nodeId ) );
            assertFalse( hits.hasNext() );
            hits.close();
            ctx.success();
        }
    }

    @Test
    public void shouldCreateExplicitRelationshipIndexEagerly() throws Exception
    {
        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            ExplicitIndexWrite indexWrite = tx.indexWrite();
            HashMap<String,String> config = new HashMap<>();
            config.put( "type", "exact" );
            config.put( "provider", "lucene" );
            indexWrite.relationshipExplicitIndexCreate( INDEX_NAME, config );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            assertTrue( graphDb.index().existsForRelationships( INDEX_NAME ) );
            ctx.success();
        }
    }

    @Test
    public void shouldCreateExplicitRelationshipIndexLazily() throws Exception
    {
        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            ExplicitIndexWrite indexWrite = tx.indexWrite();
            HashMap<String,String> config = new HashMap<>();
            config.put( "type", "exact" );
            config.put( "provider", "lucene" );
            indexWrite.relationshipExplicitIndexCreateLazily( INDEX_NAME, config );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            assertTrue( graphDb.index().existsForRelationships( INDEX_NAME ) );
            ctx.success();
        }
    }

    @Test
    public void shouldCreateExplicitIndexTwice() throws Exception
    {
        // Given
        HashMap<String,String> config = new HashMap<>();
        config.put( "type", "exact" );
        config.put( "provider", "lucene" );

        try ( Transaction tx = session.beginTransaction() )
        {
            ExplicitIndexWrite indexWrite = tx.indexWrite();
            indexWrite.nodeExplicitIndexCreateLazily( INDEX_NAME, config );
            tx.success();
        }

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            ExplicitIndexWrite indexWrite = tx.indexWrite();
            indexWrite.nodeExplicitIndexCreateLazily( INDEX_NAME, config );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            assertTrue( graphDb.index().existsForNodes( INDEX_NAME ) );
            ctx.success();
        }
    }

    @Test
    public void shouldAddRelationshipToExplicitIndex() throws Exception
    {
        long relId;
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            relId = graphDb.createNode().createRelationshipTo( graphDb.createNode(), RelationshipType.withName( "R" ) )
                    .getId();
            ctx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            ExplicitIndexWrite indexWrite = tx.indexWrite();
            indexWrite.relationshipAddToExplicitIndex( INDEX_NAME, relId, KEY, VALUE );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            IndexHits<Relationship> hits = graphDb.index().forRelationships( INDEX_NAME ).get( KEY, VALUE );
            assertThat( hits.next().getId(), equalTo( relId ) );
            hits.close();
            ctx.success();
        }
    }

    @Test
    public void shouldRemoveRelationshipFromExplicitIndex() throws Exception
    {
        // Given
        long relId = addRelationshipToExplicitIndex();

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            ExplicitIndexWrite indexWrite = tx.indexWrite();
            indexWrite.relationshipRemoveFromExplicitIndex( INDEX_NAME, relId );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            IndexHits<Node> hits = graphDb.index().forNodes( INDEX_NAME ).get( KEY, VALUE );
            assertFalse( hits.hasNext() );
            hits.close();
            ctx.success();
        }
    }

    @Test
    public void shouldHandleRemoveRelationshipFromExplicitIndexTwice() throws Exception
    {
        // Given
        long relId = addRelationshipToExplicitIndex();

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            ExplicitIndexWrite indexWrite = tx.indexWrite();
            indexWrite.relationshipRemoveFromExplicitIndex( INDEX_NAME, relId );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            ExplicitIndexWrite indexWrite = tx.indexWrite();
            indexWrite.relationshipRemoveFromExplicitIndex( INDEX_NAME, relId );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            IndexHits<Relationship> hits = graphDb.index().forRelationships( INDEX_NAME ).get( KEY, VALUE );
            assertFalse( hits.hasNext() );
            hits.close();
            ctx.success();
        }
    }

    @Test
    public void shouldRemoveNonExistingRelationshipFromExplicitIndex() throws Exception
    {
        // Given
        long relId = addRelationshipToExplicitIndex();

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            ExplicitIndexWrite indexWrite = tx.indexWrite();
            indexWrite.relationshipRemoveFromExplicitIndex( INDEX_NAME, relId + 1 );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            IndexHits<Relationship> hits = graphDb.index().forRelationships( INDEX_NAME ).get( KEY, VALUE );
            assertThat( hits.next().getId(), equalTo( relId ) );
            assertFalse( hits.hasNext() );
            hits.close();
            ctx.success();
        }
    }

    private long addNodeToExplicitIndex() throws Exception
    {
        long nodeId;
        try ( Transaction tx = session.beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            ExplicitIndexWrite indexWrite = tx.indexWrite();
            HashMap<String,String> config = new HashMap<>();
            config.put( "type", "exact" );
            config.put( "provider", "lucene" );
            indexWrite.nodeExplicitIndexCreateLazily( INDEX_NAME, config );
            indexWrite.nodeAddToExplicitIndex( INDEX_NAME, nodeId, KEY, VALUE );
            tx.success();
        }
        return nodeId;
    }

    private long addRelationshipToExplicitIndex()
    {
        long relId;
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            Relationship rel =
                    graphDb.createNode().createRelationshipTo( graphDb.createNode(), RelationshipType.withName( "R" ) );
            relId = rel
                    .getId();
            graphDb.index().forRelationships( INDEX_NAME ).add( rel, KEY, VALUE );
            ctx.success();
        }
        return relId;
    }
}
