/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.util.Collections;
import java.util.HashMap;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.IndexHits;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class ExplicitIndexCursorWritesTestBase<G extends KernelAPIWriteTestSupport>
        extends KernelAPIWriteTestBase<G>
{

    private static final String INDEX_NAME = "foo";
    private static final String KEY = "bar";
    private static final String VALUE = "this is it";

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
            IndexHits<Node> hits = graphDb.index().forNodes( INDEX_NAME ).get( KEY, "this is it" );
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
            IndexHits<Node> hits = graphDb.index().forNodes( INDEX_NAME ).get( KEY, "this is it" );
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
    public void shouldCreateExplicitIndex() throws Exception
    {
        // Given

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

    private long addNodeToExplicitIndex()
    {
        long nodeId;
        try ( org.neo4j.graphdb.Transaction ctx = graphDb.beginTx() )
        {
            Node node = graphDb.createNode();
            nodeId = node.getId();
            graphDb.index().forNodes( INDEX_NAME ).add( node, KEY, VALUE );
            ctx.success();
        }
        return nodeId;
    }
}
