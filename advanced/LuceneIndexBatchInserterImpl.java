/*
sw * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.index.impl.lucene;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.commons.iterator.IterableWrapper;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.IndexService;
import org.neo4j.index.impl.SimpleIndexHits;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;

/**
 * The implementation of {@link LuceneIndexBatchInserter}.
 */
public class LuceneIndexBatchInserterImpl
{
    final BatchInserter inserter;
    final IndexService asIndexService;
    final LuceneBatchInserterIndexProvider provider;
    final Set<String> indexes = new HashSet<String>();
    
    /**
     * @param inserter the {@link BatchInserter} to use.
     */
    public LuceneIndexBatchInserterImpl( BatchInserter inserter )
    {
        this.inserter = inserter;
        this.provider = new LuceneBatchInserterIndexProvider( inserter );
        this.asIndexService = new AsIndexService();
    }
    
    protected BatchInserterIndex getIndex( String indexName )
    {
        return this.provider.nodeIndex( indexName, LuceneIndexProvider.EXACT_CONFIG );
    }
    
    public void index( long node, String key, Object value )
    {
        indexes.add( key );
        getIndex( key ).add( node, Collections.singletonMap( key, value ) );
    }

    public void shutdown()
    {
        this.provider.shutdown();
    }

    public IndexHits<Long> getNodes( String key, Object value )
    {
        return getIndex( key ).get( key, value );
    }
    
    public long getSingleNode( String key, Object value )
    {
        Iterator<Long> nodes = getNodes( key, value ).iterator();
        long node = nodes.hasNext() ? nodes.next() : -1;
        if ( nodes.hasNext() )
        {
            throw new RuntimeException( "More than one node for " +
                key + "=" + value );
        }
        return node;
    }

    public IndexService getIndexService()
    {
        return asIndexService;
    }
    
    public void optimize()
    {
        for ( String index : indexes )
        {
            getIndex( index ).flush();
            System.out.println( "flushed " + index );
        }
    }
    
    private class AsIndexService implements IndexService
    {
        public IndexHits<Node> getNodes( String key, Object value )
        {
            IndexHits<Long> ids = LuceneIndexBatchInserterImpl.this.getNodes(
                key, value );
            Iterable<Node> nodes = new IterableWrapper<Node, Long>( ids )
            {
                @Override
                protected Node underlyingObjectToObject( Long id )
                {
                    return inserter.getGraphDbService().getNodeById( id );
                }
            };
            return new SimpleIndexHits<Node>( nodes, ids.size() );
        }

        public Node getSingleNode( String key, Object value )
        {
            long id =
                LuceneIndexBatchInserterImpl.this.getSingleNode( key, value );
            return id == -1 ? null : inserter.getGraphDbService().getNodeById( id );
        }

        public void index( Node node, String key, Object value )
        {
            LuceneIndexBatchInserterImpl.this.index( node.getId(), key, value );
        }

        public void removeIndex( Node node, String key, Object value )
        {
            throw new UnsupportedOperationException();
        }

        public void removeIndex( Node node, String key )
        {
            throw new UnsupportedOperationException();
        }
        
        public void removeIndex( String key )
        {
            throw new UnsupportedOperationException();
        }
        
        public void shutdown()
        {
            LuceneIndexBatchInserterImpl.this.shutdown();
        }
    }
}
