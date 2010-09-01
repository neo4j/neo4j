/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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

import java.util.Map;

import org.neo4j.commons.collection.MapUtil;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;

/**
 * The "batch inserter" version of {@link LuceneFulltextIndexServiceWrapper}. It should
 * be used with a BatchInserter and stores the indexes in the same format as
 * {@link LuceneFulltextIndexServiceWrapper}.
 * 
 * It's optimized for large chunks of either reads or writes. So try to avoid
 * mixed reads and writes because there's a slight overhead to go from read mode
 * to write mode (the "mode" is per key and will not affect other keys)
 * 
 * See more information at {link
 * http://wiki.neo4j.org/content/Indexing_with_BatchInserter the Indexing with
 * BatchInserter wiki page}.
 */
public class LuceneFulltextIndexBatchInserter extends
        LuceneIndexBatchInserterImpl
{
    private static final Map<String, String> FULLTEXT_CONFIG =
            MapUtil.stringMap( "type", "fulltext" );
    
    /**
     * @param inserter the {@link BatchInserter} to use.
     */
    public LuceneFulltextIndexBatchInserter( BatchInserter inserter )
    {
        super( inserter );
    }
    
    @Override
    protected BatchInserterIndex getIndex( String indexName )
    {
        return this.provider.nodeIndex( indexName, FULLTEXT_CONFIG );
    }
    
    @Override
    public IndexHits<Long> getNodes( String key, Object value )
    {
        return getIndex( key ).query( LuceneFulltextIndexServiceWrapper.toQuery( key, value ) );
    }
}
