/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.index.lucene.unsafe.batchinsert;

import java.util.Map;

import org.neo4j.graphdb.index.Index;
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProviderNewImpl;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;

/**
 * The {@link org.neo4j.unsafe.batchinsert.BatchInserter} version of the Lucene-based indexes. Indexes
 * created and populated using {@link org.neo4j.unsafe.batchinsert.BatchInserterIndex}s from this provider
 * are compatible with the normal {@link Index}es.
 */
public class LuceneBatchInserterIndexProvider implements BatchInserterIndexProvider
{
    private final BatchInserterIndexProvider provider;

    public LuceneBatchInserterIndexProvider( final BatchInserter inserter )
    {
        provider = new LuceneBatchInserterIndexProviderNewImpl( inserter );
    }

    @Override
    public BatchInserterIndex nodeIndex( String indexName, Map<String, String> config )
    {
        return provider.nodeIndex( indexName, config );
    }

    @Override
    public BatchInserterIndex relationshipIndex( String indexName, Map<String, String> config )
    {
        return provider.relationshipIndex( indexName, config );
    }

    @Override
    public void shutdown()
    {
        provider.shutdown();
    }
}
