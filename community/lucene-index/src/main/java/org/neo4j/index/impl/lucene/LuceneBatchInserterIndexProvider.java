/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import java.util.Map;

import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;

/**
 * The {@link BatchInserter} version of {@link LuceneIndexImplementation}.
 * Indexes created and populated using {@link BatchInserterIndex}s from this
 * provider are compatible with {@link Index}s from
 * {@link LuceneIndexImplementation}.
 *
 * @deprecated This class has been replaced by
 *             {@link org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider}
 *             as of Neo4j 1.7.
 */
public class LuceneBatchInserterIndexProvider implements
        BatchInserterIndexProvider
{
    private final LuceneBatchInserterIndexProviderImpl provider;

    /**
     * @deprecated This class has been replaced by
     *             {@link org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider}
     *             as of Neo4j 1.7.
     */
    public LuceneBatchInserterIndexProvider( final BatchInserter inserter )
    {
        provider = new LuceneBatchInserterIndexProviderImpl( inserter );
    }

    public BatchInserterIndex nodeIndex( String indexName,
                                         Map<String, String> config )
    {
        return provider.nodeIndex( indexName, config );
    }

    public BatchInserterIndex relationshipIndex( String indexName,
                                                 Map<String, String> config )
    {
        return provider.relationshipIndex( indexName, config );
    }

    public void shutdown()
    {
        provider.shutdown();
    }
}
