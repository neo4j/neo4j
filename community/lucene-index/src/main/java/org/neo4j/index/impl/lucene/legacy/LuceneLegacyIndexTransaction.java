/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.impl.lucene.legacy;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.api.LegacyIndex;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.spi.legacyindex.IndexCommandFactory;
import org.neo4j.kernel.spi.legacyindex.LegacyIndexProviderTransaction;

public class LuceneLegacyIndexTransaction implements LegacyIndexProviderTransaction
{
    private final LuceneDataSource dataSource;
    private final Map<String,LuceneLegacyIndex> nodeIndexes = new HashMap<>();
    private final Map<String,LuceneLegacyIndex> relationshipIndexes = new HashMap<>();
    private final LuceneTransactionState luceneTransaction;
    private final IndexCommandFactory commandFactory;

    public LuceneLegacyIndexTransaction( LuceneDataSource dataSource, IndexCommandFactory commandFactory )
    {
        this.dataSource = dataSource;
        this.commandFactory = commandFactory;
        this.luceneTransaction = new LuceneTransactionState();
    }

    @Override
    public LegacyIndex nodeIndex( String indexName, Map<String, String> configuration )
    {
        LuceneLegacyIndex index = nodeIndexes.get( indexName );
        if ( index == null )
        {
            IndexIdentifier identifier = new IndexIdentifier( IndexEntityType.Node, indexName );
            index = new LuceneLegacyIndex.NodeLegacyIndex( dataSource, identifier, luceneTransaction,
                    IndexType.getIndexType( configuration ), commandFactory );
            nodeIndexes.put( indexName, index );
        }
        return index;
    }

    @Override
    public LegacyIndex relationshipIndex( String indexName, Map<String, String> configuration )
    {
        LuceneLegacyIndex index = relationshipIndexes.get( indexName );
        if ( index == null )
        {
            IndexIdentifier identifier = new IndexIdentifier( IndexEntityType.Relationship, indexName );
            index = new LuceneLegacyIndex.RelationshipLegacyIndex( dataSource, identifier, luceneTransaction,
                    IndexType.getIndexType( configuration ), commandFactory );
            relationshipIndexes.put( indexName, index );
        }
        return index;
    }

    @Override
    public void close()
    {
        luceneTransaction.close();
    }
}
