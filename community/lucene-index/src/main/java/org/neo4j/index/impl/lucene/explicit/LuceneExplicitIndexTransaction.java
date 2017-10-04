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
package org.neo4j.index.impl.lucene.explicit;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.api.ExplicitIndex;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.spi.explicitindex.ExplicitIndexProviderTransaction;
import org.neo4j.kernel.spi.explicitindex.IndexCommandFactory;

public class LuceneExplicitIndexTransaction implements ExplicitIndexProviderTransaction
{
    private final LuceneDataSource dataSource;
    private final Map<String,LuceneExplicitIndex> nodeIndexes = new HashMap<>();
    private final Map<String,LuceneExplicitIndex> relationshipIndexes = new HashMap<>();
    private final LuceneTransactionState luceneTransaction;
    private final IndexCommandFactory commandFactory;

    public LuceneExplicitIndexTransaction( LuceneDataSource dataSource, IndexCommandFactory commandFactory )
    {
        this.dataSource = dataSource;
        this.commandFactory = commandFactory;
        this.luceneTransaction = new LuceneTransactionState();
    }

    @Override
    public ExplicitIndex nodeIndex( String indexName, Map<String, String> configuration )
    {
        LuceneExplicitIndex index = nodeIndexes.get( indexName );
        if ( index == null )
        {
            IndexIdentifier identifier = new IndexIdentifier( IndexEntityType.Node, indexName );
            index = new LuceneExplicitIndex.NodeExplicitIndex( dataSource, identifier, luceneTransaction,
                    IndexType.getIndexType( configuration ), commandFactory );
            nodeIndexes.put( indexName, index );
        }
        return index;
    }

    @Override
    public ExplicitIndex relationshipIndex( String indexName, Map<String, String> configuration )
    {
        LuceneExplicitIndex index = relationshipIndexes.get( indexName );
        if ( index == null )
        {
            IndexIdentifier identifier = new IndexIdentifier( IndexEntityType.Relationship, indexName );
            index = new LuceneExplicitIndex.RelationshipExplicitIndex( dataSource, identifier, luceneTransaction,
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
