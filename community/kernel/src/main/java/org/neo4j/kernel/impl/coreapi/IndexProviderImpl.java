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
package org.neo4j.kernel.impl.coreapi;

import java.util.Map;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

public class IndexProviderImpl implements IndexProvider
{
    private final LegacyIndexProxy.Lookup lookup;
    private final ThreadToStatementContextBridge transactionBridge;

    public IndexProviderImpl( LegacyIndexProxy.Lookup lookup, ThreadToStatementContextBridge transactionBridge )
    {
        this.lookup = lookup;
        this.transactionBridge = transactionBridge;
    }

    @Override
    public Index<Node> getOrCreateNodeIndex( String indexName, Map<String,String> customConfiguration )
    {
        try ( Statement statement = transactionBridge.get() )
        {
            // There's a sub-o-meta thing here where we create index config,
            // and the index will itself share the same IndexConfigStore as us and pick up and use
            // that. We should pass along config somehow with calls.
            statement.dataWriteOperations().nodeLegacyIndexCreateLazily( indexName, customConfiguration );
            return new LegacyIndexProxy<>( indexName, LegacyIndexProxy.Type.NODE, lookup, transactionBridge );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public RelationshipIndex getOrCreateRelationshipIndex( String indexName, Map<String,
            String> customConfiguration )
    {
        try ( Statement statement = transactionBridge.get() )
        {
            // There's a sub-o-meta thing here where we create index config,
            // and the index will itself share the same IndexConfigStore as us and pick up and use
            // that. We should pass along config somehow with calls.
            statement.dataWriteOperations().relationshipLegacyIndexCreateLazily( indexName, customConfiguration );
            return new RelationshipLegacyIndexProxy( indexName, lookup, transactionBridge );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }
}
