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
package org.neo4j.kernel.impl.coreapi;

import java.util.Map;
import java.util.function.Supplier;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipAutoIndexer;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.kernel.impl.api.explicitindex.InternalAutoIndexing;

public class IndexManagerImpl implements IndexManager
{
    private final Supplier<Statement> transactionBridge;
    private final IndexProvider provider;
    private final AutoIndexer<Node> nodeAutoIndexer;
    private final RelationshipAutoIndexer relAutoIndexer;

    public IndexManagerImpl( Supplier<Statement> bridge,
                             IndexProvider provider,
                             AutoIndexer<Node> nodeAutoIndexer,
                             RelationshipAutoIndexer relAutoIndexer )
    {
        this.transactionBridge = bridge;
        this.provider = provider;
        this.nodeAutoIndexer = nodeAutoIndexer;
        this.relAutoIndexer = relAutoIndexer;
    }

    @Override
    public boolean existsForNodes( String indexName )
    {
        try ( Statement statement = transactionBridge.get() )
        {
            statement.readOperations().nodeExplicitIndexGetConfiguration( indexName );
            return true;
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            return false;
        }
    }

    @Override
    public Index<Node> forNodes( String indexName )
    {
        return forNodes( indexName, null );
    }

    @Override
    public Index<Node> forNodes( String indexName, Map<String,String> customConfiguration )
    {
        Index<Node> toReturn = provider.getOrCreateNodeIndex( indexName, customConfiguration );

        // TODO move this into kernel layer
        if ( InternalAutoIndexing.NODE_AUTO_INDEX.equals( indexName ) )
        {
            return new ReadOnlyIndexFacade<>( toReturn );
        }
        return toReturn;
    }

    @Override
    public String[] nodeIndexNames()
    {
        try ( Statement statement = transactionBridge.get() )
        {
            return statement.readOperations().nodeExplicitIndexesGetAll();
        }
    }

    @Override
    public boolean existsForRelationships( String indexName )
    {
        try ( Statement statement = transactionBridge.get() )
        {
            statement.readOperations().relationshipExplicitIndexGetConfiguration( indexName );
            return true;
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            return false;
        }
    }

    @Override
    public RelationshipIndex forRelationships( String indexName )
    {
        return forRelationships( indexName, null );
    }

    @Override
    public RelationshipIndex forRelationships( String indexName,
                                               Map<String,String> customConfiguration )
    {
        RelationshipIndex toReturn = provider.getOrCreateRelationshipIndex( indexName, customConfiguration );

        // TODO move this into kernel layer
        if ( InternalAutoIndexing.RELATIONSHIP_AUTO_INDEX.equals( indexName ) )
        {
            return new RelationshipReadOnlyIndexFacade( toReturn );
        }
        return toReturn;
    }

    @Override
    public String[] relationshipIndexNames()
    {
        try ( Statement statement = transactionBridge.get() )
        {
            return statement.readOperations().relationshipExplicitIndexesGetAll();
        }
    }

    @Override
    public Map<String,String> getConfiguration( Index<? extends PropertyContainer> index )
    {
        try ( Statement statement = transactionBridge.get() )
        {
            if ( index.getEntityType().equals( Node.class ) )
            {
                return statement.readOperations().nodeExplicitIndexGetConfiguration( index.getName() );
            }
            if ( index.getEntityType().equals( Relationship.class ) )
            {
                return statement.readOperations().relationshipExplicitIndexGetConfiguration( index.getName() );
            }
            throw new IllegalArgumentException( "Unknown entity type " + index.getEntityType().getSimpleName() );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new NotFoundException( "No node index '" + index.getName() + "' found" );
        }
    }

    @Override
    public String setConfiguration( Index<? extends PropertyContainer> index, String key, String value )
    {
        // Configuration changes should be done transactionally. However this
        // has always been done non-transactionally, so it's not a regression.
        try ( Statement statement = transactionBridge.get() )
        {
            if ( index.getEntityType().equals( Node.class ) )
            {
                return statement.dataWriteOperations().nodeExplicitIndexSetConfiguration( index.getName(), key, value );
            }
            if ( index.getEntityType().equals( Relationship.class ) )
            {
                return statement.dataWriteOperations().relationshipExplicitIndexSetConfiguration(
                        index.getName(), key, value );
            }
            throw new IllegalArgumentException( "Unknown entity type " + index.getEntityType().getSimpleName() );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new NotFoundException( e );
        }
    }

    @Override
    public String removeConfiguration( Index<? extends PropertyContainer> index, String key )
    {
        // Configuration changes should be done transactionally. However this
        // has always been done non-transactionally, so it's not a regression.
        try ( Statement statement = transactionBridge.get() )
        {
            if ( index.getEntityType().equals( Node.class ) )
            {
                return statement.dataWriteOperations().nodeExplicitIndexRemoveConfiguration( index.getName(), key );
            }
            if ( index.getEntityType().equals( Relationship.class ) )
            {
                return statement.dataWriteOperations().relationshipExplicitIndexRemoveConfiguration(
                        index.getName(), key );
            }
            throw new IllegalArgumentException( "Unknown entity type " + index.getEntityType().getSimpleName() );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new NotFoundException( e );
        }
    }

    @Override
    public AutoIndexer<Node> getNodeAutoIndexer()
    {
        return nodeAutoIndexer;
    }

    @Override
    public RelationshipAutoIndexer getRelationshipAutoIndexer()
    {
        return relAutoIndexer;
    }
}
