/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.kernel.api.exceptions.ReadOnlyDatabaseKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.LegacyIndexNotFoundKernelException;
import org.neo4j.kernel.impl.core.ReadOnlyDbException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.LegacyIndexProxy.Lookup;
import org.neo4j.kernel.impl.coreapi.LegacyIndexProxy.Type;

public class IndexManagerImpl implements IndexManager
{
    private NodeAutoIndexerImpl nodeAutoIndexer;
    private RelationshipAutoIndexerImpl relAutoIndexer;
    private final ThreadToStatementContextBridge transactionBridge;
    private final Lookup lookup;

    public IndexManagerImpl( LegacyIndexProxy.Lookup lookup, ThreadToStatementContextBridge transactionBridge )
    {
        this.lookup = lookup;
        this.transactionBridge = transactionBridge;
    }

    @Override
    public boolean existsForNodes( String indexName )
    {
        try ( Statement statement = transactionBridge.instance() )
        {
            statement.readOperations().nodeLegacyIndexGetConfiguration( indexName );
            return true;
        }
        catch ( LegacyIndexNotFoundKernelException e )
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
    public Index<Node> forNodes( String indexName, Map<String, String> customConfiguration )
    {
        Index<Node> toReturn = getOrCreateNodeIndex( indexName, customConfiguration );

        // TODO 2.2-future move this into kernel layer
        if ( NodeAutoIndexerImpl.NODE_AUTO_INDEX.equals( indexName ) )
        {
            toReturn = new AbstractAutoIndexerImpl.ReadOnlyIndexToIndexAdapter<Node>( toReturn );
        }
        return toReturn;
    }

    Index<Node> getOrCreateNodeIndex(
            String indexName, Map<String, String> customConfiguration )
    {
        try ( Statement statement = transactionBridge.instance() )
        {
            // TODO 2.2-future there's a sub-o-meta thing here where we create index config,
            // and the index will itself share the same IndexConfigStore as us and pick up and use
            // that. We should pass along config somehow with calls.
            statement.dataWriteOperations().nodeLegacyIndexCreateLazily( indexName, customConfiguration );
            return new LegacyIndexProxy<>( indexName, Type.NODE, lookup, transactionBridge );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
    }

    RelationshipIndex getOrCreateRelationshipIndex( String indexName, Map<String, String> customConfiguration )
    {
        try ( Statement statement = transactionBridge.instance() )
        {
            // TODO 2.2-future there's a sub-o-meta thing here where we create index config,
            // and the index will itself share the same IndexConfigStore as us and pick up and use
            // that. We should pass along config somehow with calls.
            statement.dataWriteOperations().relationshipLegacyIndexCreateLazily( indexName, customConfiguration );
            return new RelationshipLegacyIndexProxy( indexName, lookup, transactionBridge );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
    }

    @Override
    public String[] nodeIndexNames()
    {
        try ( Statement statement = transactionBridge.instance() )
        {
            return statement.readOperations().nodeLegacyIndexesGetAll();
        }
    }

    @Override
    public boolean existsForRelationships( String indexName )
    {
        try ( Statement statement = transactionBridge.instance() )
        {
            statement.readOperations().relationshipLegacyIndexGetConfiguration( indexName );
            return true;
        }
        catch ( LegacyIndexNotFoundKernelException e )
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
                                               Map<String, String> customConfiguration )
    {
        RelationshipIndex toReturn = getOrCreateRelationshipIndex( indexName,
                customConfiguration );

        // TODO 2.2-future move this into kernel layer
        if ( RelationshipAutoIndexerImpl.RELATIONSHIP_AUTO_INDEX.equals( indexName ) )
        {
            toReturn = new RelationshipAutoIndexerImpl.RelationshipReadOnlyIndexToIndexAdapter(
                    toReturn );
        }
        return toReturn;
    }

    @Override
    public String[] relationshipIndexNames()
    {
        try ( Statement statement = transactionBridge.instance() )
        {
            return statement.readOperations().relationshipLegacyIndexesGetAll();
        }
    }

    @Override
    public Map<String, String> getConfiguration( Index<? extends PropertyContainer> index )
    {
        try ( Statement statement = transactionBridge.instance() )
        {
            if ( index.getEntityType().equals( Node.class ) )
            {
                return statement.readOperations().nodeLegacyIndexGetConfiguration( index.getName() );
            }
            if ( index.getEntityType().equals( Relationship.class ) )
            {
                return statement.readOperations().relationshipLegacyIndexGetConfiguration( index.getName() );
            }
            throw new IllegalArgumentException( "Unknown entity type " + index.getEntityType().getSimpleName() );
        }
        catch ( LegacyIndexNotFoundKernelException e )
        {
            throw new NotFoundException( "No node index '" + index.getName() + "' found" );
        }
    }

    @Override
    public String setConfiguration( Index<? extends PropertyContainer> index, String key, String value )
    {
        // TODO 2.2-future configuration changes should be done transactionally. However this
        // has always been done non-transactionally, so it's not a regression.
        try ( Statement statement = transactionBridge.instance() )
        {
            if ( index.getEntityType().equals( Node.class ) )
            {
                return statement.dataWriteOperations().nodeLegacyIndexSetConfiguration( index.getName(), key, value );
            }
            if ( index.getEntityType().equals( Relationship.class ) )
            {
                return statement.dataWriteOperations().relationshipLegacyIndexSetConfiguration(
                        index.getName(), key, value );
            }
            throw new IllegalArgumentException( "Unknown entity type " + index.getEntityType().getSimpleName() );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
    }

    @Override
    public String removeConfiguration( Index<? extends PropertyContainer> index, String key )
    {
        // TODO 2.2-future configuration changes should be done transactionally. However this
        // has always been done non-transactionally, so it's not a regression.
        try ( Statement statement = transactionBridge.instance() )
        {
            if ( index.getEntityType().equals( Node.class ) )
            {
                return statement.dataWriteOperations().nodeLegacyIndexRemoveConfiguration( index.getName(), key );
            }
            if ( index.getEntityType().equals( Relationship.class ) )
            {
                return statement.dataWriteOperations().relationshipLegacyIndexRemoveConfiguration(
                        index.getName(), key );
            }
            throw new IllegalArgumentException( "Unknown entity type " + index.getEntityType().getSimpleName() );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
    }

    // TODO These setters/getters stick. Why are these indexers exposed!?
    public void setNodeAutoIndexer( NodeAutoIndexerImpl nodeAutoIndexer )
    {
        this.nodeAutoIndexer = nodeAutoIndexer;
    }

    public void setRelAutoIndexer( RelationshipAutoIndexerImpl relAutoIndexer )
    {
        this.relAutoIndexer = relAutoIndexer;
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
