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
package org.neo4j.kernel.impl.core;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.util.ArrayMap;

public class ReadOnlyNodeManager extends NodeManager
{
    public ReadOnlyNodeManager(Config config, GraphDatabaseService graphDb, LockManager lockManager,
                               AbstractTransactionManager transactionManager, PersistenceManager persistenceManager,
                               EntityIdGenerator idGenerator, RelationshipTypeHolder relationshipTypeHolder,
                               CacheProvider cacheType, PropertyIndexManager propertyIndexManager,
                               NodeProxy.NodeLookup nodeLookup, RelationshipProxy.RelationshipLookups relationshipLookups,
                               Cache<NodeImpl> nodeCache, Cache<RelationshipImpl> relCache, XaDataSourceManager xaDsm )
    {
        super(config, graphDb, lockManager, transactionManager, persistenceManager, idGenerator,
                relationshipTypeHolder, cacheType, propertyIndexManager, nodeLookup, relationshipLookups,
                nodeCache, relCache, xaDsm );
    }

    @Override
    public Node createNode()
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public Relationship createRelationship( Node startNodeProxy, NodeImpl startNode, Node endNode,
        RelationshipType type )
    {
        throw new ReadOnlyDbException();
    }

    @Override
    PropertyIndex createPropertyIndex( String key )
    {
        throw new ReadOnlyDbException();
    }

    @Override
    ArrayMap<Integer,PropertyData> deleteNode( NodeImpl node, TransactionState tx )
    {
        throw new ReadOnlyDbException();
    }

    @Override
    PropertyData nodeAddProperty( NodeImpl node, PropertyIndex index, Object value )
    {
        throw new ReadOnlyDbException();
    }

    @Override
    PropertyData nodeChangeProperty( NodeImpl node, PropertyData property,
            Object value, TransactionState tx )
    {
        throw new ReadOnlyDbException();
    }

    @Override
    void nodeRemoveProperty( NodeImpl node, PropertyData property, TransactionState tx )
    {
        throw new ReadOnlyDbException();
    }

    @Override
    ArrayMap<Integer,PropertyData> deleteRelationship( RelationshipImpl rel, TransactionState tx )
    {
        throw new ReadOnlyDbException();
    }

    @Override
    PropertyData relAddProperty( RelationshipImpl rel, PropertyIndex index, Object value )
    {
        throw new ReadOnlyDbException();
    }

    @Override
    PropertyData relChangeProperty( RelationshipImpl rel,
            PropertyData property, Object value, TransactionState tx )
    {
        throw new ReadOnlyDbException();
    }

    @Override
    void relRemoveProperty( RelationshipImpl rel, PropertyData property, TransactionState tx )
    {
        throw new ReadOnlyDbException();
    }
}
