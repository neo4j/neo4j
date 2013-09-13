/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.StringLogger;

public class ReadOnlyNodeManager extends NodeManager
{
    public ReadOnlyNodeManager( StringLogger logger, GraphDatabaseService graphDb,
                                AbstractTransactionManager transactionManager, PersistenceManager persistenceManager,
                                EntityIdGenerator idGenerator, RelationshipTypeTokenHolder relationshipTypeTokenHolder,
                                CacheProvider cacheType, PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokenHolder,
                                NodeProxy.NodeLookup nodeLookup, RelationshipProxy.RelationshipLookups relationshipLookups,
                                Cache<NodeImpl> nodeCache, Cache<RelationshipImpl> relCache, XaDataSourceManager xaDsm,
                                ThreadToStatementContextBridge statementCtxProvider )
    {
        super( logger, graphDb, transactionManager, persistenceManager, idGenerator,
                relationshipTypeTokenHolder, cacheType, propertyKeyTokenHolder, labelTokenHolder, nodeLookup, relationshipLookups,
                nodeCache, relCache, xaDsm, statementCtxProvider );
    }

    @Override
    public Node createNode()
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public Relationship createRelationship( Node startNodeProxy, NodeImpl startNode, Node endNode, long typeId )
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public ArrayMap<Integer, DefinedProperty> deleteNode( NodeImpl node, TransactionState tx )
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public ArrayMap<Integer, DefinedProperty> deleteRelationship( RelationshipImpl rel, TransactionState tx )
    {
        throw new ReadOnlyDbException();
    }
}
