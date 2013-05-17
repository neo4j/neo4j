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
package org.neo4j.kernel;

import org.neo4j.kernel.impl.api.PersistenceCache;
import org.neo4j.kernel.impl.api.SchemaCache;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;

public class BridgingCacheAccess implements CacheAccessBackDoor
{
    private final NodeManager nodeManager;
    private final SchemaCache schemaCache;
    private final SchemaState schemaState;
    private final PersistenceCache persistenceCache;

    public BridgingCacheAccess( NodeManager nodeManager, SchemaCache schemaCache, SchemaState schemaState,
                                PersistenceCache persistenceCache )
    {
        this.nodeManager = nodeManager;
        this.schemaCache = schemaCache;
        this.schemaState = schemaState;
        this.persistenceCache = persistenceCache;
    }
    
    @Override
    public void removeNodeFromCache( long nodeId )
    {
        nodeManager.removeNodeFromCache( nodeId );
        persistenceCache.evictNode( nodeId );
    }

    @Override
    public void removeRelationshipFromCache( long id )
    {
        nodeManager.removeRelationshipFromCache( id );
    }

    @Override
    public void removeRelationshipTypeFromCache( int id )
    {
        nodeManager.removeRelationshipTypeFromCache( id );
    }

    @Override
    public void removeGraphPropertiesFromCache()
    {
        nodeManager.removeGraphPropertiesFromCache();
    }

    @Override
    public void addSchemaRule( SchemaRule rule )
    {
        schemaCache.addSchemaRule( rule );
    }

    @Override
    public void removeSchemaRuleFromCache( long id )
    {
        schemaCache.removeSchemaRule( id );
        schemaState.clear();
    }

    @Override
    public void addRelationshipTypeToken( Token type )
    {
        nodeManager.addRelationshipTypeToken( type );
    }

    @Override
    public void addLabelToken( Token label )
    {
        nodeManager.addLabelToken( label );
    }

    @Override
    public void addPropertyKeyToken( Token propertyKey )
    {
        nodeManager.addPropertyKeyToken( propertyKey );
    }

    @Override
    public void patchDeletedRelationshipNodes( long relId, long firstNodeId, long firstNodeNextRelId,
            long secondNodeId, long secondNodeNextRelId )
    {
        nodeManager.patchDeletedRelationshipNodes( relId, firstNodeId, firstNodeNextRelId, secondNodeId,
                secondNodeNextRelId );
    }
}
