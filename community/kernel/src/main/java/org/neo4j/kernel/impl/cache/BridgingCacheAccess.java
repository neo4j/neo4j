/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.cache;

import java.util.Collection;

import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.store.PersistenceCache;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.transaction.command.RelationshipHoles;

public class BridgingCacheAccess implements CacheAccessBackDoor
{
    private final SchemaCache schemaCache;
    private final SchemaState schemaState;
    private final PersistenceCache persistenceCache;

    public BridgingCacheAccess( SchemaCache schemaCache, SchemaState schemaState,
                                PersistenceCache persistenceCache )
    {
        this.schemaCache = schemaCache;
        this.schemaState = schemaState;
        this.persistenceCache = persistenceCache;
    }

    @Override
    public void removeNodeFromCache( long nodeId )
    {
        if ( nodeId != -1 )
        {
            persistenceCache.evictNode( nodeId );
        }
    }

    @Override
    public void removeRelationshipFromCache( long id )
    {
        persistenceCache.evictRelationship( id );
    }

    @Override
    public void removeRelationshipTypeFromCache( int id )
    {
        persistenceCache.evictRelationshipType( id );
    }

    @Override
    public void removePropertyKeyFromCache( int id )
    {
        persistenceCache.evictPropertyKey( id );
    }

    @Override
    public void removeLabelFromCache( int id )
    {
        persistenceCache.evictLabel( id );
    }

    @Override
    public void removeGraphPropertiesFromCache()
    {
        persistenceCache.evictGraphProperties();
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
        persistenceCache.cacheRelationshipType( type );
    }

    @Override
    public void addLabelToken( Token label )
    {
        persistenceCache.cacheLabel( label );
    }

    @Override
    public void addPropertyKeyToken( Token propertyKey )
    {
        persistenceCache.cachePropertyKey( propertyKey );
    }

    @Override
    public void patchDeletedRelationshipNodes( long nodeId, RelationshipHoles holes )
    {
        persistenceCache.patchDeletedRelationshipNodes( nodeId, holes );
    }

    @Override
    public void applyLabelUpdates( Collection<NodeLabelUpdate> labelUpdates )
    {
        persistenceCache.apply( labelUpdates );
    }
}
