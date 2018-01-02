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
package org.neo4j.kernel.impl.cache;

import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.store.record.SchemaRule;

public class BridgingCacheAccess implements CacheAccessBackDoor
{
    private final SchemaCache schemaCache;
    private final SchemaState schemaState;
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final RelationshipTypeTokenHolder relationshipTypeTokenHolder;
    private final LabelTokenHolder labelTokenHolder;

    public BridgingCacheAccess( SchemaCache schemaCache, SchemaState schemaState,
            PropertyKeyTokenHolder propertyKeyTokenHolder,
            RelationshipTypeTokenHolder relationshipTypeTokenHolder,
            LabelTokenHolder labelTokenHolder )
    {
        this.schemaCache = schemaCache;
        this.schemaState = schemaState;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.relationshipTypeTokenHolder = relationshipTypeTokenHolder;
        this.labelTokenHolder = labelTokenHolder;
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
    public void addRelationshipTypeToken( RelationshipTypeToken type )
    {
        relationshipTypeTokenHolder.addToken( type );
    }

    @Override
    public void addLabelToken( Token label )
    {
        labelTokenHolder.addToken( label );
    }

    @Override
    public void addPropertyKeyToken( Token propertyKey )
    {
        propertyKeyTokenHolder.addToken( propertyKey );
    }
}
