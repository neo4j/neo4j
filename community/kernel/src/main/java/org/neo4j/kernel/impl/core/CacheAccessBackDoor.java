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

import org.neo4j.kernel.impl.nioneo.store.SchemaRule;

public interface CacheAccessBackDoor
{
    void removeNodeFromCache( long nodeId );
    
    void removeRelationshipFromCache( long id );

    void removeRelationshipTypeFromCache( int id );

    void removeGraphPropertiesFromCache();
    
    void addSchemaRule( SchemaRule schemaRule );
    
    void removeSchemaRuleFromCache( long id );

    void addRelationshipTypeToken( Token type );

    void addLabelToken( Token labelId );

    void addPropertyKeyToken( Token index );

    /**
     * Patches the relationship chain loading parts of the start and end nodes of deleted relationships. This is
     * a good idea to call when deleting relationships, otherwise the in memory representation of relationship chains
     * may become damaged.
     * This is not expected to remove the deleted relationship from the cache - use
     * {@link #removeRelationshipFromCache(long)} for that purpose before calling this method.
     *
     * @param relId The relId of the relationship deleted
     * @param firstNodeId The relId of the first node
     * @param firstNodeNextRelId The next relationship relId of the first node in its relationship chain
     * @param secondNodeId The relId of the second node
     * @param secondNodeNextRelId The next relationship relId of the second node in its relationship chain
     */
    void patchDeletedRelationshipNodes( long relId, long firstNodeId, long firstNodeNextRelId, long secondNodeId,
                                      long secondNodeNextRelId );
}
