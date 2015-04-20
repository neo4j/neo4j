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
package org.neo4j.kernel.impl.core;

import java.util.Collection;

import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.transaction.command.RelationshipHoles;

public interface CacheAccessBackDoor
{
    void removeNodeFromCache( long nodeId );

    void removeRelationshipFromCache( long id );

    void removeRelationshipTypeFromCache( int id );

    void removePropertyKeyFromCache( int id );

    void removeLabelFromCache( int id );

    void removeGraphPropertiesFromCache();

    void addSchemaRule( SchemaRule schemaRule );

    void removeSchemaRuleFromCache( long id );

    void addRelationshipTypeToken( Token type );

    void addLabelToken( Token labelId );

    void addPropertyKeyToken( Token index );

    void applyLabelUpdates( Collection<NodeLabelUpdate> labelUpdates );

    /**
     * Patches the relationship chain loading parts of the start and end nodes of deleted relationships. This is
     * a good idea to call when deleting relationships, otherwise the in memory representation of relationship chains
     * may become damaged.
     * This is not expected to remove the deleted relationship from the cache - use
     * {@link #removeRelationshipFromCache(long)} for that purpose before calling this method.
     *
     * @param relId The relId of the relationship deleted
     * @param type type of the relationship deleted
     * @param firstNodeId The relId of the first node
     * @param firstNodeNextRelId The next relationship relId of the first node in its relationship chain
     * @param secondNodeId The relId of the second node
     * @param secondNodeNextRelId The next relationship relId of the second node in its relationship chain
     */
    void patchDeletedRelationshipNodes( long nodeId, RelationshipHoles holes );
}
