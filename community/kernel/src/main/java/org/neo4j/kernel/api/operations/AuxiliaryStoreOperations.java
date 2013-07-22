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
package org.neo4j.kernel.api.operations;

import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.properties.Property;

/**
 * A more optimized way of storing built-up transaction state (usually when it's time for commit), instead of
 * delegating the same granularity of calls that made the changes in the transaction, down to the store.
 *
 * This class uses the same naming schema as StatementContext but uses the particle "Store" to differentiate
 * methods from their StatementContext counterparts.
 */
public interface AuxiliaryStoreOperations
{
    void nodeAddStoreProperty( long nodeId, Property property ) throws PropertyNotFoundException;

    void nodeChangeStoreProperty( long nodeId, Property previousProperty, Property property )
            throws PropertyNotFoundException;

    void relationshipAddStoreProperty( long relationshipId, Property property ) throws PropertyNotFoundException;

    void relationshipChangeStoreProperty( long relationshipId, Property previousProperty, Property property )
            throws PropertyNotFoundException;

    void nodeRemoveStoreProperty( long nodeId, Property property ) throws PropertyNotFoundException;

    void relationshipRemoveStoreProperty( long relationshipId, Property property ) throws PropertyNotFoundException;

    void graphAddStoreProperty( Property property ) throws PropertyNotFoundException;

    void graphChangeStoreProperty( Property previousProperty, Property property ) throws PropertyNotFoundException;

    void graphRemoveStoreProperty( Property property );

    void nodeDelete( long nodeId );

    void relationshipDelete( long relationshipId );
}
