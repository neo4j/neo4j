/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format;

import static org.neo4j.helpers.ArrayUtil.contains;

/**
 * A collection of high level capabilities a store can have, should not be more granular than necessary
 * for differentiating different version from one another.
 */
public enum Capability
{
    /**
     * Store has schema support
     */
    SCHEMA( CapabilityType.STORE ),

    /**
     * Store has dense node support
     */
    DENSE_NODES( CapabilityType.FORMAT, CapabilityType.STORE ),

    /**
     * 3 bytes relationship type support
     */
    RELATIONSHIP_TYPE_3BYTES( CapabilityType.FORMAT, CapabilityType.STORE ),

    /**
     * Store has version trailers in the end of cleanly shut down store
     */
    VERSION_TRAILERS( CapabilityType.STORE ),

    /**
     * Lucene version 3.x
     */
    LUCENE_3( CapabilityType.INDEX ),

    /**
     * Lucene version 5.x
     */
    LUCENE_5( CapabilityType.INDEX );

    private final CapabilityType[] types;

    Capability( CapabilityType... types )
    {
        this.types = types;
    }

    public boolean isType( CapabilityType type )
    {
        return contains( types, type );
    }
}
