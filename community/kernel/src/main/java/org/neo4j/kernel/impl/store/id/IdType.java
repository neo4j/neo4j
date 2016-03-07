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
package org.neo4j.kernel.impl.store.id;

public enum IdType
{
    NODE( false ),
    RELATIONSHIP( false ),
    PROPERTY( true ),
    STRING_BLOCK( true ),
    ARRAY_BLOCK( true ),
    PROPERTY_KEY_TOKEN( false ),
    PROPERTY_KEY_TOKEN_NAME( false ),
    RELATIONSHIP_TYPE_TOKEN( false ),
    RELATIONSHIP_TYPE_TOKEN_NAME( false ),
    LABEL_TOKEN( false ),
    LABEL_TOKEN_NAME( false ),
    NEOSTORE_BLOCK( false ),
    SCHEMA( false ),
    NODE_LABELS( true ),
    RELATIONSHIP_GROUP( false );

    private final boolean allowAggressiveReuse;


    IdType( boolean allowAggressiveReuse )
    {
        this.allowAggressiveReuse = allowAggressiveReuse;
    }

    public boolean allowAggressiveReuse()
    {
        return allowAggressiveReuse;
    }

    public int getGrabSize()
    {
        return allowAggressiveReuse ? 50000 : 1024;
    }
}
