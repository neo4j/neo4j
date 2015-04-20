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
package org.neo4j.kernel;

/**
 * @deprecated This will be moved to internal packages in the next major release.
 */
@Deprecated
public enum IdType
{
    NODE( 35, false ),
    RELATIONSHIP( 35, false ),
    PROPERTY( 36, true ),
    STRING_BLOCK( 36, true ),
    ARRAY_BLOCK( 36, true ),
    PROPERTY_KEY_TOKEN( false ),
    PROPERTY_KEY_TOKEN_NAME( false ),
    RELATIONSHIP_TYPE_TOKEN( 16, false ),
    RELATIONSHIP_TYPE_TOKEN_NAME( false ),
    LABEL_TOKEN( false ),
    LABEL_TOKEN_NAME( false ),
    NEOSTORE_BLOCK( false ),
    SCHEMA( 35, false ),
    NODE_LABELS( 35, true ),
    RELATIONSHIP_GROUP( 35, true );

    private final long max;
    private final boolean allowAggressiveReuse;

    private IdType( boolean allowAggressiveReuse )
    {
        this( 32, allowAggressiveReuse );
    }

    private IdType( int bits, boolean allowAggressiveReuse )
    {
        this.allowAggressiveReuse = allowAggressiveReuse;
        this.max = (long)Math.pow( 2, bits )-1;
    }

    public long getMaxValue()
    {
        return this.max;
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
