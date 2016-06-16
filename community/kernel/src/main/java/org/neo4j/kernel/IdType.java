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
package org.neo4j.kernel;

import java.util.Arrays;
import java.util.List;

/**
 * @deprecated This will be moved to internal packages in the next major release.
 */
@Deprecated
public class IdType
{
    public static final IdType NODE = new IdType( 35, Name.NODE, false );
    public static final IdType RELATIONSHIP = new IdType( 35, Name.RELATIONSHIP, false );
    public static final IdType PROPERTY = new IdType( 36, Name.PROPERTY, true );
    public static final IdType STRING_BLOCK = new IdType( 36, Name.STRING_BLOCK, true );
    public static final IdType ARRAY_BLOCK = new IdType( 36, Name.ARRAY_BLOCK, true );
    public static final IdType PROPERTY_KEY_TOKEN = new IdType( Name.PROPERTY_KEY_TOKEN, false );
    public static final IdType PROPERTY_KEY_TOKEN_NAME = new IdType( Name.PROPERTY_KEY_TOKEN_NAME, false );
    public static final IdType RELATIONSHIP_TYPE_TOKEN = new IdType( 16, Name.RELATIONSHIP_TYPE_TOKEN, false );
    public static final IdType RELATIONSHIP_TYPE_TOKEN_NAME = new IdType( Name.RELATIONSHIP_TYPE_TOKEN_NAME, false );
    public static final IdType LABEL_TOKEN = new IdType( Name.LABEL_TOKEN, false );
    public static final IdType LABEL_TOKEN_NAME = new IdType( Name.LABEL_TOKEN_NAME, false );
    public static final IdType NEOSTORE_BLOCK = new IdType( Name.NEOSTORE_BLOCK, false );
    public static final IdType SCHEMA = new IdType( 35, Name.SCHEMA, false );
    public static final IdType NODE_LABELS = new IdType( 35, Name.NODE_LABELS, true );
    public static final IdType RELATIONSHIP_GROUP = new IdType( 35, Name.RELATIONSHIP_GROUP, false );

    private static final List<IdType> ALL_ID_TYPES = Arrays.asList( NODE, RELATIONSHIP, PROPERTY, STRING_BLOCK,
            ARRAY_BLOCK, PROPERTY_KEY_TOKEN, PROPERTY_KEY_TOKEN_NAME, RELATIONSHIP_TYPE_TOKEN,
            RELATIONSHIP_TYPE_TOKEN_NAME, LABEL_TOKEN, LABEL_TOKEN_NAME, NEOSTORE_BLOCK,
            SCHEMA, NODE_LABELS, RELATIONSHIP_GROUP );

    /**
     * Get all defined id types.
     * @return list of all id types.
     */
    public static List<IdType> getAllIdTypes()
    {
        return ALL_ID_TYPES;
    }

    public static IdType forName(IdType.Name typeName)
    {
        for ( IdType idType : ALL_ID_TYPES )
        {
            if ( idType.getName().equals( typeName ) )
            {
                return idType;
            }
        }
        throw new IllegalArgumentException( "IdType with requested name: " + typeName + " does not exist." );
    }

    private final long max;
    private boolean allowAggressiveReuse;
    private final Name name;

    private IdType( Name name, boolean allowAggressiveReuse )
    {
        this( 32, name, allowAggressiveReuse );
    }

    private IdType( int bits, Name name, boolean allowAggressiveReuse )
    {
        this.allowAggressiveReuse = allowAggressiveReuse;
        this.name = name;
        this.max = (long) Math.pow( 2, bits ) - 1;
    }

    public int ordinal()
    {
        return name.ordinal();
    }

    public long getMaxValue()
    {
        return this.max;
    }

    public void setAllowAggressiveReuse( boolean allowAggressiveReuse )
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

    public Name getName()
    {
        return name;
    }

    @Override
    public String toString()
    {
        return "IdType{" +
               "max=" + max +
               ", allowAggressiveReuse=" + allowAggressiveReuse +
               ", name=" + name +
               '}';
    }

    public enum Name
    {
        NODE,
        RELATIONSHIP,
        PROPERTY,
        STRING_BLOCK,
        ARRAY_BLOCK,
        PROPERTY_KEY_TOKEN,
        PROPERTY_KEY_TOKEN_NAME,
        RELATIONSHIP_TYPE_TOKEN,
        RELATIONSHIP_TYPE_TOKEN_NAME,
        LABEL_TOKEN,
        LABEL_TOKEN_NAME,
        NEOSTORE_BLOCK,
        SCHEMA,
        NODE_LABELS,
        RELATIONSHIP_GROUP
    }
}
