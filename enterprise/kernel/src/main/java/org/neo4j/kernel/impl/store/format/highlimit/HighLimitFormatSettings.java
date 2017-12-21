/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store.format.highlimit;

import org.neo4j.kernel.impl.store.format.standard.StandardFormatSettings;

/**
 * Reference class for high limit format settings.
 *
 * @see HighLimit
 */
public class HighLimitFormatSettings
{
    /**
     * Default maximum number of bits that can be used to represent id
     */
    static final int DEFAULT_MAXIMUM_BITS_PER_ID = 50;

    static final int PROPERTY_MAXIMUM_ID_BITS = DEFAULT_MAXIMUM_BITS_PER_ID;
    static final int NODE_MAXIMUM_ID_BITS = DEFAULT_MAXIMUM_BITS_PER_ID;
    static final int RELATIONSHIP_MAXIMUM_ID_BITS = DEFAULT_MAXIMUM_BITS_PER_ID;
    static final int RELATIONSHIP_GROUP_MAXIMUM_ID_BITS = DEFAULT_MAXIMUM_BITS_PER_ID;
    static final int DYNAMIC_MAXIMUM_ID_BITS = DEFAULT_MAXIMUM_BITS_PER_ID;

    @SuppressWarnings( "unused" )
    static final int PROPERTY_TOKEN_MAXIMUM_ID_BITS = StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS;
    @SuppressWarnings( "unused" )
    static final int LABEL_TOKEN_MAXIMUM_ID_BITS = StandardFormatSettings.LABEL_TOKEN_MAXIMUM_ID_BITS;
    @SuppressWarnings( "unused" )
    static final int RELATIONSHIP_TYPE_TOKEN_MAXIMUM_ID_BITS = Byte.SIZE * 3;

    private HighLimitFormatSettings()
    {
    }
}
