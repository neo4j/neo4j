/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.store.format.highlimit.v320;

import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.StandardFormatSettings;

/**
 * Reference class for high limit format settings.
 *
 * @see HighLimit
 */
public class HighLimitFormatSettingsV3_2_0
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
    static final int RELATIONSHIP_TYPE_TOKEN_MAXIMUM_ID_BITS = StandardFormatSettings.RELATIONSHIP_TYPE_TOKEN_MAXIMUM_ID_BITS;

    private HighLimitFormatSettingsV3_2_0()
    {
    }
}
