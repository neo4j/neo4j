/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.store.format.standard;

/**
 * Common low limit format settings.
 */
public final class StandardFormatSettings
{
    public static final int PROPERTY_TOKEN_MAXIMUM_ID_BITS = 24;
    static final int NODE_MAXIMUM_ID_BITS = 35;
    static final int RELATIONSHIP_MAXIMUM_ID_BITS = 35;
    static final int PROPERTY_MAXIMUM_ID_BITS = 36;
    public static final int DYNAMIC_MAXIMUM_ID_BITS = 36;
    public static final int LABEL_TOKEN_MAXIMUM_ID_BITS = 32;
    public static final int RELATIONSHIP_TYPE_TOKEN_MAXIMUM_ID_BITS = 16;
    static final int RELATIONSHIP_GROUP_MAXIMUM_ID_BITS = 35;

    private StandardFormatSettings()
    {
    }
}
