/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.id.configuration;

import org.neo4j.kernel.impl.store.id.IdType;

/**
 * Configuration for any specific id type
 * @see IdType
 * @see IdTypeConfigurationProvider
 */
public class IdTypeConfiguration
{
    static final int DEFAULT_GRAB_SIZE = 1024;
    static final int AGGRESSIVE_GRAB_SIZE = 50000;

    private final boolean allowAggressiveReuse;

    public IdTypeConfiguration( boolean allowAggressiveReuse )
    {
        this.allowAggressiveReuse = allowAggressiveReuse;
    }

    public boolean allowAggressiveReuse()
    {
        return allowAggressiveReuse;
    }

    public int getGrabSize()
    {
        return allowAggressiveReuse ? AGGRESSIVE_GRAB_SIZE : DEFAULT_GRAB_SIZE;
    }
}
