/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.store.format;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.storageengine.api.format.Capability;

/**
 * Family of the format. Family of format is specific to a format across all version that format support.
 * Two formats in different versions should have same format family.
 * Family is one of the criteria that will determine if migration between formats is possible.
 */
public enum FormatFamily
{
    standard( 0 ),
    aligned( 1 ),
    high_limit( 2 );

    private final int rank;
    private final Capability formatCapability;

    FormatFamily( int rank )
    {
        this.rank = rank;
        this.formatCapability = new RecordFormatFamilyCapability( this );
    }

    public Capability formatCapability()
    {
        return formatCapability;
    }

    /**
     * Check if this format family is higher ranked than another format family.
     * It is generally possible to migrate from a lower ranked family to a higher ranked family.
     * @param other family to compare with.
     * @return {@code true} if this family is higher ranked than {@code other}.
     */
    public boolean isHigherThan( FormatFamily other )
    {
        return rank > other.rank;
    }

    public GraphDatabaseSettings.DatabaseRecordFormat databaseRecordFormat()
    {
        return switch ( this )
                {
                    case standard -> GraphDatabaseSettings.DatabaseRecordFormat.standard;
                    case aligned -> GraphDatabaseSettings.DatabaseRecordFormat.aligned;
                    case high_limit -> GraphDatabaseSettings.DatabaseRecordFormat.high_limit;
                };
    }
}
