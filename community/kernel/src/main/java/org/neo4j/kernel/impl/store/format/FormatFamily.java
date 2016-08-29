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

/**
 * Family of the format. Family of format is specific to a format across all version that format support.
 * Two formats in different versions should have same format family.
 * Family is one of the criteria that will determine if migration between formats is possible.
 */
public abstract class FormatFamily implements Comparable<FormatFamily>
{
    /**
     * Get format family name
     * @return family name
     */
    public abstract String getName();

    /**
     * get format family rank
     * @return rank
     */
    public abstract int rank();

    @Override
    public int compareTo( FormatFamily formatFamily )
    {
        return Integer.compare( this.rank(), formatFamily.rank() );
    }

    /**
     * Check if migration between provided font families is possible.
     * Family is upgradable if rank of the new family is higher then rank of old family
     * @param oldFamily old family
     * @param newFamily new family
     * @return true if upgrade between families is possible, false otherwise
     */
    public static boolean isUpgradable( FormatFamily oldFamily, FormatFamily newFamily )
    {
        return oldFamily.compareTo( newFamily ) < 0;
    }

    /**
     * Check if provided font families is not upgradable.
     * Family is not upgradable if rank of the new family is lower or equal to rank of old family
     * @param oldFamily old family
     * @param newFamily new family
     * @return true if families not upgradable, false otherwise
     */
    public static boolean isNotUpgradable( FormatFamily oldFamily, FormatFamily newFamily )
    {
        return oldFamily.compareTo( newFamily ) > 0;
    }
}
