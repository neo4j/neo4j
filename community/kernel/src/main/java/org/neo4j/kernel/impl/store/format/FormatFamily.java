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
     * Check if new record format family is higher then old record format family.
     * New format family is higher then old one in case when it safe to migrate from old format into new
     * format in terms of format capabilities and determined by family rank: new family is higher if it's rank is higher
     * then rank
     * of old family
     * @param newFormat new record format
     * @param oldFormat old record format
     * @return true if new record format family is higher
     */
    public static boolean isHigherFamilyFormat( RecordFormats newFormat, RecordFormats oldFormat )
    {
        return oldFormat.getFormatFamily().compareTo( newFormat.getFormatFamily() ) < 0;
    }

    /**
     * Check if record formats have same format family
     * @param recordFormats1 first record format
     * @param recordFormats2 second record format
     * @return true if formats have the same format family
     */
    public static boolean isSameFamily( RecordFormats recordFormats1, RecordFormats recordFormats2 )
    {
        return recordFormats1.getFormatFamily().equals( recordFormats2.getFormatFamily() );
    }

    /**
     * Check if new record format family is lower then old record format family.
     * New format family is lower then old one in case when its not safe to migrate from old format into new
     * format in terms of format capabilities and determined by family rank: new family is lower if it's rank is lower
     * then rank of old family
     * @param newFormat new record format
     * @param oldFormat old record format
     * @return true if new record format family is lower
     */
    public static boolean isLowerFamilyFormat( RecordFormats newFormat, RecordFormats oldFormat )
    {
        return oldFormat.getFormatFamily().compareTo( newFormat.getFormatFamily() ) > 0;
    }
}
