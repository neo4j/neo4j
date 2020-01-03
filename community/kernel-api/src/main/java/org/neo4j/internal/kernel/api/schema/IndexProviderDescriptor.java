/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.kernel.api.schema;

public class IndexProviderDescriptor
{
    /**
     * Indicate that {@link IndexProviderDescriptor} has not yet been decided.
     * Specifically before transaction that create a new index has committed.
     */
    public static final IndexProviderDescriptor UNDECIDED = new IndexProviderDescriptor( "Undecided", "0" );

    private final String key;
    private final String version;

    public IndexProviderDescriptor( String key, String version )
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "null provider key prohibited" );
        }
        if ( key.length() == 0 )
        {
            throw new IllegalArgumentException( "empty provider key prohibited" );
        }
        if ( version == null )
        {
            throw new IllegalArgumentException( "null provider version prohibited" );
        }

        this.key = key;
        this.version = version;
    }

    public String getKey()
    {
        return key;
    }

    public String getVersion()
    {
        return version;
    }

    /**
     * @return a combination of {@link #getKey()} and {@link #getVersion()} with a '-' in between.
     */
    public String name()
    {
        return key + "-" + version;
    }

    @Override
    public int hashCode()
    {
        return ( 23 + key.hashCode() ) ^ version.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof IndexProviderDescriptor )
        {
            IndexProviderDescriptor otherDescriptor = (IndexProviderDescriptor) obj;
            return key.equals( otherDescriptor.getKey() ) && version.equals( otherDescriptor.getVersion() );
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "{key=" + key + ", version=" + version + "}";
    }
}
