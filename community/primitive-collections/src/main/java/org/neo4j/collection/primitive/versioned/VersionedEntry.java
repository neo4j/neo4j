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
package org.neo4j.collection.primitive.versioned;

class VersionedEntry<V>
{
    private static final int NO_VERSION = -1;

    private final VersionedCollection versionedCollection;
    private int currentVersion = NO_VERSION;
    private V currentValue;
    private int stableVersion = NO_VERSION;
    private V stableValue;

    VersionedEntry( VersionedCollection versionedCollection )
    {
        this.versionedCollection = versionedCollection;
    }

    VersionedEntry( int version, V value, VersionedCollection versionedCollection )
    {
        this.currentVersion = version;
        this.currentValue = value;
        this.versionedCollection = versionedCollection;
    }

    public int getCurrentVersion()
    {
        return currentVersion;
    }

    public V getCurrentValue()
    {
        return currentValue;
    }

    void setCurrentValue( V newValue, int newVersion )
    {
        if ( versionedCollection.stableVersion() >= currentVersion )
        {
            stableValue = currentValue;
            stableVersion = versionedCollection.stableVersion();
        }
        currentValue = newValue;
        currentVersion = newVersion;
    }

    V getStableValue()
    {
        if ( currentVersion <= versionedCollection.stableVersion() )
        {
            return currentValue;
        }
        else if ( stableVersion <= versionedCollection.stableVersion() )
        {
            return stableValue;
        }
        return null;
    }

    public void clear()
    {
        currentValue = null;
        currentVersion = NO_VERSION;
        stableValue = null;
        stableVersion = NO_VERSION;
    }

    public boolean isOrphan()
    {
        return currentValue == null && getStableValue() == null;
    }
}
