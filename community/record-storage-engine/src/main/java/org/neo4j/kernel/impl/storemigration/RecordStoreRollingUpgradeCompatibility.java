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
package org.neo4j.kernel.impl.storemigration;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.storageengine.migration.RollingUpgradeCompatibility;

import static org.neo4j.storageengine.api.StoreVersion.versionLongToString;

public class RecordStoreRollingUpgradeCompatibility implements RollingUpgradeCompatibility
{
    /**
     * A map between store versions and the versions they are compatible with for rolling upgrade
     */
    private final Map<String,Set<String>> compatibilityMap = new HashMap<>();

    public RecordStoreRollingUpgradeCompatibility( Iterable<RecordFormats> knownFormats )
    {
        //Known formats, add compatibilities
        for ( RecordFormats knownFormat : knownFormats )
        {
            Set<String> compatibleVersions = new HashSet<>();
            for ( RecordFormats recordFormats : knownFormat.compatibleVersionsForRollingUpgrade() )
            {
                compatibleVersions.add( recordFormats.storeVersion() );
            }
            compatibilityMap.put( knownFormat.storeVersion(), compatibleVersions );
        }

        //Known compatibilities. Future versions in newer releases can be added manually here to add knowledge about if that is compatible to our local one
        //e.g compatibilityMap.put( "SF4.3.0", Set.of( STANDARD_V4_0.versionString() ) );
    }

    @Override
    public boolean isVersionCompatibleForRollingUpgrade( String format, String otherFormat )
    {
        return Objects.equals( format, otherFormat ) ||
                compatibilityMap.getOrDefault( otherFormat, Collections.emptySet() ).contains( format );
    }

    @Override
    public boolean isVersionCompatibleForRollingUpgrade( long format, long otherFormat )
    {
        return isVersionCompatibleForRollingUpgrade( versionLongToString( format ), versionLongToString( otherFormat ) );
    }
}
