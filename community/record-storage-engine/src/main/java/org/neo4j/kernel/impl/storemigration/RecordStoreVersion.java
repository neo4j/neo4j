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
package org.neo4j.kernel.impl.storemigration;

import java.util.Optional;

import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.format.Capability;
import org.neo4j.storageengine.api.format.CapabilityType;

public class RecordStoreVersion implements StoreVersion
{
    private final RecordFormats format;

    public RecordStoreVersion( RecordFormats format )
    {
        this.format = format;
    }

    @Override
    public String storeVersion()
    {
        return format.storeVersion();
    }

    @Override
    public boolean hasCapability( Capability capability )
    {
        return format.hasCapability( capability );
    }

    @Override
    public boolean hasCompatibleCapabilities( StoreVersion otherVersion, CapabilityType type )
    {
        return format.hasCompatibleCapabilities( RecordFormatSelector.selectForVersion( otherVersion.storeVersion() ), type );
    }

    @Override
    public String introductionNeo4jVersion()
    {
        return format.introductionVersion();
    }

    @Override
    public Optional<StoreVersion> successor()
    {
        return RecordFormatSelector.findSuccessor( format ).map( RecordStoreVersion::new );
    }

    @Override
    public boolean isCompatibleWith( StoreVersion otherVersion )
    {
        if ( !(otherVersion instanceof RecordStoreVersion) )
        {
            return false;
        }
        return RecordFormatSelector.isStoreAndConfigFormatsCompatible( format, ((RecordStoreVersion) otherVersion).format );
    }
}
