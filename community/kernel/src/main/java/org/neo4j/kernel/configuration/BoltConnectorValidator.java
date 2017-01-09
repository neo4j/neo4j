/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.configuration;

import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;

import org.neo4j.graphdb.config.Setting;

import static org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.kernel.configuration.Connector.ConnectorType.BOLT;
import static org.neo4j.kernel.configuration.Settings.advertisedAddress;
import static org.neo4j.kernel.configuration.Settings.listenAddress;
import static org.neo4j.kernel.configuration.Settings.options;
import static org.neo4j.kernel.configuration.Settings.setting;

public class BoltConnectorValidator extends ConnectorValidator
{
    public BoltConnectorValidator()
    {
        super( BOLT );
    }

    @Override
    @Nonnull
    protected Optional<Setting> getSettingFor( @Nonnull String settingName, @Nonnull Map<String,String> params )
    {
        // owns has already verified that 'type' is correct and that this split is possible
        String[] parts = settingName.split( "\\." );
        final String subsetting = parts[3];

        switch ( subsetting )
        {
        case "tls_level":
            return Optional.of( setting( settingName, options( BoltConnector.EncryptionLevel.class ),
                    OPTIONAL.name() ) );
        case "address":
        case "listen_address":
            return Optional.of( listenAddress( settingName, 7687 ) );
        case "advertised_address":
            return Optional.of( advertisedAddress( settingName,
                    listenAddress( settingName, 7687 ) ) );
        default:
            return super.getSettingFor( settingName, params );
        }
    }
}
