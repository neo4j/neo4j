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

import org.neo4j.graphdb.config.BaseSetting;
import org.neo4j.graphdb.config.Setting;

import static java.lang.String.format;
import static org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.kernel.configuration.Connector.ConnectorType.BOLT;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
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
    protected Optional<Setting<Object>> getSettingFor( @Nonnull String settingName, @Nonnull Map<String,String> params )
    {
        // owns has already verified that 'type' is correct and that this split is possible
        String[] parts = settingName.split( "\\." );
        final String name = parts[2];
        final String subsetting = parts[3];

        BaseSetting setting;

        switch ( subsetting )
        {
        case "enabled":
            setting = setting( settingName, BOOLEAN, "false" );
            setting.setDescription( "Enable this connector." );
            break;
        case "type":
            setting = setting( settingName, options( Connector.ConnectorType.class ), NO_DEFAULT );
            setting.setDeprecated( true );
            setting.setDescription( "Connector type. This setting is deprecated and its value will instead be " +
                    "inferred from the name of the connector." );
            break;
        case "tls_level":
            setting = setting( settingName, options( BoltConnector.EncryptionLevel.class ),
                    OPTIONAL.name() );
            setting.setDescription( "Encryption level to require this connector to use." );
            break;
        case "address":
            setting = listenAddress( settingName, 7687 );
            setting.setDeprecated( true );
            setting.setReplacement( "dbms.connector." + name + ".listen_address" );
            setting.setDescription( "Address the connector should bind to. Deprecated and replaced by "
                    + setting.replacement().get() + "." );
            break;
        case "listen_address":
            setting = listenAddress( settingName, 7687 );
            setting.setDescription( "Address the connector should bind to." );
            break;
        case "advertised_address":
            setting = advertisedAddress( settingName,
                    listenAddress( settingName, 7687 ) );
            setting.setDescription( "Advertised address for this connector." );
            break;
        default:
            return Optional.empty();
        }

        // If not deprecated for other reasons
        if ( isDeprecatedConnectorName( name ) && !setting.deprecated() )
        {
            setting.setDeprecated( true );
            setting.setReplacement( format( "%s.%s.%s.%s", parts[0], parts[1], "bolt", subsetting ) );
        }
        return Optional.of( setting );
    }
}
