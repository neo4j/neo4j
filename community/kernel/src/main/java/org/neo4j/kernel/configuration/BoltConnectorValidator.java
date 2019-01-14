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
import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
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
            setting = (BaseSetting) setting( settingName, BOOLEAN, "false" );
            setting.setDescription( "Enable this connector." );
            break;
        case "type":
            setting =
                    (BaseSetting) setting( settingName, options( Connector.ConnectorType.class ), NO_DEFAULT );
            setting.setDeprecated( true );
            setting.setDescription( "Connector type. This setting is deprecated and its value will instead be " +
                    "inferred from the name of the connector." );
            break;
        case "tls_level":
            setting = (BaseSetting) setting( settingName, options( BoltConnector.EncryptionLevel.class ),
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
        case "thread_pool_min_size":
            setting = (BaseSetting) setting( settingName, INTEGER, NO_DEFAULT );
            setting.setDescription( "The number of threads to keep in the thread pool bound to this connector, even if they are idle." );
            break;
        case "thread_pool_max_size":
            setting = (BaseSetting) setting( settingName, INTEGER, NO_DEFAULT );
            setting.setDescription( "The maximum number of threads allowed in the thread pool bound to this connector." );
            break;
        case "thread_pool_keep_alive":
            setting = (BaseSetting) setting( settingName, DURATION, NO_DEFAULT );
            setting.setDescription( "The maximum time an idle thread in the thread pool bound to this connector will wait for new tasks." );
            break;
        case "unsupported_thread_pool_queue_size":
            setting = (BaseSetting) setting( settingName, INTEGER, NO_DEFAULT );
            setting.setDescription( "The queue size of the thread pool bound to this connector (-1 for unbounded, 0 for direct handoff, > 0 for bounded)" );
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
