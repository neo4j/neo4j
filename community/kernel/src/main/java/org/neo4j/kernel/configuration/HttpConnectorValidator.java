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

import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nonnull;

import org.neo4j.graphdb.config.BaseSetting;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.HttpConnector.Encryption;

import static java.lang.String.format;
import static org.neo4j.kernel.configuration.Connector.ConnectorType.HTTP;
import static org.neo4j.kernel.configuration.HttpConnector.Encryption.NONE;
import static org.neo4j.kernel.configuration.HttpConnector.Encryption.TLS;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.advertisedAddress;
import static org.neo4j.kernel.configuration.Settings.describeOneOf;
import static org.neo4j.kernel.configuration.Settings.listenAddress;
import static org.neo4j.kernel.configuration.Settings.options;
import static org.neo4j.kernel.configuration.Settings.setting;

public class HttpConnectorValidator extends ConnectorValidator
{
    private static final Consumer<String> nullConsumer = s ->
    {

    };

    public HttpConnectorValidator()
    {
        super( HTTP );
    }

    @Override
    @Nonnull
    protected Optional<Setting<Object>> getSettingFor( @Nonnull String settingName, @Nonnull Map<String,String> params )
    {
        // owns has already verified that 'type' is correct and that this split is possible
        String[] parts = settingName.split( "\\." );
        final String name = parts[2];
        final String subsetting = parts[3];

        final boolean encrypted = encryptionSetting( name ).apply( params::get ) == Encryption.TLS;
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
        case "encryption":
            setting = encryptionSetting( name );
            setting.setDescription( "Enable TLS for this connector." );
            break;
        case "address":
            setting = listenAddress( settingName, defaultPort( name, params ) );
            setting.setDeprecated( true );
            setting.setReplacement( "dbms.connector." + name + ".listen_address" );
            setting.setDescription( "Address the connector should bind to. Deprecated and replaced by "
                    + setting.replacement().get() + "." );
            break;
        case "listen_address":
            setting = listenAddress( settingName, defaultPort( name, params ) );
            setting.setDescription( "Address the connector should bind to." );
            break;
        case "advertised_address":
            setting = advertisedAddress( settingName,
                    listenAddress( settingName, defaultPort( name, params ) ) );
            setting.setDescription( "Advertised address for this connector." );
            break;
        default:
            return Optional.empty();
        }

        // If not deprecated for other reasons
        if ( isDeprecatedConnectorName( name ) && !setting.deprecated() )
        {
            setting.setDeprecated( true );
            setting.setReplacement( format( "%s.%s.%s.%s", parts[0], parts[1],
                    encrypted ? "https" : "http",
                    subsetting) );
        }
        return Optional.of( setting );
    }

    /**
     * @param name of connector, like 'bob' in 'dbms.connector.bob.type = HTTP'
     * @param rawConfig to parse
     * @return the default for the encryption level designated for the HTTP connector
     */
    private int defaultPort( @Nonnull String name, @Nonnull Map<String,String> rawConfig )
    {
        switch ( name )
        {
        case "http":
            return Encryption.NONE.defaultPort;
        case "https":
            return TLS.defaultPort;
        default:
            Setting<Encryption> es = encryptionSetting( name );
            return es.apply( rawConfig::get ).defaultPort;
        }
    }

    @Nonnull
    private static Map<String,String> assertEncryption( @Nonnull String name,
            @Nonnull Setting<?> setting,
            @Nonnull Map<String,String> rawConfig ) throws InvalidSettingException
    {
        Map<String,String> result = setting.validate( rawConfig, nullConsumer );

        Optional<?> encryption = Optional.ofNullable( setting.apply( rawConfig::get ) );

        if ( "https".equalsIgnoreCase( name ) )
        {
            if ( encryption.isPresent() && encryption.get() != TLS )
            {
                throw new InvalidSettingException(
                        format( "'%s' is only allowed to be '%s'; not '%s'",
                                setting.name(), TLS.name(), encryption.get() ) );
            }
        }
        else if ( "http".equalsIgnoreCase( name ) )
        {
            if ( encryption.isPresent() && encryption.get() != NONE )
            {
                throw new InvalidSettingException(
                        format( "'%s' is only allowed to be '%s'; not '%s'",
                                setting.name(), NONE.name(), encryption.get() ) );
            }
        }

        return result;
    }

    @Nonnull
    public static BaseSetting<HttpConnector.Encryption> encryptionSetting( @Nonnull String name )
    {
        return encryptionSetting( name, Encryption.NONE );
    }

    @Nonnull
    public static BaseSetting<HttpConnector.Encryption> encryptionSetting( @Nonnull String name, Encryption
            defaultValue )
    {
        Setting<Encryption> s = setting( "dbms.connector." + name + ".encryption",
                options( Encryption.class ), defaultValue.name() );

        return new BaseSetting<Encryption>()
        {
            @Override
            public boolean deprecated()
            {
                // For HTTP the encryption is decided by the connector name
                return true;
            }

            @Override
            public Optional<String> replacement()
            {
                return Optional.empty();
            }

            @Override
            public boolean internal()
            {
                return false;
            }

            @Override
            public Optional<String> documentedDefaultValue()
            {
                return Optional.empty();
            }

            @Override
            public String valueDescription()
            {
                return describeOneOf( EnumSet.allOf( Encryption.class ) );
            }

            @Override
            public Optional<String> description()
            {
                return Optional.of( "Enable TLS for this connector. This is deprecated and is decided based on the " +
                        "connector name instead." );
            }

            @Override
            public String name()
            {
                return s.name();
            }

            @Override
            public void withScope( Function<String,String> scopingRule )
            {
                s.withScope( scopingRule );
            }

            @Override
            public String getDefaultValue()
            {
                return s.getDefaultValue();
            }

            @Override
            public Encryption from( Configuration config )
            {
                return s.from( config );
            }

            @Override
            public Encryption apply( Function<String,String> stringStringFunction )
            {
                return s.apply( stringStringFunction );
            }

            @Override
            public Map<String,String> validate( Map<String,String> rawConfig, Consumer<String> warningConsumer )
                    throws InvalidSettingException
            {
                Map<String,String> result = s.validate( rawConfig, warningConsumer );
                assertEncryption( name, s, rawConfig );
                return result;
            }
        };
    }
}
