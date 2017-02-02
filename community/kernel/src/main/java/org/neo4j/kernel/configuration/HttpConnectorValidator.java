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
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nonnull;

import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.HttpConnector.Encryption;

import static org.neo4j.kernel.configuration.Connector.ConnectorType.HTTP;
import static org.neo4j.kernel.configuration.HttpConnector.Encryption.NONE;
import static org.neo4j.kernel.configuration.HttpConnector.Encryption.TLS;
import static org.neo4j.kernel.configuration.Settings.advertisedAddress;
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
    protected Optional<Setting> getSettingFor( @Nonnull String settingName, @Nonnull Map<String,String> params )
    {
        // owns has already verified that 'type' is correct and that this split is possible
        String[] parts = settingName.split( "\\." );
        final String name = parts[2];
        final String subsetting = parts[3];

        switch ( subsetting )
        {
        case "encryption":
            return Optional.of( encryptionSetting( name ) );
        case "address":
        case "listen_address":
            return Optional.of( listenAddress( settingName, defaultPort( name, params ) ) );
        case "advertised_address":
            return Optional.of( advertisedAddress( settingName,
                    listenAddress( settingName, defaultPort( name, params ) ) ) );
        default:
            return super.getSettingFor( settingName, params );
        }
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
            @Nonnull Map<String,String> rawConfig )
    {
        Map<String,String> result = setting.validate( rawConfig, nullConsumer );

        Optional<?> encryption = Optional.ofNullable( setting.apply( rawConfig::get ) );

        if ( "https".equalsIgnoreCase( name ) )
        {
            if ( encryption.isPresent() && !TLS.equals( encryption.get() ) )
            {
                throw new InvalidSettingException(
                        String.format( "'%s' is only allowed to be '%s'; not '%s'",
                                setting.name(), TLS.name(), encryption.get() ) );
            }
        }
        else if ( "http".equalsIgnoreCase( name ) )
        {
            if ( encryption.isPresent() && !NONE.equals( encryption.get() ) )
            {
                throw new InvalidSettingException(
                        String.format( "'%s' is only allowed to be '%s'; not '%s'",
                                setting.name(), NONE.name(), encryption.get() ) );
            }
        }

        return result;
    }

    @Nonnull
    public static Setting<HttpConnector.Encryption> encryptionSetting( @Nonnull String name )
    {
        return encryptionSetting( name, Encryption.NONE );
    }

    @Nonnull
    public static Setting<HttpConnector.Encryption> encryptionSetting( @Nonnull String name, Encryption defaultValue )
    {
        Setting<Encryption> s = setting( "dbms.connector." + name + ".encryption",
                options( Encryption.class ), defaultValue.name() );

        return new Setting<Encryption>()
        {
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
