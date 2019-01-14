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
package org.neo4j.ext.udc;

import java.util.function.Function;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.HostnamePort;

import static org.neo4j.kernel.configuration.Settings.ANY;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.HOSTNAME_PORT;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.buildSetting;
import static org.neo4j.kernel.configuration.Settings.illegalValueMessage;
import static org.neo4j.kernel.configuration.Settings.matches;
import static org.neo4j.kernel.configuration.Settings.min;
import static org.neo4j.kernel.configuration.Settings.setting;

@Description( "Usage Data Collector configuration settings" )
public class UdcSettings implements LoadableConfig
{
    /** Configuration key for enabling the UDC extension. */
    @Description( "Enable the UDC extension." )
    public static final Setting<Boolean> udc_enabled = setting(
            "dbms.udc.enabled", Enabled.UNLESS_EXPLICITLY_DISABLED, Enabled.AS_DEFAULT_VALUE );

    /** Configuration key for the first delay, expressed in milliseconds. */
    @Internal
    public static final Setting<Integer> first_delay =
            buildSetting( "unsupported.dbms.udc.first_delay", INTEGER, Integer.toString( 10 * 1000 * 60 ) ).constraint( min( 1 ) ).build();

    /** Configuration key for the interval for regular updates, expressed in milliseconds. */
    @Internal
    public static final Setting<Integer> interval = buildSetting( "unsupported.dbms.udc.interval", INTEGER, Integer.toString(
            1000 * 60 * 60 * 24 ) ).constraint( min( 1 ) ).build();

    /** The host address to which UDC updates will be sent. Should be of the form hostname[:port]. */
    @Internal
    public static final Setting<HostnamePort> udc_host = setting( "unsupported.dbms.udc.host", HOSTNAME_PORT,
            "udc.neo4j.org" );

    /** Configuration key for overriding the source parameter in UDC */
    @Internal
    public static final Setting<String> udc_source = buildSetting( "unsupported.dbms.udc.source", STRING, "maven" )
            .constraint(
            illegalValueMessage( "Must be a valid source", matches( ANY ) ) ).build();

    /** Unique registration id */
    @Internal
    public static final Setting<String> udc_registration_key = buildSetting( "unsupported.dbms.udc.reg", STRING, "unreg" ).constraint(
            illegalValueMessage( "Must be a valid registration id", matches( ANY ) ) ).build();

    private enum Enabled implements Function<String,Boolean>
    {
        /** Only explicitly configuring this as 'false' disables UDC, all other values leaves UDC enabled. */
        UNLESS_EXPLICITLY_DISABLED;
        /**
         * Explicitly allocate a String here so that we know it is unique and can do identity equality comparisons on it
         * to detect that the default value has been used.
         */
        @SuppressWarnings( "RedundantStringConstructorCall" )
        static final String AS_DEFAULT_VALUE = new String( TRUE );

        @Override
        public Boolean apply( String from )
        {
            // Perform identity equality here to differentiate between the default value (which is explicitly allocated
            // as a new instance, and is thus known to be unique), and explicitly being configured as "true".
            //noinspection StringEquality
            if ( from == AS_DEFAULT_VALUE ) // yes, this should really be ==
            { // the default value, as opposed to explicitly configured to "true"
                // Should result in UDC being enabled, unless one of the other ways to configure explicitly disables it
                String enabled = System.getProperty( udc_enabled.name() );
                if ( FALSE.equalsIgnoreCase( enabled ) )
                { // the 'enabled' system property tries to disable UDC
                    String disabled = System.getProperty( udc_disabled() );
                    if ( disabled == null || disabled.equalsIgnoreCase( TRUE ) )
                    { // the 'disabled' system property does nothing to enable UDC
                        return Boolean.FALSE;
                    }
                }
                else if ( TRUE.equalsIgnoreCase( System.getProperty( udc_disabled() ) ) )
                { // the 'disabled' system property tries to disable UDC
                    return enabled != null; // only disable if 'enabled' was not defined
                }
                return Boolean.TRUE;
            }
            else if ( FALSE.equalsIgnoreCase( from ) )
            { // the setting tries to disable UDC
                // if any other way of configuring UDC enables it, trust that instead.
                String enabled = System.getProperty( udc_enabled.name() );
                String disabled = System.getProperty( udc_disabled() );
                if ( enabled == null || enabled.equalsIgnoreCase( FALSE ) )
                { // the 'enabled' system property does nothing to enable UDC
                    if ( disabled == null || disabled.equalsIgnoreCase( TRUE ) )
                    { // the 'disabled' system property does nothing to enable UDC
                        return Boolean.FALSE;
                    }
                }
                return Boolean.TRUE;
            }
            else
            { // the setting enabled UDC
                return Boolean.TRUE;
            }
        }

        @Override
        public String toString()
        {
            return "a boolean";
        }

        private static String udc_disabled()
        {
            return udc_enabled.name().replace( "enabled", "disable" );
        }
    }
}
