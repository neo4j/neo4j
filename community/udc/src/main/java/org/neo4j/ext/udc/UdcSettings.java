/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.ext.udc;

import static org.neo4j.helpers.Settings.ANY;
import static org.neo4j.helpers.Settings.BOOLEAN;
import static org.neo4j.helpers.Settings.INTEGER;
import static org.neo4j.helpers.Settings.STRING;
import static org.neo4j.helpers.Settings.TRUE;
import static org.neo4j.helpers.Settings.illegalValueMessage;
import static org.neo4j.helpers.Settings.matches;
import static org.neo4j.helpers.Settings.min;
import static org.neo4j.helpers.Settings.setting;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.Settings;

public class UdcSettings
{
    /**
     * Configuration key for enabling the UDC extension.
     */
    public static final Setting<Boolean> udc_enabled = setting( "neo4j.ext.udc.enabled", BOOLEAN, TRUE );

    /**
     * Configuration key for the first delay, expressed
     * in milliseconds.
     */
    public static final Setting<Integer> first_delay =
            setting( "neo4j.ext.udc.first_delay", INTEGER, Integer.toString( 10 * 1000 * 60 ), min( 1 ) );

    /**
     * Configuration key for the interval for regular updates,
     * expressed in milliseconds.
     */
    public static final Setting<Integer> interval = setting( "neo4j.ext.udc.interval", INTEGER, Integer.toString(
            1000 * 60 * 60 * 24 ), min( 1 ) );

    /**
     * The host address to which UDC updates will be sent.
     * Should be of the form hostname[:port].
     */
    public static final Setting<String> udc_host = setting( "neo4j.ext.udc.host", STRING, "udc.neo4j.org" );

    /**
     * Configuration key for overriding the source parameter in UDC
     */
    public static final Setting<String> udc_source = setting( "neo4j.ext.udc.source", STRING, Settings.NO_DEFAULT,
            illegalValueMessage( "Must be a valid source", matches( ANY ) ) );

    /**
     * Unique registration id
     */
    public static final Setting<String> udc_registration_key = setting( "neo4j.ext.udc.reg", STRING, "unreg",
            illegalValueMessage( "Must be a valid registration id", matches( ANY ) ) );
}