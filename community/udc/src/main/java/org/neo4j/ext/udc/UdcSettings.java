/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.neo4j.graphdb.factory.GraphDatabaseSetting.ANY;

import org.neo4j.graphdb.factory.Default;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

public class UdcSettings
{

    // This is a work around for GraphDatabaseSettings no longer containing type information.
    // We should introduce an internal list of settings with type information in the kernel,
    // then we would not have to duplicate settings here.

    public static final GraphDatabaseSetting<String> udc_source = new GraphDatabaseSetting.StringSetting(
            GraphDatabaseSettings.udc_source.name(), ANY, "Must be a valid source");

    public static final GraphDatabaseSetting<Boolean> udc_enabled = new GraphDatabaseSetting.BooleanSetting(
            GraphDatabaseSettings.udc_enabled.name());

    /**
     * Configuration key for the first delay, expressed
     * in milliseconds.
     */
    @Default( ""+10 * 1000 * 60 )
    public static final GraphDatabaseSetting<Integer> first_delay = new GraphDatabaseSetting.IntegerSetting("neo4j.ext.udc.first_delay", "Must be nr of milliseconds to delay", 1, null);

    /**
     * Configuration key for the interval for regular updates,
     * expressed in milliseconds.
     */
    @Default(""+1000 * 60 * 60 * 24)
    public static final GraphDatabaseSetting<Integer> interval = new GraphDatabaseSetting.IntegerSetting("neo4j.ext.udc.interval", "Must be nr of milliseconds of the interval for checking", 1, null);

    /**
     * The host address to which UDC updates will be sent.
     * Should be of the form hostname[:port].
     */
    @Default( "udc.neo4j.org" )
    public static final GraphDatabaseSetting<String> udc_host = new GraphDatabaseSetting.StringSetting(  "neo4j.ext.udc.host", ANY, "Must be a valid hostname");

    /**
     * Unique registration id
     */
    @Default( "unreg" )
    public static final GraphDatabaseSetting<String> udc_registration_key = new GraphDatabaseSetting.StringSetting( "neo4j.ext.udc.reg", ANY, "Must be a valid registration id" );

}
