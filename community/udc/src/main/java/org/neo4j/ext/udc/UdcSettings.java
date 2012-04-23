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
import static org.neo4j.graphdb.factory.GraphDatabaseSetting.TRUE;

import org.neo4j.graphdb.factory.Default;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;

/**
 * Settings to use for the UDC extension. Use this with the GraphDatabaseBuilder
 */
public class UdcSettings
{
    /**
     * Configuration key for enabling the UDC extension. Set to "false"
     * to disable; any other value is considered false.
     */
    @Default( TRUE)
    public static final GraphDatabaseSetting.BooleanSetting udc_enabled = new GraphDatabaseSetting.BooleanSetting("neo4j.ext.udc.enabled");

    /**
     * Configuration key for the first delay, expressed
     * in milliseconds.
     */
    @Default( ""+10 * 1000 * 60 )
    public static final GraphDatabaseSetting.IntegerSetting first_delay = new GraphDatabaseSetting.IntegerSetting("neo4j.ext.udc.first_delay", "Must be nr of milliseconds to delay", 1, null);

    /**
     * Configuration key for the interval for regular updates,
     * expressed in milliseconds.
     */
    @Default(""+1000 * 60 * 60 * 24)
    public static final GraphDatabaseSetting.IntegerSetting interval = new GraphDatabaseSetting.IntegerSetting("neo4j.ext.udc.interval", "Must be nr of milliseconds of the interval for checking", 1, null);

    /**
     * The host address to which UDC updates will be sent.
     * Should be of the form hostname[:port].
     */
    @Default( "udc.neo4j.org" )
    public static final GraphDatabaseSetting.StringSetting udc_host = new GraphDatabaseSetting.StringSetting(  "neo4j.ext.udc.host", ANY, "Must be a valid hostname");

    /**
     * Configuration key for overriding the source parameter in UDC
     */
    public static final GraphDatabaseSetting.StringSetting udc_source = new GraphDatabaseSetting.StringSetting("neo4j.ext.udc.source", ANY, "Must be a valid source");

    /**
     * Unique registration id
     */
    @Default( "unreg" )
    public static final GraphDatabaseSetting.StringSetting udc_registration_key = new GraphDatabaseSetting.StringSetting( "neo4j.ext.udc.reg", ANY, "Must be a valid registration id" );
}
