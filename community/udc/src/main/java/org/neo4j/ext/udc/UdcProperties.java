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

/**
 * Property keys used to configure the UDC extension.
 */
public interface UdcProperties {

    /**
     * Configuration key for the first delay, expressed
     * in milliseconds.
     */
    public static final String FIRST_DELAY_CONFIG_KEY = "neo4j.ext.udc.first_delay";

    /**
     * Configuration key for the interval for regular updates,
     * expressed in milliseconds.
     */
    public static final String INTERVAL_CONFIG_KEY = "neo4j.ext.udc.interval";

    /**
     * Configuration key for disabling the UDC extension. Set to "true"
     * to disable; any other value is considered false.
     */
    public static final String UDC_DISABLE_KEY = "neo4j.ext.udc.disable";


    /**
     * The host address to which UDC updates will be sent.
     * Should be of the form hostname[:port].
     */
    public static final String UDC_HOST_ADDRESS_KEY = "neo4j.ext.udc.host";

    /**
     * Configuration key for overriding the source parameter in UDC
     */
    public static final String UDC_SOURCE_KEY = "neo4j.ext.udc.source";
}
