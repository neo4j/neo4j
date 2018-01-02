/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.udc;

import org.neo4j.concurrent.RecentK;
import org.neo4j.function.Supplier;

import static org.neo4j.udc.UsageDataKey.key;

/**
 * Inventory of common keys. This list is not exhaustive, and all items listed may not be available.
 * Still, this serves as a useful starting point for what you can expect to find, and new items added are
 * encouraged to have their keys listed here.
 */
public class UsageDataKeys
{
    /** Edition of Neo4j running, eg 'community' or 'enterprise' */
    public static final UsageDataKey<Edition> edition = key( "neo4j.edition", Edition.unknown );

    /** Version of Neo4j running, eg. 1.2.3-RC1 */
    public static final UsageDataKey<String> version = key( "neo4j.version", "N/A" );

    /** Revision of Neo4j running, a link back to source control revision ids. */
    public static final UsageDataKey<String> revision = key( "neo4j.revision", "N/A" );

    /** Operational mode of the database */
    public static final UsageDataKey<OperationalMode> operationalMode = key( "neo4j.opMode", OperationalMode.unknown );

    /** Self-reported names of clients connecting to us. */
    public static final UsageDataKey<RecentK<String>> clientNames = key( "neo4j.clientNames", new Supplier<RecentK<String>>()
    {
        @Override
        public RecentK<String> get()
        {
            return new RecentK<>( 10 );
        }
    } );

    /** Cluster server ID */
    public static final UsageDataKey<String> serverId = key( "neo4j.serverId" );

    public enum OperationalMode
    {
        // Note, these are sent verbatum via UDC if UDC is enabled
        unknown,
        single,
        ha
    }

    public enum Edition
    {
        // Note, these are sent verbatum via UDC if UDC is enabled
        unknown,
        community,
        advanced,
        enterprise
    }
}
