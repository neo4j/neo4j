/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

public class UdcConstants {
    public static final String ID = "id";
    public static final String TAGS = "tags";
    public static final String CLUSTER_HASH = "cluster";
    public static final String REGISTRATION = "reg";
    public static final String PING = "p";
    public static final String DISTRIBUTION = "dist";
    public static final String USER_AGENTS = "ua";

    public static final String VERSION = "v";
    public static final String REVISION = "revision";
    public static final String EDITION = "edition";
    public static final String SOURCE = "source";

    public static final String MAC = "mac";
    public static final String NUM_PROCESSORS = "numprocs";
    public static final String TOTAL_MEMORY = "totalmem";
    public static final String HEAP_SIZE = "heapsize";

    public static final String NODE_IDS_IN_USE = "nodeids";
    public static final String RELATIONSHIP_IDS_IN_USE = "relids";
    public static final String PROPERTY_IDS_IN_USE = "propids";
    public static final String LABEL_IDS_IN_USE = "labelids";

    public static final String UDC_PROPERTY_PREFIX = "neo4j.ext.udc";
    public static final String OS_PROPERTY_PREFIX = "os";
    public static final String UNKNOWN_DIST = "unknown";
}
