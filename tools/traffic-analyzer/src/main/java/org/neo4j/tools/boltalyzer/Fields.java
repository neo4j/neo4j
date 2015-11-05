/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.tools.boltalyzer;

import java.net.InetAddress;
import java.nio.ByteBuffer;

public class Fields
{
    public static final Field<Long> timestamp = Field.field( "ts" );
    public static final Field<InetAddress> src = Field.field( "src" );
    public static final Field<Integer> srcPort = Field.field( "srcPort" );
    public static final Field<InetAddress> dst = Field.field( "dst" );
    public static final Field<Integer> dstPort = Field.field( "dstPort" );
    public static final Field<ByteBuffer> payload = Field.field( "raw" );
    public static final Field<String> connectionKey = Field.field( "connectionKey" );
    public static final Field<String> description = Field.field( "description" );
    public static final Field<AnalyzedSession> session = Field.field( "session" );
    public static final Field<String> logicalSource = Field.field( "logicalSource" );
}
