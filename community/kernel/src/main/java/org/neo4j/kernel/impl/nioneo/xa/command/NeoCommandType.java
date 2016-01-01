/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa.command;

public interface NeoCommandType
{
    // means the first byte of the command record was only written but second
    // (saying what type) did not get written but the file still got expanded
    public static final byte NONE = (byte) 0;

    public static final byte NODE_COMMAND = (byte) 1;
    public static final byte PROP_COMMAND = (byte) 2;
    public static final byte REL_COMMAND = (byte) 3;
    public static final byte REL_TYPE_COMMAND = (byte) 4;
    public static final byte PROP_INDEX_COMMAND = (byte) 5;
    public static final byte NEOSTORE_COMMAND = (byte) 6;
    public static final byte SCHEMA_RULE_COMMAND = (byte) 7;
    public static final byte LABEL_KEY_COMMAND = (byte) 8;
    public static final byte REL_GROUP_COMMAND = (byte) 9;
}
