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
package org.neo4j.kernel.impl.transaction.command;

public interface NeoCommandType
{
    // means the first byte of the command record was only written but second
    // (saying what type) did not get written but the file still got expanded
    byte NONE = (byte) 0;

    byte NODE_COMMAND = (byte) 1;
    byte PROP_COMMAND = (byte) 2;
    byte REL_COMMAND = (byte) 3;
    byte REL_TYPE_COMMAND = (byte) 4;
    byte PROP_INDEX_COMMAND = (byte) 5;
    byte NEOSTORE_COMMAND = (byte) 6;
    byte SCHEMA_RULE_COMMAND = (byte) 7;
    byte LABEL_KEY_COMMAND = (byte) 8;
    byte REL_GROUP_COMMAND = (byte) 9;

    byte INDEX_DEFINE_COMMAND = (byte) 10;
    byte INDEX_ADD_COMMAND = (byte) 11;
    byte INDEX_ADD_RELATIONSHIP_COMMAND = (byte) 12;
    byte INDEX_REMOVE_COMMAND = (byte) 13;
    byte INDEX_DELETE_COMMAND = (byte) 14;
    byte INDEX_CREATE_COMMAND = (byte) 15;

    byte UPDATE_RELATIONSHIP_COUNTS_COMMAND = (byte) 16;
    byte UPDATE_NODE_COUNTS_COMMAND = (byte) 17;
}
