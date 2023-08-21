/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.recordstorage;

import org.neo4j.storageengine.api.enrichment.EnrichmentCommand;

public interface NeoCommandType {
    byte NODE_COMMAND = (byte) 1;
    byte PROP_COMMAND = (byte) 2;
    byte REL_COMMAND = (byte) 3;
    byte REL_TYPE_COMMAND = (byte) 4;
    byte PROP_INDEX_COMMAND = (byte) 5;
    // Command type of graph property commands. No longer used as of 4.0.
    byte NEOSTORE_COMMAND = (byte) 6;
    // Command type of previous (legacy) schema commands. No longer used as of 4.0. Use SCHEMA_RULE_COMMAND instead.
    byte LEGACY_SCHEMA_RULE_COMMAND = (byte) 7;

    byte LABEL_KEY_COMMAND = (byte) 8;
    byte REL_GROUP_COMMAND = (byte) 9;

    /*legacy index command types*/
    byte INDEX_DEFINE_COMMAND = (byte) 10;
    byte INDEX_ADD_COMMAND = (byte) 11;
    byte INDEX_ADD_RELATIONSHIP_COMMAND = (byte) 12;
    byte INDEX_REMOVE_COMMAND = (byte) 13;
    byte INDEX_DELETE_COMMAND = (byte) 14;
    byte INDEX_CREATE_COMMAND = (byte) 15;

    byte UPDATE_RELATIONSHIP_COUNTS_COMMAND = (byte) 16;
    byte UPDATE_NODE_COUNTS_COMMAND = (byte) 17;
    byte SCHEMA_RULE_COMMAND = (byte) 18;
    byte META_DATA_COMMAND = (byte) 19;
    byte UPDATE_GROUP_DEGREE_COMMAND = (byte) 20;

    byte REL_GROUP_EXTENDED_COMMAND = (byte) 21; // Relationship group with 3 byte type id

    byte CREATE_NODE_COMMAND = (byte) 22;
    byte DELETE_NODE_COMMAND = (byte) 23;

    byte CREATE_PROP_COMMAND = (byte) 24;
    byte DELETE_PROP_COMMAND = (byte) 25;

    byte CREATE_REL_COMMAND = (byte) 26;
    byte DELETE_REL_COMMAND = (byte) 27;

    byte ENRICHMENT_COMMAND = EnrichmentCommand.COMMAND_CODE;
}
