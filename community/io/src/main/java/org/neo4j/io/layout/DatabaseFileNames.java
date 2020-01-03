/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.io.layout;

/**
 * List of file names for a database.
 */
final class DatabaseFileNames
{
    static final String METADATA_STORE = "neostore";

    static final String LABEL_SCAN_STORE = "neostore.labelscanstore.db";

    static final String COUNTS_STORE_A = "neostore.counts.db.a";
    static final String COUNTS_STORE_B = "neostore.counts.db.b";

    static final String NODE_STORE = "neostore.nodestore.db";
    static final String NODE_LABELS_STORE = "neostore.nodestore.db.labels";

    static final String RELATIONSHIP_STORE = "neostore.relationshipstore.db";
    static final String RELATIONSHIP_GROUP_STORE = "neostore.relationshipgroupstore.db";
    static final String RELATIONSHIP_TYPE_TOKEN_STORE = "neostore.relationshiptypestore.db";
    static final String RELATIONSHIP_TYPE_TOKEN_NAMES_STORE = "neostore.relationshiptypestore.db.names";

    static final String PROPERTY_STORE = "neostore.propertystore.db";
    static final String PROPERTY_ARRAY_STORE = "neostore.propertystore.db.arrays";
    static final String PROPERTY_STRING_STORE = "neostore.propertystore.db.strings";
    static final String PROPERTY_KEY_TOKEN_STORE = "neostore.propertystore.db.index";
    static final String PROPERTY_KEY_TOKEN_NAMES_STORE = "neostore.propertystore.db.index.keys";

    static final String LABEL_TOKEN_STORE = "neostore.labeltokenstore.db";
    static final String LABEL_TOKEN_NAMES_STORE = "neostore.labeltokenstore.db.names";

    static final String SCHEMA_STORE = "neostore.schemastore.db";
}
