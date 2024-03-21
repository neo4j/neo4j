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
package org.neo4j.server.httpv2.response.format;

/**
 * Some API specific field names. {@literal $type} seems to be widely used as an actual type denominator for non-standard
 * JSON types.
 * <p>
 * It will clash with the term "type" for relationship types. But in both cases the general term is the best. So it makes sense
 * to maybe separate everything that is "neo4j" specific (here, labels and types for nodes and relationships) as well as the
 * value of a non-standard json type with an underscore ({@literal _}).
 */
public final class Fieldnames {

    public static final String CYPHER_TYPE = "$type";
    public static final String CYPHER_VALUE = "_value";
    public static final String _LABELS = "_labels";
    public static final String LABELS = "labels";
    public static final String _ELEMENT_ID = "_element_id";
    public static final String ELEMENT_ID = "elementId";
    public static final String _START_NODE_ELEMENT_ID = "_start_node_element_id";
    public static final String START_NODE_ELEMENT_ID = "startNodeElementId";
    public static final String _END_NODE_ELEMENT_ID = "_end_node_element_id";
    public static final String END_NODE_ELEMENT_ID = "endNodeElementId";
    public static final String _RELATIONSHIP_TYPE = "_type";
    public static final String RELATIONSHIP_TYPE = "type";
    public static final String _PROPERTIES = "_properties";
    public static final String PROPERTIES = "properties";
    public static final String FIELDS_KEY = "fields";
    public static final String VALUES_KEY = "values";
    public static final String DATA_KEY = "data";
    public static final String BOOKMARKS_KEY = "bookmarks";

    public static final String NOTIFICATIONS_KEY = "notifications";
    public static final String QUERY_PLAN_KEY = "queryPlan";
    public static final String COUNTERS_KEY = "counters";

    public static final String PROFILE_KEY = "profiledQueryPlan";

    public static final String ERRORS_KEY = "errors";

    public static final String ERROR_KEY = "error";

    private Fieldnames() {}
}
