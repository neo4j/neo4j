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
package org.neo4j.server.queryapi.response.format;

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
    public static final String COUNTERS_KEY = "counters";
    public static final String QUERY_TYPE_KEY = "queryType";
    public static final String RESULT_AVAILABLE_AFTER_KEY = "resultAvailableAfter";
    public static final String RESULT_CONSUMED_AFTER_KEY = "resultConsumedAfter";

    public static final String QUERY_PLAN_KEY = "queryPlan";
    public static final String QUERY_PLAN_OPERATOR_TYPE_KEY = "operatorType";
    public static final String QUERY_PLAN_ARGUMENTS_KEY = "arguments";
    public static final String QUERY_PLAN_IDENTIFIERS_KEY = "identifiers";
    public static final String QUERY_PLAN_CHILDREN_KEY = "children";

    public static final String PROFILE_KEY = "profiledQueryPlan";
    public static final String PROFILE_DB_HITS_KEY = "dbHits";
    public static final String PROFILE_ROWS_KEY = "records";
    public static final String PROFILE_HAS_PAGE_CACHE_STATS_KEY = "hasPageCacheStats";
    public static final String PROFILE_PAGE_CACHE_HITS_KEY = "pageCacheHits";
    public static final String PROFILE_PAGE_CACHE_MISSES_KEY = "pageCacheMisses";
    public static final String PROFILE_PAGE_CACHE_RATION_KEY = "pageCacheHitRatio";
    public static final String PROFILE_TIME_KEY = "time";
    public static final String PROFILE_CHILDREN_KEY = "children";

    public static final String ERRORS_KEY = "errors";
    public static final String TRANSACTION_KEY = "transaction";
    public static final String TX_ID_KEY = "id";
    public static final String TX_EXPIRY_KEY = "expires";

    public static final String ERROR_KEY = "error";
    public static final String ERROR_MESSAGE = "message";
    public static final String ERROR_CODE = "code";

    public static final String CYPHER_EVENT = "$event";
    public static final String CYPHER_BODY = "_body";

    public static final String CYPHER_EVENT_HEADER = "Header";
    public static final String CYPHER_EVENT_RECORD = "Record";
    public static final String CYPHER_EVENT_SUMMARY = "Summary";
    public static final String CYPHER_EVENT_ERROR = "Error";

    private Fieldnames() {}
}
