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
package org.neo4j.server.queryapi.driver.boltmessage.pipeline;

import java.util.Set;

public class BoltFields {

    static final String USER_AGENT_KEY = "user_agent";
    static final String PATCH_BOLT_KEY = "patch_bolt";
    static final String ROUTING_KEY = "routing";
    static final String BOLT_AGENT_KEY = "bolt_agent";
    static final String BOOKMARKS_KEY = "bookmarks";
    static final String SCHEME_KEY = "scheme";
    static final String CREDENTIALS_KEY = "credentials";
    static final String PRINCIPAL_KEY = "principal";
    static final String TX_TYPE_KEY = "tx_type";
    static final String NUMBER_OF_RECORDS_KEY = "n";
    static final String QUERY_ID_KEY = "qid";
    static final String TX_TIMEOUT_KEY = "tx_timeout";
    static final String ACCESS_MODE_KEY = "mode";
    static final String TX_METADATA_KEY = "tx_metadata";
    static final String IMPERSONATED_USER_KEY = "imp_user";
    static final String DATABASE_NAME_KEY = "db";
    static final String BEARER_VALUE = "bearer";
    static final String BASIC_VALUE = "basic";
    static final String READ_ACCESS_MODE_VALUE = "r";
    static final String EXPLICIT_TX_VALUE = "EXPLICIT";
    static final String IMPLICIT_TX_VALUE = "IMPLICIT";
    static final Set<String> AUTH_KEYS = Set.of(SCHEME_KEY, CREDENTIALS_KEY, PRINCIPAL_KEY);
}
