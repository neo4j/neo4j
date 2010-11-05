/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.rest;

import java.io.IOException;

import org.neo4j.server.NeoServer;
import org.neo4j.server.rest.domain.JsonHelper;

abstract class FunctionalTestUtil {
    static void assertLegalJson(String entity) throws IOException {
        JsonHelper.jsonToMap(entity);
    }

    static String baseUri() {
        return NeoServer.server().restApiUri().toString();
    }

    static String nodeUri() {
        return baseUri() + "node";
    }

    static String nodeUri(long id) {
        return nodeUri() + "/" + id;
    }

    static String nodePropertiesUri(long id) {
        return nodeUri(id) + "/properties";
    }

    static String nodePropertyUri(long id, String key) {
        return nodePropertiesUri(id) + "/" + key;
    }

    static String relationshipUri() {
        return baseUri() + "relationship";
    }

    static String relationshipUri(long id) {
        return relationshipUri() + "/" + id;
    }

    static String relationshipPropertiesUri(long id) {
        return relationshipUri(id) + "/properties";
    }

    static String relationshipPropertyUri(long id, String key) {
        return relationshipPropertiesUri(id) + "/" + key;
    }

    static String relationshipsUri(long nodeId, String dir, String... types) {
        StringBuilder typesString = new StringBuilder();
        for (String type : types) {
            typesString.append(typesString.length() > 0 ? "&" : "");
            typesString.append(type);
        }
        return nodeUri(nodeId) + "/relationships/" + dir + "/" + typesString;
    }

    static String indexUri() {
        return baseUri() + "index";
    }

    static String indexUri(String indexName) {
        return indexUri() + "/" + indexName;
    }

    static String indexUri(String indexName, String key, Object value) {
        return indexUri(indexName) + "/" + key + "/" + value;
    }
}
