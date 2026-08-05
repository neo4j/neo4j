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
package org.neo4j.dbms.systemgraph;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

public interface SecurityGraphDbmsModel {
    Label USER_LABEL = Label.label("User");
    String USER = USER_LABEL.name();
    String USER_ID_PROPERTY = "id";
    String USER_NAME_PROPERTY = "name";
    String USER_CREDENTIALS_PROPERTY = "credentials";
    String USER_CREDENTIALS_EXPIRED_PROPERTY = "passwordChangeRequired";
    String USER_SUSPENDED_PROPERTY = "suspended";
    String USER_HOME_DB_PROPERTY = "homeDatabase";
    String USER_TAGS_PROPERTY = "tags";

    RelationshipType HAS_AUTH_TYPE = RelationshipType.withName("HAS_AUTH");
    String HAS_AUTH = HAS_AUTH_TYPE.name();

    String AUTH_CONSTRAINT = "auth-constraint";
    Label AUTH_LABEL = Label.label("Auth");
    String AUTH = AUTH_LABEL.name();
    String AUTH_PROVIDER_PROPERTY = "provider";
    String AUTH_ID_PROPERTY = "id";

    RelationshipType HAS_ROLE_TYPE = RelationshipType.withName("HAS_ROLE");
    Label ROLE_LABEL = Label.label("Role");
    String ROLE = ROLE_LABEL.name();
    String ROLE_NAME_PROPERTY = "name";

    Label AUTH_RULE_LABEL = Label.label("AuthRule");
    String AUTH_RULE = AUTH_RULE_LABEL.name();
    String AUTH_RULE_NAME_PROPERTY = "name";
    String AUTH_RULE_CONDITION_PROPERTY = "condition";
    String AUTH_RULE_ENABLED_PROPERTY = "enabled";
    String AUTH_RULE_CYPHER_VERSION_PROPERTY = "cypherVersion";
}
