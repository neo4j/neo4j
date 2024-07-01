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
package org.neo4j.kernel.impl.security;

/**
 * Controls authorization and authentication for an individual user.
 * NOTE: User ids were introduced in Neo4j 4.3-drop04.
 * The id is added when the user is added to the system database or on a system database upgrade from a previous version.
 * Thus, this method returning null indicates that
 * 1. the user was created before Neo4j 4.3-drop04 and has not been properly upgraded
 * or
 * 2. the user was created through a neo4j-admin command and has not yet been added to the system database.
 */
public record User(String name, String id, Credential credential, boolean passwordChangeRequired, boolean suspended) {
    public static final String PASSWORD_CHANGE_REQUIRED = "password_change_required";
}
