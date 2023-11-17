/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.server.rest.dbms;

import static java.util.Collections.singletonList;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

import java.io.IOException;
import jakarta.servlet.http.HttpServletResponse;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.web.AbstractFilter;

public abstract class AuthorizationFilter extends AbstractFilter {
    protected static ThrowingConsumer<HttpServletResponse, IOException> unauthorizedAccess(final String message) {
        return error(
                403,
                map(
                        "errors",
                        singletonList(map(
                                "code", Status.Security.Forbidden.code().serialize(),
                                "message", String.format("Unauthorized access violation: %s.", message)))));
    }
}
