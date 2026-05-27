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
package org.neo4j.queryapi.annotation.support;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.server.queryapi.tx.TransactionManager;

public record QueryAPIParameterResolver(
        DatabaseManagementService dbms, QueryAPITestClient testClient, TransactionManager txManager)
        implements ParameterResolver {
    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterContext.getParameter().getType() == DatabaseManagementService.class
                || parameterContext.getParameter().getType() == QueryAPITestClient.class
                || parameterContext.getParameter().getType() == TransactionManager.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        if (parameterContext.getParameter().getType() == DatabaseManagementService.class) {
            return dbms;
        } else if (parameterContext.getParameter().getType() == QueryAPITestClient.class) {
            return testClient;
        } else if (parameterContext.getParameter().getType() == TransactionManager.class) {
            return txManager;
        }
        throw new ParameterResolutionException("Parameter type not supported: "
                + parameterContext.getParameter().getType());
    }
}
