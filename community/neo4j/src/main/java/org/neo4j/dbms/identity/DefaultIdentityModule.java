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
package org.neo4j.dbms.identity;

import java.util.UUID;
import org.neo4j.graphdb.factory.module.GlobalModule;

public class DefaultIdentityModule extends AbstractIdentityModule {
    private final ServerId serverId;

    public static ServerIdentityFactory fromGlobalModule() {
        return DefaultIdentityModule::new;
    }

    public DefaultIdentityModule(GlobalModule globalModule) {
        this(globalModule, UUID.randomUUID());
    }

    public DefaultIdentityModule(GlobalModule globalModule, UUID uuid) {
        var logService = globalModule.getLogService();
        var storage = createServerIdStorage(
                globalModule.getFileSystem(), globalModule.getNeo4jLayout().serverIdFile());
        this.serverId = readOrGenerate(
                storage, logService.getInternalLog(getClass()), ServerId.class, ServerId::new, () -> uuid);
        logService.getUserLog(getClass()).info("This instance is %s (%s)", serverId, serverId.uuid());
    }

    @Override
    public ServerId serverId() {
        return serverId;
    }
}
