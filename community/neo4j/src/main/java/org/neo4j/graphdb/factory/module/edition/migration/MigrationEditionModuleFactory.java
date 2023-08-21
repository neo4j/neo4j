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
package org.neo4j.graphdb.factory.module.edition.migration;

import java.util.function.Function;
import org.neo4j.annotations.service.Service;
import org.neo4j.commandline.dbms.MigrateStoreCommand;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.service.PrioritizedService;

/**
 * Used by {@link MigrateStoreCommand} in order to service load edition module depending on edition.
 * Should never be used for general service loading of edition module because implementations will
 * modify behaviour to fit migration and upgrade.
 */
@Service
public interface MigrationEditionModuleFactory
        extends Function<GlobalModule, AbstractEditionModule>, PrioritizedService {}
