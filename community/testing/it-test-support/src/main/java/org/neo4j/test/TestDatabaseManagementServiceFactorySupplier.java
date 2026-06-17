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
package org.neo4j.test;

import java.util.function.Function;
import org.neo4j.annotations.service.Service;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.time.SystemNanoClock;

@Service
public interface TestDatabaseManagementServiceFactorySupplier {
    String FACTORY_SUPPLIER = System.getProperty("NEO4J_OVERRIDE_DBMS_TEST_FACTORY_SUPPLIER");

    static boolean isSpd() {
        return "spd".equalsIgnoreCase(FACTORY_SUPPLIER);
    }

    DatabaseManagementServiceFactory create(
            DbmsInfo dbmsInfo,
            Function<GlobalModule, AbstractEditionModule> editionFactory,
            FileSystemAbstraction fileSystem,
            SystemNanoClock clock,
            InternalLogProvider internalLogProvider);

    default String name() {
        return "";
    }

    TestDatabaseManagementServiceFactorySupplier DEFAULT = TestDatabaseManagementServiceFactory::new;
}
