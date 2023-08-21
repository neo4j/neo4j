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
package org.neo4j.dbms.database;

import java.util.Optional;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.kernel.database.AbstractDatabase;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public interface DatabaseContext {
    AbstractDatabase database();

    default Optional<Database> optionalDatabase() {
        if (database() instanceof Database db) {
            return Optional.of(db);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Returns a per-database {@link Dependencies} object.
     * These per-database dependencies sit in a tree underneath the parent, global dependencies.
     * If you `satisfy` an instance of a type on this object then you may only `resolve` it on this object.
     * However, if you `resolve` a type which is satisfied on the global dependencies but not here, that
     * will work fine. You will receive the global instance.
     *
     * @return dependencies service for this database
     */
    default DependencyResolver dependencies() {
        return database().getDependencyResolver();
    }

    GraphDatabaseAPI databaseFacade();
}
