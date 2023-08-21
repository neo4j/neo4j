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
package org.neo4j.test.extension;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.util.function.UnaryOperator;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

/**
 * Implementations of this interface can be {@link Inject injected} into {@link DbmsExtension} based tests, to allow them to restart the DBMS or the database
 * the test is operating on.
 */
public interface DbmsController {
    /**
     * Restart the DBMS while applying the given changes to the builder.
     * @param databaseName name of the database used to re inject dependencies from after restart
     * @param callback The callback that will apply changes to the DBMS builder.
     */
    void restartDbms(String databaseName, UnaryOperator<TestDatabaseManagementServiceBuilder> callback);

    /**
     * Restart the DBMS without changing anything.
     * @param databaseName name of the database used to re inject dependencies from after restart
     */
    default void restartDbms(String databaseName) {
        restartDbms(databaseName, UnaryOperator.identity());
    }

    /**
     * Restart the database without changing anything.
     * @param databaseName name of the database to restart
     */
    void restartDatabase(String databaseName);

    default void restartDbms() {
        restartDbms(DEFAULT_DATABASE_NAME, UnaryOperator.identity());
    }

    default void restartDbms(UnaryOperator<TestDatabaseManagementServiceBuilder> callback) {
        restartDbms(DEFAULT_DATABASE_NAME, callback);
    }

    default void restartDatabase() {
        restartDbms(DEFAULT_DATABASE_NAME);
    }
}
