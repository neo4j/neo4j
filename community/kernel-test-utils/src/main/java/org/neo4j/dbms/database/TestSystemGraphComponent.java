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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

public class TestSystemGraphComponent implements SystemGraphComponentWithVersion {
    final Name component;
    final int version;
    SystemGraphComponent.Status status;
    Exception onInit;
    Exception onMigrate;

    public TestSystemGraphComponent(
            Name component, SystemGraphComponent.Status status, Exception onInit, Exception onMigrate) {
        this(component, status, onInit, onMigrate, 0);
    }

    public TestSystemGraphComponent(
            Name component, SystemGraphComponent.Status status, Exception onInit, Exception onMigrate, int version) {
        this.component = component;
        this.status = status;
        this.onInit = onInit;
        this.onMigrate = onMigrate;
        this.version = version;
    }

    @Override
    public Name componentName() {
        return component;
    }

    @Override
    public int getLatestSupportedVersion() {
        return version;
    }

    @Override
    public Status detect(Transaction tx) {
        return status;
    }

    @Override
    public void initializeSystemGraph(GraphDatabaseService system, boolean firstInitialization) throws Exception {
        if (status == Status.UNINITIALIZED) {
            if (onInit == null) {
                status = Status.CURRENT;
            } else {
                throw onInit;
            }
        }
    }

    @Override
    public void upgradeToCurrent(GraphDatabaseService system) throws Exception {
        if (status == Status.REQUIRES_UPGRADE) {
            if (onMigrate == null) {
                status = Status.CURRENT;
            } else {
                throw onMigrate;
            }
        }
    }
}
