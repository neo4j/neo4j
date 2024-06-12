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

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

import java.util.Arrays;
import java.util.Optional;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.util.Preconditions;

/**
 * Common code for all system graph components, apart from test implementations and the central collection class {@link SystemGraphComponents}.
 */
public abstract class AbstractSystemGraphComponent implements SystemGraphComponent {
    protected final Config config;

    public AbstractSystemGraphComponent(Config config) {
        this.config = config;
    }

    protected void initializeSystemGraphConstraints(Transaction tx) {}

    protected void initializeSystemGraphModel(Transaction tx, GraphDatabaseService systemDb) throws Exception {}

    protected void verifySystemGraph(GraphDatabaseService system) throws Exception {}

    private void initializeSystemGraphConstraints(GraphDatabaseService system) {
        try (Transaction tx = system.beginTx()) {
            initializeSystemGraphConstraints(tx);
            tx.commit();
        }
    }

    protected void initializeSystemGraphModel(GraphDatabaseService system) throws Exception {
        try (Transaction tx = system.beginTx()) {
            initializeSystemGraphModel(tx, system);
            tx.commit();
        }
    }

    protected void postInitialization(GraphDatabaseService system, boolean wasInitialized) throws Exception {}

    @Override
    public void initializeSystemGraph(GraphDatabaseService system, boolean firstInitialization) throws Exception {
        boolean mayUpgrade = config.get(GraphDatabaseInternalSettings.automatic_upgrade_enabled);

        Preconditions.checkState(
                system.databaseName().equals(SYSTEM_DATABASE_NAME),
                "Cannot initialize system graph on database '" + system.databaseName() + "'");

        Status status = detect(system);
        if (status == Status.UNINITIALIZED) {
            initializeSystemGraphConstraints(system);
            initializeSystemGraphModel(system);
            postInitialization(system, true);
        } else if (status == Status.CURRENT || (status == Status.REQUIRES_UPGRADE && !mayUpgrade)) {
            verifySystemGraph(system);
            postInitialization(system, false);
        } else if ((mayUpgrade && status == Status.REQUIRES_UPGRADE) || status == Status.UNSUPPORTED_BUT_CAN_UPGRADE) {
            upgradeToCurrent(system);
        } else {
            throw new IllegalStateException(
                    String.format("Unsupported component state for '%s': %s", componentName(), status.description()));
        }
    }

    protected static void initializeSystemGraphConstraint(Transaction tx, Label label, String... properties) {
        // Makes the creation of constraints for security idempotent
        if (!hasUniqueConstraint(tx, label, properties)) {
            checkForClashingIndexes(tx, label, properties);
            ConstraintCreator cb = tx.schema().constraintFor(label);
            for (String prop : properties) {
                cb = cb.assertPropertyIsUnique(prop);
            }
            cb.create();
        }
    }

    protected static void initializeSystemGraphConstraint(
            Transaction tx, String name, Label label, String... properties) {
        if (!hasUniqueConstraint(tx, label, properties)) {
            checkForClashingIndexes(tx, label, properties);
            ConstraintCreator cb = tx.schema().constraintFor(label);
            for (String prop : properties) {
                cb = cb.assertPropertyIsUnique(prop);
            }
            cb.withName(name).create();
        }
    }

    protected static boolean hasUniqueConstraint(Transaction tx, Label label, String... properties) {
        return findUniqueConstraint(tx, label, properties).isPresent();
    }

    protected static Optional<ConstraintDefinition> findUniqueConstraint(
            Transaction tx, Label label, String... properties) {
        for (ConstraintDefinition constraintDefinition : tx.schema().getConstraints(label)) {
            if (constraintDefinition.getPropertyKeys().equals(Arrays.asList(properties))
                    && constraintDefinition.isConstraintType(ConstraintType.UNIQUENESS))
                return Optional.of(constraintDefinition);
        }
        return Optional.empty();
    }

    private static void checkForClashingIndexes(Transaction tx, Label label, String... properties) {
        tx.schema().getIndexes(label).forEach(index -> {
            String[] propertyKeys = Iterables.asArray(String.class, index.getPropertyKeys());
            if (Arrays.equals(propertyKeys, properties)) {
                index.drop();
            }
        });
    }
}
