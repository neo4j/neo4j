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
package org.neo4j.graphdb;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@DbmsExtension(configurationCallback = "configure")
public final class LabelCoexistenceConstraintCommunityIT {
    @Inject
    private GraphDatabaseAPI graphDatabaseAPI;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseInternalSettings.relationship_endpoint_and_label_coexistence_constraints, true);
    }

    @Test
    void shouldNotAllowLabelCoexistenceConstraintsInCE() {

        try (Transaction tx =
                graphDatabaseAPI.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
            assertThatThrownBy(() -> {
                        try (InternalTransaction internalTransaction = graphDatabaseAPI.beginTransaction(
                                KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
                            internalTransaction
                                    .kernelTransaction()
                                    .schemaWrite()
                                    .labelCoexistenceConstraintCreate(
                                            SchemaDescriptors.forLabelCoexistence(0), "ConstraintName", 1);
                        }
                    })
                    .hasMessageContaining("requires Neo4j Enterprise Edition");
        }
    }
}
