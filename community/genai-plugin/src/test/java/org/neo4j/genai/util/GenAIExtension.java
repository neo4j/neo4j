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
package org.neo4j.genai.util;

import org.neo4j.genai.vector.VectorEncoding;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

/**
 * Can be used with {@link TestDatabaseManagementServiceBuilder#addExtension(ExtensionFactory)}
 * to register the GenAI plugin procedures and functions.
 */
public class GenAIExtension extends ExtensionFactory<GenAIExtension.Dependencies> {
    interface Dependencies {
        GlobalProcedures procedures();
    }

    public GenAIExtension() {
        super("GenAI");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        return new LifecycleAdapter() {
            @Override
            public void start() throws Exception {
                final var procedures = dependencies.procedures();

                procedures.registerProcedure(VectorEncoding.class);
                procedures.registerFunction(VectorEncoding.class);
            }
        };
    }
}
