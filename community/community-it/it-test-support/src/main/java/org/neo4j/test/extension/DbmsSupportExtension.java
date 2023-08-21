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

import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class DbmsSupportExtension
        implements BeforeEachCallback, BeforeAllCallback, AfterEachCallback, AfterAllCallback {

    @Override
    public void beforeAll(ExtensionContext context) {
        if (getLifecycle(context) == PER_CLASS) {
            DbmsSupportController controller = new DbmsSupportController(context);
            controller.startDbms();
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        if (getLifecycle(context) == PER_METHOD) {
            DbmsSupportController controller = new DbmsSupportController(context);
            controller.startDbms();
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {
        if (getLifecycle(context) == PER_METHOD) {
            DbmsSupportController.remove(context).shutdown();
        }
    }

    @Override
    public void afterAll(ExtensionContext context) {
        if (getLifecycle(context) == PER_CLASS) {
            DbmsSupportController.remove(context).shutdown();
        }
    }

    private static TestInstance.Lifecycle getLifecycle(ExtensionContext context) {
        return context.getTestInstanceLifecycle().orElse(PER_METHOD);
    }
}
