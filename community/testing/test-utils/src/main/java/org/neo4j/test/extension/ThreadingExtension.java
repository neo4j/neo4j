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

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class ThreadingExtension extends StatefulFieldExtension<Threading>
        implements BeforeEachCallback, AfterEachCallback, AfterAllCallback {
    private static final String THREADING = "threading";
    private static final ExtensionContext.Namespace THREADING_NAMESPACE = ExtensionContext.Namespace.create(THREADING);

    @Override
    public void afterEach(ExtensionContext extensionContext) {
        Threading threading = getStoredValue(extensionContext);
        threading.after();
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) {
        Threading threading = getStoredValue(extensionContext);
        threading.before();
    }

    @Override
    protected String getFieldKey() {
        return THREADING;
    }

    @Override
    protected Class<Threading> getFieldType() {
        return Threading.class;
    }

    @Override
    protected Threading createField(ExtensionContext extensionContext) {
        return new Threading();
    }

    @Override
    protected ExtensionContext.Namespace getNameSpace() {
        return THREADING_NAMESPACE;
    }
}
