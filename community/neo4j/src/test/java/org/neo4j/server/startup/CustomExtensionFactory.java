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
package org.neo4j.server.startup;

import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;

class CustomExtensionFactory extends ExtensionFactory<CustomExtensionFactory.Dependencies> {

    public CustomExtensionFactory() {
        super(ExtensionType.DATABASE, "NAUGHTY");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {

        var lifecycle = new CustomExtension(dependencies.log().getUserLog(this.getClass()));
        dependencies
                .globalProceduresRegistry()
                .registerComponent((Class<CustomExtension>) lifecycle.getClass(), ctx -> lifecycle, true);
        return lifecycle;
    }

    public interface Dependencies {
        LogService log();

        GlobalProcedures globalProceduresRegistry();
    }
}
