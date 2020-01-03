/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.spi;

import java.io.File;

import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.util.DependencySatisfier;

public interface KernelContext
{
    /**
     * @return store directory for {@link ExtensionType#GLOBAL} extensions and
     * particular database directory if extension is per {@link ExtensionType#DATABASE}.
     * @deprecated Please use {@link #directory()} instead.
     */
    @Deprecated
    File storeDir();

    DatabaseInfo databaseInfo();

    DependencySatisfier dependencySatisfier();

    /**
     * @return store directory for {@link ExtensionType#GLOBAL} extensions and
     * particular database directory if extension is per {@link ExtensionType#DATABASE}.
     */
    default File directory()
    {
        return storeDir();
    }
}
