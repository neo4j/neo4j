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
package org.neo4j.kernel.extension.context;

import java.nio.file.Path;

import org.neo4j.common.DependencySatisfier;
import org.neo4j.kernel.impl.factory.DbmsInfo;

abstract class BaseExtensionContext implements ExtensionContext
{
    private final Path contextDirectory;
    private final DbmsInfo dbmsInfo;
    private final DependencySatisfier satisfier;

    BaseExtensionContext( Path contextDirectory, DbmsInfo dbmsInfo, DependencySatisfier satisfier )
    {
        this.contextDirectory = contextDirectory;
        this.dbmsInfo = dbmsInfo;
        this.satisfier = satisfier;
    }

    @Override
    public DbmsInfo dbmsInfo()
    {
        return dbmsInfo;
    }

    @Override
    public DependencySatisfier dependencySatisfier()
    {
        return satisfier;
    }

    @Override
    public Path directory()
    {
        return contextDirectory;
    }
}
