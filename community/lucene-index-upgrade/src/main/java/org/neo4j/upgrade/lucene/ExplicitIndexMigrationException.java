/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.upgrade.lucene;

/**
 * Exception that will be thrown in case if {@link LuceneExplicitIndexUpgrader} can migrate particular index.
 * Name of failed index can be found in {@link ExplicitIndexMigrationException#failedIndexName}
 */
public class ExplicitIndexMigrationException extends Exception
{
    private final String failedIndexName;

    public ExplicitIndexMigrationException( String failedIndexName, String message, Throwable cause )
    {
        super( message, cause );
        this.failedIndexName = failedIndexName;
    }

    public String getFailedIndexName()
    {
        return failedIndexName;
    }
}
