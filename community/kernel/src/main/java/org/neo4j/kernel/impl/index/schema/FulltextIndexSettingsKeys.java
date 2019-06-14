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
package org.neo4j.kernel.impl.index.schema;

/**
 * Responsible for naming of
 */
public final class FulltextIndexSettingsKeys
{
    // Config keys used by index config. Belonging to 'fulltext' namespace to differentiate from other index config.
    private static final String FULLTEXT_CONFIG_PREFIX = "fulltext.";
    public static final String ANALYZER = FULLTEXT_CONFIG_PREFIX + "analyzer";
    public static final String EVENTUALLY_CONSISTENT = FULLTEXT_CONFIG_PREFIX + "eventually_consistent";

    // Config keys used as arguments by user in procedure call. No name space needed because implicit from procedure.
    public static final String PROCEDURE_ANALYZER = "analyzer";
    public static final String PROCEDURE_EVENTUALLY_CONSISTENT = "eventually_consistent";

    private FulltextIndexSettingsKeys()
    {}
}
