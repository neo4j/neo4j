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
package org.neo4j.kernel.api.impl.fulltext;

import org.neo4j.graphdb.schema.IndexSettingImpl;

/**
 * Responsible for naming of
 */
public final class FulltextIndexSettingsKeys
{
    public static final String FULLTEXT_PREFIX = "fulltext.";
    public static final String ANALYZER = IndexSettingImpl.FULLTEXT_ANALYZER.getSettingName();
    public static final String EVENTUALLY_CONSISTENT = IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT.getSettingName();

    // Config keys used as arguments by user in procedure call. No name space needed because implicit from procedure.
    public static final String PROCEDURE_ANALYZER = "analyzer";
    public static final String PROCEDURE_EVENTUALLY_CONSISTENT = "eventually_consistent";

    private FulltextIndexSettingsKeys()
    {}
}
