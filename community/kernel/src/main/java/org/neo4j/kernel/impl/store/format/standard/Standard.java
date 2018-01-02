/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.store.format.standard;

import org.neo4j.kernel.impl.store.format.RecordFormats;
/**
 * This is a utility class always pointing to the latest Standard record format.
 */
public class Standard
{
    private Standard()
    {
    }

    public static final String LATEST_STORE_VERSION = StandardV3_4.STORE_VERSION;
    public static final RecordFormats LATEST_RECORD_FORMATS = StandardV3_4.RECORD_FORMATS;
    public static final String LATEST_NAME = StandardV3_4.NAME;
}
