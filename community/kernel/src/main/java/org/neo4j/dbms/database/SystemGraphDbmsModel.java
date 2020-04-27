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
package org.neo4j.dbms.database;

import org.neo4j.graphdb.Label;

public class SystemGraphDbmsModel
{
    public static final Label DATABASE_LABEL = Label.label( "Database" );
    public static final Label DELETED_DATABASE_LABEL = Label.label( "DeletedDatabase" );
    public static final String DATABASE_UUID_PROPERTY = "uuid";
    public static final String DATABASE_NAME_PROPERTY = "name";
    public static final String DATABASE_STATUS_PROPERTY = "status";
    public static final String DATABASE_DEFAULT_PROPERTY = "default";
    public static final String DUMP_DATA_PROPERTY = "dump_data";
    public static final String UPDATED_AT_PROPERTY = "updated_at";
}
