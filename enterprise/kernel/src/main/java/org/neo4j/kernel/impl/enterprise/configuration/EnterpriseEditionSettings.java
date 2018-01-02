/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.enterprise.configuration;


import java.util.List;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.kernel.IdType;

import static org.neo4j.kernel.IdType.NODE;
import static org.neo4j.kernel.IdType.RELATIONSHIP;
import static org.neo4j.kernel.configuration.Settings.EMPTY;
import static org.neo4j.kernel.configuration.Settings.list;
import static org.neo4j.kernel.configuration.Settings.optionsIgnoreCase;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Enterprise edition specific settings
 */
public class EnterpriseEditionSettings
{
    @Description( "Specified names of id types (comma separated) that should be reused. " +
                  "Currently only 'node' and 'relationship' types are supported. " )
    public static Setting<List<IdType>> idTypesToReuse = setting(
            "dbms.ids.reuse.types.override", list( ",", optionsIgnoreCase( NODE, RELATIONSHIP ) ), EMPTY );
}
