/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.kernel.configuration.Internal;
import org.neo4j.kernel.impl.store.id.IdType;

import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.list;
import static org.neo4j.kernel.configuration.Settings.optionsIgnoreCase;
import static org.neo4j.kernel.configuration.Settings.setting;
import static org.neo4j.kernel.impl.store.id.IdType.NODE;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP;

/**
 * Enterprise edition specific settings
 */
public class EnterpriseEditionSettings
{
    public static final String ENTERPRISE_SECURITY_MODULE_ID = "enterprise-security-module";

    @Description( "Specified names of id types (comma separated) that should be reused. " +
                  "Currently only 'node' and 'relationship' types are supported. " )
    public static Setting<List<IdType>> idTypesToReuse = setting(
            "dbms.ids.reuse.types.override", list( ",", optionsIgnoreCase( NODE, RELATIONSHIP ) ),
            String.join( ",", IdType.RELATIONSHIP.name(), IdType.NODE.name() ) );

    @Internal
    public static final Setting<String> security_module = setting( "unsupported.dbms.security.module", STRING,
            ENTERPRISE_SECURITY_MODULE_ID );
}
