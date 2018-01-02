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
package org.neo4j.management;

import java.util.List;

import org.neo4j.jmx.Description;
import org.neo4j.jmx.ManagementInterface;

@ManagementInterface( name = Diagnostics.NAME )
@Description( "Diagnostics provided by Neo4j" )
public interface Diagnostics
{
    final String NAME = "Diagnostics";

    @Description( "Dump diagnostics information to the log." )
    void dumpToLog();

    @Description( "Dump diagnostics information for the diagnostics provider with the specified id." )
    void dumpToLog( String providerId );

    @Description("Dump diagnostics information to JMX")
    String dumpAll(  );

    @Description( "Extract diagnostics information for the diagnostics provider with the specified id." )
    String extract( String providerId );

    @Description( "A list of the ids for the registered diagnostics providers." )
    List<String> getDiagnosticsProviders();
}
