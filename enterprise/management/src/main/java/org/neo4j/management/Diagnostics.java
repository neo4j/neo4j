/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.management;

import java.util.List;

import org.neo4j.jmx.Description;
import org.neo4j.jmx.ManagementInterface;

@ManagementInterface( name = Diagnostics.NAME )
@Description( "Diagnostics provided by Neo4j" )
public interface Diagnostics
{
    String NAME = "Diagnostics";

    @Description( "Dump diagnostics information to the log." )
    void dumpToLog();

    @Description( "Dump diagnostics information for the diagnostics provider with the specified id." )
    void dumpToLog( String providerId );

    @Description( "Dump diagnostics information to JMX" )
    String dumpAll(  );

    @Description( "Extract diagnostics information for the diagnostics provider with the specified id." )
    String extract( String providerId );

    @Description( "A list of the ids for the registered diagnostics providers." )
    List<String> getDiagnosticsProviders();
}
