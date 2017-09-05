/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.server.security.enterprise.auth;

import java.util.Map;

import org.neo4j.helpers.ValueUtils;

public class BoltConfiguredAuthScenariosInteractionIT extends ConfiguredAuthScenariosInteractionTestBase<BoltInteraction.BoltSubject>
{
    public BoltConfiguredAuthScenariosInteractionIT()
    {
        super();
        IS_EMBEDDED = false;
        IS_BOLT = true;
    }

    @Override
    public NeoInteractionLevel<BoltInteraction.BoltSubject> setUpNeoServer( Map<String, String> config ) throws
            Throwable
    {
        return new BoltInteraction( config );
    }

    @Override
    protected Object valueOf( Object obj )
    {
        return ValueUtils.of( obj );
    }

}
