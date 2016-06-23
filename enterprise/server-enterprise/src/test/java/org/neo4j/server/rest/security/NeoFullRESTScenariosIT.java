/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.rest.security;

import org.neo4j.server.security.enterprise.auth.AuthScenariosLogic;
import org.neo4j.server.security.enterprise.auth.NeoInteractionLevel;

public class NeoFullRESTScenariosIT extends AuthScenariosLogic<RESTSubject>
{
    public NeoFullRESTScenariosIT()
    {
        super();
        CHANGE_PWD_ERR_MSG = "User is required to change their password.";
        PWD_CHANGE_CHECK_FIRST = true;
        HAS_ILLEGAL_ARGS_CHECK = true;
        IS_EMBEDDED = false;
    }

    @Override
    protected NeoInteractionLevel<RESTSubject> setUpNeoServer() throws Throwable
    {
        return new NeoFullRESTInteraction();
    }
}
