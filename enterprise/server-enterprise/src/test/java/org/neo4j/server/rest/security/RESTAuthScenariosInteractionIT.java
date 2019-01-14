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
package org.neo4j.server.rest.security;

import org.junit.Rule;

import java.util.Map;

import org.neo4j.server.security.enterprise.auth.AuthScenariosInteractionTestBase;
import org.neo4j.server.security.enterprise.auth.NeoInteractionLevel;
import org.neo4j.test.rule.SuppressOutput;

import static org.neo4j.test.rule.SuppressOutput.suppressAll;

public class RESTAuthScenariosInteractionIT extends AuthScenariosInteractionTestBase<RESTSubject>
{
    @Rule
    public SuppressOutput suppressOutput = suppressAll();

    public RESTAuthScenariosInteractionIT()
    {
        super();
        CHANGE_PWD_ERR_MSG = "User is required to change their password.";
        PWD_CHANGE_CHECK_FIRST = true;
        IS_EMBEDDED = false;
    }

    @Override
    protected NeoInteractionLevel<RESTSubject> setUpNeoServer( Map<String,String> config ) throws Throwable
    {
        return new RESTInteraction( config );
    }

    @Override
    protected Object valueOf( Object obj )
    {
        if ( obj instanceof Long )
        {
            return ((Long) obj).intValue();
        }
        else
        {
            return obj;
        }
    }
}
