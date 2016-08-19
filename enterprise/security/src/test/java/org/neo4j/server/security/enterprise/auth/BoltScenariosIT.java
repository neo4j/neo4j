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
package org.neo4j.server.security.enterprise.auth;

import org.junit.Rule;
import org.junit.rules.RuleChain;

import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.SuppressOutput;

public class BoltScenariosIT extends AuthScenariosLogic<BoltInteraction.BoltSubject>
{
    private Neo4jWithSocket server = new Neo4jWithSocket( getTestGraphDatabaseFactory(),
            settings -> {
                settings.put( GraphDatabaseSettings.auth_enabled, "true" );
            } );

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( SuppressOutput.suppressAll() ).around( server );

    public BoltScenariosIT()
    {
        super();
        IS_EMBEDDED = false;
        IS_BOLT = true;
    }

    protected TestGraphDatabaseFactory getTestGraphDatabaseFactory()
    {
        return new TestEnterpriseGraphDatabaseFactory();
    }

    @Override
    public NeoInteractionLevel<BoltInteraction.BoltSubject> setUpNeoServer() throws Throwable
    {
        return new BoltInteraction( server );
    }
}
