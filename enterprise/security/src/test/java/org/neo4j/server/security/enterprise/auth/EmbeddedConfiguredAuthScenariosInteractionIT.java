/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.server.security.enterprise.auth;

import org.junit.Rule;

import java.util.Map;

import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

public class EmbeddedConfiguredAuthScenariosInteractionIT extends ConfiguredAuthScenariosInteractionTestBase<EnterpriseLoginContext>
{
    @Rule
    public EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Override
    protected NeoInteractionLevel<EnterpriseLoginContext> setUpNeoServer( Map<String, String> config ) throws Throwable
    {
        return new EmbeddedInteraction( config, () -> new UncloseableDelegatingFileSystemAbstraction( fileSystemRule.get() ) );
    }
}
