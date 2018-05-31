/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.harness.junit;

import org.eclipse.jetty.http.HttpStatus;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.harness.extensionpackage.MyEnterpriseUnmanagedExtension;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.configuration.ssl.LegacySslPolicyConfig;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.neo4j.server.ServerTestUtils.getRelativePath;
import static org.neo4j.server.ServerTestUtils.getSharedTestTemporaryFolder;

public class EnterpriseNeo4jRuleTest
{
    @Rule
    public Neo4jRule neo4j = new EnterpriseNeo4jRule()
            .withConfig( LegacySslPolicyConfig.certificates_directory.name(),
                    getRelativePath( getSharedTestTemporaryFolder(), LegacySslPolicyConfig.certificates_directory ) )
            .withExtension( "/test", MyEnterpriseUnmanagedExtension.class )
            .withConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );

    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void shouldExtensionWork()
    {
        // Given running enterprise server
        String doSomethingUri = neo4j.httpURI().resolve( "test/myExtension/doSomething" ).toString();

        // When I run this test

        // Then
        HTTP.Response response = HTTP.GET( doSomethingUri );
        assertThat( response.status(), equalTo( 234 ) );
    }

    @Test
    public void testPropertyExistenceConstraintCanBeCreated()
    {
        // Given running enterprise server
        String createConstraintUri = neo4j.httpURI().resolve( "test/myExtension/createConstraint" ).toString();

        // When I run this server

        // Then constraint should be created
        HTTP.Response response = HTTP.GET( createConstraintUri );
        assertThat( response.status(), equalTo( HttpStatus.CREATED_201 ) );
    }
}
