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
package org.neo4j.server.security.enterprise.auth.integration.bolt;

import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.annotations.SaslMechanism;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.annotations.LoadSchema;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.apache.directory.server.ldap.handlers.extended.StartTlsHandler;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.server.security.enterprise.auth.plugin.LdapGroupHasUsersAuthPlugin;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

@RunWith( FrameworkRunner.class )
@CreateDS(
        name = "Test",
        partitions = { @CreatePartition(
                name = "example",
                suffix = "dc=example,dc=com",
                contextEntry = @ContextEntry( entryLdif = "dn: dc=example,dc=com\n" +
                                                          "dc: example\n" +
                                                          "o: example\n" +
                                                          "objectClass: top\n" +
                                                          "objectClass: dcObject\n" +
                                                          "objectClass: organization\n\n" ) ),
        },
        loadedSchemas = {
                @LoadSchema( name = "nis", enabled = true ),
        } )
@CreateLdapServer(
        transports = { @CreateTransport( protocol = "LDAP", port = 10389, address = "0.0.0.0" ),
                @CreateTransport( protocol = "LDAPS", port = 10636, address = "0.0.0.0", ssl = true )
        },

        saslMechanisms = {
                @SaslMechanism( name = "DIGEST-MD5", implClass = org.apache.directory.server.ldap.handlers.sasl
                        .digestMD5.DigestMd5MechanismHandler.class ),
                @SaslMechanism( name  = "CRAM-MD5", implClass = org.apache.directory.server.ldap.handlers.sasl
                        .cramMD5.CramMd5MechanismHandler.class )
        },
        saslHost = "0.0.0.0",
        extendedOpHandlers = { StartTlsHandler.class },
        keyStore = "target/test-classes/neo4j_ldap_test_keystore.jks",
        certificatePassword = "secret"
)
@ApplyLdifFiles( "ldap_group_has_users_test_data.ldif" )
public class LdapExamplePluginAuthenticationIT extends EnterpriseAuthenticationTestBase
{
    @Before
    @Override
    public void setup()
    {
        super.setup();
        getLdapServer().setConfidentialityRequired( false );
    }

    @Override
    protected Consumer<Map<Setting<?>, String>> getSettingsFunction()
    {
        return super.getSettingsFunction().andThen( settings -> settings.put( SecuritySettings.auth_provider,
                SecuritySettings.PLUGIN_REALM_NAME_PREFIX + new LdapGroupHasUsersAuthPlugin().name() ) );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeWithLdapGroupHasUsersAuthPlugin() throws Throwable
    {
        testAuthWithReaderUser();
        reconnect();
        testAuthWithPublisherUser();
        reconnect();
        testAuthWithNoPermissionUser( "smith", "abc123" );
    }
}
