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
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

@RunWith( FrameworkRunner.class )
@CreateDS(
        name = "Test",
        partitions =
        {
                @CreatePartition(
                    name = "example",
                    suffix = "dc=example,dc=com",
                    contextEntry = @ContextEntry( entryLdif =
                            "dn: dc=example,dc=com\n" +
                            "dc: example\n" +
                            "o: example\n" +
                            "objectClass: top\n" +
                            "objectClass: dcObject\n" +
                            "objectClass: organization\n\n" ) ),
        },
        loadedSchemas =
        {
                @LoadSchema( name = "nis" ),
        } )
@CreateLdapServer(
        transports =
        {
                @CreateTransport( protocol = "LDAP", port = 10389, address = "0.0.0.0" ),
                @CreateTransport( protocol = "LDAPS", port = 10636, address = "0.0.0.0", ssl = true )
        },

        saslMechanisms =
        {
                @SaslMechanism( name = "DIGEST-MD5", implClass =
                        org.apache.directory.server.ldap.handlers.sasl.digestMD5.DigestMd5MechanismHandler.class ),
                @SaslMechanism( name  = "CRAM-MD5", implClass =
                        org.apache.directory.server.ldap.handlers.sasl.cramMD5.CramMd5MechanismHandler.class )
        },
        saslHost = "0.0.0.0",
        extendedOpHandlers = { StartTlsHandler.class },
        keyStore = "target/test-classes/neo4j_ldap_test_keystore.jks",
        certificatePassword = "secret"
)
@ApplyLdifFiles( {"ad_schema.ldif", "ad_test_data.ldif"} )
public class ADAuthIT extends EnterpriseAuthenticationTestBase
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
        return super.getSettingsFunction().andThen( settings ->
        {
            settings.put( SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME );
            settings.put( SecuritySettings.native_authentication_enabled, "false" );
            settings.put( SecuritySettings.native_authorization_enabled, "false" );
            settings.put( SecuritySettings.ldap_authentication_enabled, "true" );
            settings.put( SecuritySettings.ldap_authorization_enabled, "true" );
            settings.put( SecuritySettings.ldap_server, "0.0.0.0:10389" );
            settings.put( SecuritySettings.ldap_authentication_user_dn_template, "cn={0},ou=local,ou=users,dc=example,dc=com" );
            settings.put( SecuritySettings.ldap_authentication_cache_enabled, "true" );
            settings.put( SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system" );
            settings.put( SecuritySettings.ldap_authorization_system_password, "secret" );
            settings.put( SecuritySettings.ldap_authorization_use_system_account, "true" );
            settings.put( SecuritySettings.ldap_authorization_user_search_base, "dc=example,dc=com" );
            settings.put( SecuritySettings.ldap_authorization_user_search_filter,
                    "(&(objectClass=*)(samaccountname={0}))" );
            settings.put( SecuritySettings.ldap_authorization_group_membership_attribute_names, "memberOf" );
            settings.put( SecuritySettings.ldap_authorization_group_to_role_mapping,
                        "cn=reader,ou=groups,dc=example,dc=com=reader;" +
                        "cn=publisher,ou=groups,dc=example,dc=com=publisher;" +
                        "cn=architect,ou=groups,dc=example,dc=com=architect;" +
                        "cn=admin,ou=groups,dc=example,dc=com=admin" );
            settings.put( SecuritySettings.procedure_roles, "test.allowedReadProcedure:role1" );
            settings.put( SecuritySettings.ldap_read_timeout, "1s" );
            settings.put( SecuritySettings.ldap_authentication_use_samaccountname, "true" );
        } );
    }

    @Test
    public void shouldLoginWithSamAccountName() throws Throwable
    {
        // dn: cn=n.neo4j,ou=local,ou=users,dc=example,dc=com
        assertAuth( "neo4j", "abc123" );
        reconnect();
        assertAuth( "neo4j", "abc123" );
        reconnect();
        // dn: cn=n.neo,ou=remote,ou=users,dc=example,dc=com
        assertAuth( "neo", "abc123" );
        reconnect();
        assertAuth( "neo", "abc123" );
    }

    @Test
    public void shouldFailLoginSamAccountNameWrongPassword() throws Throwable
    {
        assertAuthFail( "neo4j", "wrong" );
    }

    @Test
    public void shouldFailLoginSamAccountNameWithDN() throws Throwable
    {
        assertAuthFail( "n.neo4j", "abc123" );
    }

    @Test
    public void shouldReadWithSamAccountName() throws Throwable
    {
        assertAuth( "neo4j", "abc123" );
        assertReadSucceeds();
    }
}
