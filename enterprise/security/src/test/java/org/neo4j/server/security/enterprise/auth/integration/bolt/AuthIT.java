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
package org.neo4j.server.security.enterprise.auth.integration.bolt;

import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.annotations.SaslMechanism;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.annotations.LoadSchema;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.directory.server.ldap.handlers.extended.StartTlsHandler;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.server.security.enterprise.auth.EnterpriseAuthAndUserManager;
import org.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

@SuppressWarnings( "deprecation" )
@RunWith( Parameterized.class )
@CreateDS(
        name = "Test",
        partitions = {@CreatePartition(
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
                @LoadSchema( name = "nis" ),
        } )
@CreateLdapServer(
        transports = {@CreateTransport( protocol = "LDAP", port = 10389, address = "0.0.0.0" ),
                @CreateTransport( protocol = "LDAPS", port = 10636, address = "0.0.0.0", ssl = true )
        },

        saslMechanisms = {
                @SaslMechanism( name = "DIGEST-MD5", implClass = org.apache.directory.server.ldap.handlers.sasl
                        .digestMD5.DigestMd5MechanismHandler.class ),
                @SaslMechanism( name = "CRAM-MD5", implClass = org.apache.directory.server.ldap.handlers.sasl
                        .cramMD5.CramMd5MechanismHandler.class )
        },
        saslHost = "0.0.0.0",
        extendedOpHandlers = {StartTlsHandler.class},
        keyStore = "target/test-classes/neo4j_ldap_test_keystore.jks",
        certificatePassword = "secret"
)
@ApplyLdifFiles( "ldap_test_data.ldif" )
public class AuthIT extends AuthTestBase
{
    private static EmbeddedTestCertificates embeddedTestCertificates;

    @ClassRule
    public static CreateLdapServerRule ldapServerRule = new CreateLdapServerRule();

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> configurations()
    {
        return Arrays.asList( new Object[][]{
                {"Ldap", "abc123", false,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://0.0.0.0:10389",
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"Ldaps", "abc123", true,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_server, "ldaps://localhost:10636",
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"StartTLS", "abc123", true,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://localhost:10389",
                                SecuritySettings.ldap_use_starttls, "true",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"LdapSystemAccount", "abc123", false,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://0.0.0.0:10389",
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "true",
                                SecuritySettings.ldap_authorization_system_password, "secret",
                                SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system"
                        )
                },
                {"Ldaps SystemAccount", "abc123", true,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_server, "ldaps://localhost:10636",
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "true",
                                SecuritySettings.ldap_authorization_system_password, "secret",
                                SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system"
                        )
                },
                {"StartTLS SystemAccount", "abc123", true,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://localhost:10389",
                                SecuritySettings.ldap_use_starttls, "true",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "true",
                                SecuritySettings.ldap_authorization_system_password, "secret",
                                SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system"
                        )
                },
                {"Ldap authn cache disabled", "abc123", false,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://0.0.0.0:10389",
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false",
                                SecuritySettings.ldap_authentication_cache_enabled, "false"
                        )
                },
                {"Ldap Digest MD5", "{MD5}6ZoYxCjLONXyYIU2eJIuAw==", false,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://0.0.0.0:10389",
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false",
                                SecuritySettings.ldap_authentication_mechanism, "DIGEST-MD5",
                                SecuritySettings.ldap_authentication_user_dn_template, "{0}"
                        )
                },
                {"Ldap Cram MD5", "{MD5}6ZoYxCjLONXyYIU2eJIuAw==", false,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.LDAP_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://0.0.0.0:10389",
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false",
                                SecuritySettings.ldap_authentication_mechanism, "CRAM-MD5",
                                SecuritySettings.ldap_authentication_user_dn_template, "{0}"
                        )
                },
                {"Ldap authn Native authz", "abc123", false,
                        Arrays.asList(
                                SecuritySettings.auth_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://0.0.0.0:10389",
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "true",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "false",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"Ldap authz Native authn", "abc123", false,
                        Arrays.asList(
                                SecuritySettings.auth_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://0.0.0.0:10389",
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "true",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "false",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "true",
                                SecuritySettings.ldap_authorization_system_password, "secret",
                                SecuritySettings.ldap_authorization_system_username, "uid=admin,ou=system"
                        )
                },
                {"Ldap with Native authn", "abc123", false,
                        Arrays.asList(
                                SecuritySettings.auth_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://0.0.0.0:10389",
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "true",
                                SecuritySettings.native_authorization_enabled, "false",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"Ldap with Native authz", "abc123", false,
                        Arrays.asList(
                                SecuritySettings.auth_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://0.0.0.0:10389",
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "false",
                                SecuritySettings.native_authorization_enabled, "true",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"Ldap and Native", "abc123", false,
                        Arrays.asList(
                                SecuritySettings.auth_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://0.0.0.0:10389",
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "true",
                                SecuritySettings.native_authorization_enabled, "true",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"Native with unresponsive ldap", "abc123", false,
                        Arrays.asList(
                                SecuritySettings.auth_providers, SecuritySettings.LDAP_REALM_NAME + ", " + SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.ldap_server, "ldap://127.0.0.1:10389",
                                SecuritySettings.ldap_use_starttls, "false",
                                SecuritySettings.native_authentication_enabled, "true",
                                SecuritySettings.native_authorization_enabled, "true",
                                SecuritySettings.ldap_authentication_enabled, "true",
                                SecuritySettings.ldap_authorization_enabled, "true",
                                SecuritySettings.ldap_authorization_use_system_account, "false"
                        )
                },
                {"Native", "abc123", false,
                        Arrays.asList(
                                SecuritySettings.auth_provider, SecuritySettings.NATIVE_REALM_NAME,
                                SecuritySettings.native_authentication_enabled, "true",
                                SecuritySettings.native_authorization_enabled, "true"
                        )
                }
        } );
    }

    private final String password;
    private final Map<Setting<?>,String> configMap;
    private final boolean confidentialityRequired;

    @SuppressWarnings( "unused" )
    public AuthIT( String suiteName, String password, boolean confidentialityRequired, List<Object> settings )
    {
        this.password = password;
        this.confidentialityRequired = confidentialityRequired;
        this.configMap = new HashMap<>();
        for ( int i = 0; i < settings.size() - 1; i += 2 )
        {
            Setting setting = (Setting) settings.get( i );
            String value = (String) settings.get( i + 1 );
            configMap.put( setting, value );
        }
    }

    @BeforeClass
    public static void classSetup()
    {
        embeddedTestCertificates = new EmbeddedTestCertificates();
    }

    @Before
    @Override
    public void setup() throws Exception
    {
        super.setup();
        ldapServerRule.getLdapServer().setConfidentialityRequired( confidentialityRequired );

        EnterpriseAuthAndUserManager authManager = dbRule.resolveDependency( EnterpriseAuthAndUserManager.class );
        EnterpriseUserManager userManager = authManager.getUserManager();
        if ( userManager != null )
        {
            userManager.newUser( NONE_USER, password.getBytes(), false );
            userManager.newUser( PROC_USER, password.getBytes(), false );
            userManager.newUser( READ_USER, password.getBytes(), false );
            userManager.newUser( WRITE_USER, password.getBytes(), false );
            userManager.addRoleToUser( PredefinedRoles.READER, READ_USER );
            userManager.addRoleToUser( PredefinedRoles.PUBLISHER, WRITE_USER );
            userManager.newRole( "role1", PROC_USER );
        }
    }

    @Override
    protected Map<Setting<?>, String> getSettings()
    {
        Map<Setting<?>, String> settings = new HashMap<>();
        settings.put( SecuritySettings.ldap_authentication_user_dn_template, "cn={0},ou=users,dc=example,dc=com" );
        settings.put( SecuritySettings.ldap_authentication_cache_enabled, "true" );
        settings.put( SecuritySettings.ldap_authorization_user_search_base, "dc=example,dc=com" );
        settings.put( SecuritySettings.ldap_authorization_user_search_filter, "(&(objectClass=*)(uid={0}))" );
        settings.put( SecuritySettings.ldap_authorization_group_membership_attribute_names, "gidnumber" );
        settings.put( SecuritySettings.ldap_authorization_group_to_role_mapping, "500=reader;501=publisher;502=architect;503=admin;505=role1" );
        settings.put( SecuritySettings.procedure_roles, "test.staticReadProcedure:role1" );
        settings.put( SecuritySettings.ldap_read_timeout, "1s" );
        settings.putAll( configMap );
        return settings;
    }

    @Override
    protected String getPassword()
    {
        return password;
    }

    @AfterClass
    public static void classTeardown()
    {
        if ( embeddedTestCertificates != null )
        {
            embeddedTestCertificates.close();
        }
    }
}
