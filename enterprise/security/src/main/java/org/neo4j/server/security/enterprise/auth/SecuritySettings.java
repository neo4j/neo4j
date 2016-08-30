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

import java.util.List;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.helpers.HostnamePort;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.HOSTNAME_PORT;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.STRING_LIST;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Settings for security module
 */
@Description( "Security configuration settings" )
public class SecuritySettings
{
    @SuppressWarnings( "unused" ) // accessed by reflection

    @Description( "Enable authentication via internal authentication provider." )
    public static final Setting<Boolean> internal_authentication_enabled =
            setting( "dbms.security.realms.internal.authentication_enabled", BOOLEAN, "true" );

    @Description( "Enable authorization via internal authorization provider." )
    public static final Setting<Boolean> internal_authorization_enabled =
            setting( "dbms.security.realms.internal.authorization_enabled", BOOLEAN, "true" );

    @Description( "Enable authentication via settings configurable LDAP authentication realm." )
    public static final Setting<Boolean> ldap_authentication_enabled =
            setting( "dbms.security.realms.ldap.authentication_enabled", BOOLEAN, "false" );

    @Description( "Enable authotization via settings configurable LDAP authorization realm." )
    public static final Setting<Boolean> ldap_authorization_enabled =
            setting( "dbms.security.realms.ldap.authorization_enabled", BOOLEAN, "false" );

    @Description( "Enable authentication via plugin authentication realms." )
    public static final Setting<Boolean> plugin_authentication_enabled =
            setting( "dbms.security.realms.plugin.authentication_enabled", BOOLEAN, "false" );

    @Description( "Enable authotization via plugin authorization realms." )
    public static final Setting<Boolean> plugin_authorization_enabled =
            setting( "dbms.security.realms.plugin.authorization_enabled", BOOLEAN, "false" );

    @Description( "Hostname and port of LDAP server to use for authentication and authorization." )
    public static final Setting<HostnamePort> ldap_server =
            setting( "dbms.security.realms.ldap.host", HOSTNAME_PORT, "0.0.0.0:389" );

    @Description( "LDAP authentication mechanism. This is one of `simple` or a SASL mechanism supported by JNDI, " +
                  "e.g. `DIGEST-MD5`. `simple` is basic username" +
                  " and password authentication and SASL is used for more advanced mechanisms. See RFC 2251 LDAPv3 " +
                  "documentation for more details." )
    public static final Setting<String> ldap_auth_mechanism =
            setting( "dbms.security.realms.ldap.auth_mechanism", STRING, "simple" );

    @Description(
            "The LDAP referral behavior when creating a connection. This is one of `follow`, `ignore` or `throw`.\n" +
            "* `follow` automatically follows any referrals\n" +
            "* `ignore` ignores any referrals\n" +
            "* `throw` throws a `javax.naming.ReferralException` exception, which will lead to authentication failure\n" )
    public static final Setting<String> ldap_referral =
            setting( "dbms.security.realms.ldap.referral", STRING, "follow" );

    @Description(
            "LDAP user DN template. An LDAP object is referenced by its distinguished name (DN), and a user DN is " +
            "an LDAP fully-qualified unique user identifier. This setting is used to generate an LDAP DN that " +
            "conforms with the LDAP directory's schema from the user principal that is submitted with the " +
            "authentication token when logging in. The special token {0} is a " +
            "placeholder where the user principal will be substituted into the DN string." )
    public static final Setting<String> ldap_user_dn_template =
            setting( "dbms.security.realms.ldap.user_dn_template", STRING, "uid={0},ou=users,dc=example,dc=com" );

    @Description( "Perform LDAP search for authorization info using a system account." )
    public static final Setting<Boolean> ldap_authorization_use_system_account =
            setting( "dbms.security.realms.ldap.authorization.use_system_account", BOOLEAN, "false" );

    @Description(
            "An LDAP system account username to use for authorization searches when " +
            "`dbms.security.realms.ldap.authorization.use_system_account` is `true`." )
    public static final Setting<String> ldap_system_username =
            setting( "dbms.security.realms.ldap.system_username", STRING, NO_DEFAULT );

    @Description(
            "An LDAP system account password to use for authorization searches when " +
            "`dbms.security.realms.ldap.authorization.use_system_account` is `true`." )
    public static final Setting<String> ldap_system_password =
            setting( "dbms.security.realms.ldap.system_password", STRING, NO_DEFAULT );

    @Description( "The name of the base object or named context to search for user objects when LDAP authorization is " +
                  "enabled." )
    public static Setting<String> ldap_authorization_user_search_base =
            setting( "dbms.security.realms.ldap.authorization.user_search_base", STRING, NO_DEFAULT );

    @Description( "The LDAP search filter to search for a user principal when LDAP authorization is " +
                  "enabled. The filter should contain the placeholder token {0} which will be substituted for the " +
                  "user principal." )
    public static Setting<String> ldap_authorization_user_search_filter =
            setting( "dbms.security.realms.ldap.authorization.user_search_filter", STRING, "(&(objectClass=*)(uid={0}))" );

    @Description( "A list of attribute names on a user object that contains groups to be used for mapping to roles " +
                  "when LDAP authorization is enabled." )
    public static Setting<List<String>> ldap_authorization_group_membership_attribute_names =
            setting( "dbms.security.realms.ldap.authorization.group_membership_attributes", STRING_LIST, "memberOf" );

    @Description( "An authorization mapping from LDAP group names to internal role names. " +
                  "The map should be formatted as semicolon separated list of key-value pairs, where the " +
                  "key is the LDAP group name and the value is a comma separated list of corresponding role names. " +
                  "E.g. group1=role1;group2=role2;group3=role3,role4,role5" )
    public static Setting<String> ldap_authorization_group_to_role_mapping =
            setting( "dbms.security.realms.ldap.authorization.group_to_role_mapping", STRING, NO_DEFAULT );
}
