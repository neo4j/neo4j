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
package org.neo4j.server.security.enterprise.configuration;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Internal;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.BYTES;
import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.PATH;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.STRING_LIST;
import static org.neo4j.kernel.configuration.Settings.derivedSetting;
import static org.neo4j.kernel.configuration.Settings.max;
import static org.neo4j.kernel.configuration.Settings.min;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Settings for security module
 */
@Description( "Security configuration settings" )
public class SecuritySettings
{
    public static final String NATIVE_REALM_NAME = "native";
    public static final String LDAP_REALM_NAME = "ldap";
    public static final String PLUGIN_REALM_NAME_PREFIX = "plugin-";

    @SuppressWarnings( "unused" ) // accessed by reflection

    //=========================================================================
    // Realm settings
    //=========================================================================

    @Description( "The security authentication and authorization provider that contains both the users and roles. " +
                  "This can be one of the built-in `" + NATIVE_REALM_NAME + "` or `" + LDAP_REALM_NAME + "` providers, " +
                  "or it can be an externally provided plugin, with a custom name prefixed by `" +
                  PLUGIN_REALM_NAME_PREFIX + "`, i.e. `" + PLUGIN_REALM_NAME_PREFIX + "<example_provider_name>`." )
    public static Setting<String> active_realm =
            setting( "dbms.security.auth_provider", STRING, NATIVE_REALM_NAME );

    @Description( "A list of security authentication and authorization providers containing the users and roles. " +
                  "They will be queried in the given order when login is attempted." )
    @Internal
    public static Setting<List<String>> active_realms =
            derivedSetting( "dbms.security.auth_providers", active_realm,
                    ( r ) -> Arrays.asList( r ), STRING_LIST );

    @Description( "Enable authentication via native authentication provider." )
    @Internal
    public static final Setting<Boolean> native_authentication_enabled =
            derivedSetting( "dbms.security.native.authentication_enabled", active_realms,
                    ( providers ) -> providers.contains( NATIVE_REALM_NAME ), BOOLEAN );

    @Description( "Enable authorization via native authorization provider." )
    @Internal
    public static final Setting<Boolean> native_authorization_enabled =
            derivedSetting( "dbms.security.native.authorization_enabled", active_realms,
                    ( providers ) -> providers.contains( NATIVE_REALM_NAME ), BOOLEAN );

    @Description( "Enable authentication via settings configurable LDAP authentication provider." )
    @Internal
    public static final Setting<Boolean> ldap_authentication_enabled =
            derivedSetting( "dbms.security.ldap.authentication_enabled", active_realms,
                    ( providers ) -> providers.contains( LDAP_REALM_NAME ), BOOLEAN );

    @Description( "Enable authorization via settings configurable LDAP authorization provider." )
    @Internal
    public static final Setting<Boolean> ldap_authorization_enabled =
            derivedSetting( "dbms.security.ldap.authorization_enabled", active_realms,
                    ( providers ) -> providers.contains( LDAP_REALM_NAME ), BOOLEAN );

    @Description( "Enable authentication via plugin authentication providers." )
    @Internal
    public static final Setting<Boolean> plugin_authentication_enabled =
            derivedSetting( "dbms.security.plugin.authentication_enabled", active_realms,
                    ( providers ) -> providers.stream().anyMatch( ( r ) -> r.startsWith( PLUGIN_REALM_NAME_PREFIX ) ),
                    BOOLEAN );

    @Description( "Enable authorization via plugin authorization providers." )
    @Internal
    public static final Setting<Boolean> plugin_authorization_enabled =
            derivedSetting( "dbms.security.plugin.authorization_enabled", active_realms,
                    ( providers ) -> providers.stream().anyMatch( ( r ) -> r.startsWith( PLUGIN_REALM_NAME_PREFIX ) ),
                    BOOLEAN );

    //=========================================================================
    // LDAP settings
    //=========================================================================

    @Description( "URL of LDAP server (with protocol, hostname and port) to use for authentication and authorization. " +
                  "If no protocol is specified the default will be `ldap://`. To use LDAPS, " +
                  "set the protocol and port, e.g. `ldaps://ldap.example.com:636`" )
    public static final Setting<String> ldap_server =
            setting( "dbms.security.ldap.host", STRING, "0.0.0.0:389" );

    @Description( "Use secure communication with the LDAP server using opportunistic TLS. " +
            "First an initial insecure connection will be made with the LDAP server and a STARTTLS command will be " +
            "issued to negotiate an upgrade of the connection to TLS before initiating authentication." )
    public static final Setting<Boolean> ldap_use_starttls =
            setting( "dbms.security.ldap.use_starttls", BOOLEAN, "false" );

    @Description( "LDAP authentication mechanism. This is one of `simple` or a SASL mechanism supported by JNDI, " +
                  "e.g. `DIGEST-MD5`. `simple` is basic username" +
                  " and password authentication and SASL is used for more advanced mechanisms. See RFC 2251 LDAPv3 " +
                  "documentation for more details." )
    public static final Setting<String> ldap_auth_mechanism =
            setting( "dbms.security.ldap.auth_mechanism", STRING, "simple" );

    @Description(
            "The LDAP referral behavior when creating a connection. This is one of `follow`, `ignore` or `throw`.\n" +
            "* `follow` automatically follows any referrals\n" +
            "* `ignore` ignores any referrals\n" +
            "* `throw` throws a `javax.naming.ReferralException` exception, which will lead to authentication failure\n" )
    public static final Setting<String> ldap_referral =
            setting( "dbms.security.ldap.referral", STRING, "follow" );

    @Description(
            "LDAP user DN template. An LDAP object is referenced by its distinguished name (DN), and a user DN is " +
            "an LDAP fully-qualified unique user identifier. This setting is used to generate an LDAP DN that " +
            "conforms with the LDAP directory's schema from the user principal that is submitted with the " +
            "authentication token when logging in. The special token {0} is a " +
            "placeholder where the user principal will be substituted into the DN string." )
    public static final Setting<String> ldap_user_dn_template =
            setting( "dbms.security.ldap.user_dn_template", STRING, "uid={0},ou=users,dc=example,dc=com" );

    @Description( "Determines if the result of authentication via the LDAP server should be cached or not. " +
                  "Caching is used to limit the number of LDAP requests that have to be made over the network " +
                  "for users that have already been authenticated successfully. A user can be authenticated against " +
                  "an existing cache entry (instead of via an LDAP server) as long as it is alive " +
                  "(see `dbms.security.auth_cache_ttl`).\n" +
                  "An important consequence of setting this to `true` than needs to be well understood, is that " +
                  "Neo4j then needs to cache a hashed version of the credentials in order to perform credentials " +
                  "matching. This hashing is done using a cryptographic hash function together with a random salt. " +
                  "Preferably a conscious decision should be made if this method is considered acceptable by " +
                  "the security standards of the organization in which this Neo4j instance is deployed." )
    public static final Setting<Boolean> ldap_authentication_cache_enabled =
            setting( "dbms.security.ldap.authentication_cache_enabled", BOOLEAN, "true" );

    @Description( "Perform LDAP search for authorization info using a system account." )
    public static final Setting<Boolean> ldap_authorization_use_system_account =
            setting( "dbms.security.ldap.authorization.use_system_account", BOOLEAN, "false" );

    @Description(
            "An LDAP system account username to use for authorization searches when " +
            "`dbms.security.ldap.authorization.use_system_account` is `true`. " +
            "Note that the `dbms.security.ldap.user_dn_template` will not be applied to this username, " +
            "so you may have to specify a full DN." )
    public static final Setting<String> ldap_system_username =
            setting( "dbms.security.ldap.system_username", STRING, NO_DEFAULT );

    @Description(
            "An LDAP system account password to use for authorization searches when " +
            "`dbms.security.ldap.authorization.use_system_account` is `true`." )
    public static final Setting<String> ldap_system_password =
            setting( "dbms.security.ldap.system_password", STRING, NO_DEFAULT );

    @Description( "The name of the base object or named context to search for user objects when LDAP authorization is " +
                  "enabled." )
    public static Setting<String> ldap_authorization_user_search_base =
            setting( "dbms.security.ldap.authorization.user_search_base", STRING, NO_DEFAULT );

    @Description( "The LDAP search filter to search for a user principal when LDAP authorization is " +
                  "enabled. The filter should contain the placeholder token {0} which will be substituted for the " +
                  "user principal." )
    public static Setting<String> ldap_authorization_user_search_filter =
            setting( "dbms.security.ldap.authorization.user_search_filter", STRING, "(&(objectClass=*)(uid={0}))" );

    @Description( "A list of attribute names on a user object that contains groups to be used for mapping to roles " +
                  "when LDAP authorization is enabled." )
    public static Setting<List<String>> ldap_authorization_group_membership_attribute_names =
            setting( "dbms.security.ldap.authorization.group_membership_attributes", STRING_LIST, "memberOf" );

    @Description( "An authorization mapping from LDAP group names to internal role names. " +
                  "The map should be formatted as semicolon separated list of key-value pairs, where the " +
                  "key is the LDAP group name and the value is a comma separated list of corresponding role names. " +
                  "E.g. group1=role1;group2=role2;group3=role3,role4,role5" )
    public static Setting<String> ldap_authorization_group_to_role_mapping =
            setting( "dbms.security.ldap.authorization.group_to_role_mapping", STRING, NO_DEFAULT );

    //=========================================================================
    // Cache settings
    //=========================================================================

    @Description( "The time to live (TTL) for cached authentication and authorization info. Setting the TTL to 0 will" +
            " disable auth caching." )
    public static Setting<Long> auth_cache_ttl =
            setting( "dbms.security.auth_cache_ttl", DURATION, "10m" );

    @Description( "The maximum capacity for authentication and authorization caches (respectively)." )
    public static Setting<Integer> auth_cache_max_capacity =
            setting( "dbms.security.auth_cache_max_capacity", INTEGER, "10000" );

    //=========================================================================
    // Security log settings
    //=========================================================================

    @Internal
    public static final Setting<File> security_log_filename = derivedSetting("dbms.security.log_path",
            GraphDatabaseSettings.logs_directory,
            ( logs ) -> new File( logs, "security.log" ),
            PATH );

    @Description( "Set to log successful authentication events." )
    public static final Setting<Boolean> security_log_successful_authentication =
            setting("dbms.security.log_successful_authentication", BOOLEAN, "true" );

    @Description( "Threshold for rotation of the security log." )
    public static final Setting<Long> store_security_log_rotation_threshold =
            setting("dbms.logs.security.rotation.size", BYTES, "20m", min(0L), max( Long.MAX_VALUE ) );

    @Description( "Minimum time interval after last rotation of the security log before it may be rotated again." )
    public static final Setting<Long> store_security_log_rotation_delay =
            setting("dbms.logs.security.rotation.delay", DURATION, "300s" );

    @Description( "Maximum number of history files for the security log." )
    public static final Setting<Integer> store_security_log_max_archives =
            setting("dbms.logs.security.rotation.keep_number", INTEGER, "7", min(1) );
}
