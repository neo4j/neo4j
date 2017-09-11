/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.logging.Level;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.BYTES;
import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.PATH;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.STRING_LIST;
import static org.neo4j.kernel.configuration.Settings.buildSetting;
import static org.neo4j.kernel.configuration.Settings.derivedSetting;
import static org.neo4j.kernel.configuration.Settings.min;
import static org.neo4j.kernel.configuration.Settings.options;
import static org.neo4j.kernel.configuration.Settings.range;
import static org.neo4j.kernel.configuration.Settings.setting;
import static org.neo4j.kernel.impl.proc.ProcedureConfig.PROC_ALLOWED_SETTING_DEFAULT_NAME;
import static org.neo4j.kernel.impl.proc.ProcedureConfig.PROC_ALLOWED_SETTING_ROLES;

/**
 * Settings for security module
 */
@Description( "Security configuration settings" )
public class SecuritySettings implements LoadableConfig
{
    public static final String NATIVE_REALM_NAME = "native";
    public static final String LDAP_REALM_NAME = "ldap";
    public static final String PLUGIN_REALM_NAME_PREFIX = "plugin-";

    //=========================================================================
    // Realm settings
    //=========================================================================

    @Description( "The authentication and authorization provider that contains both the users and roles. " +
                  "This can be one of the built-in `" + NATIVE_REALM_NAME + "` or `" + LDAP_REALM_NAME + "` providers, " +
                  "or it can be an externally provided plugin, with a custom name prefixed by `" +
                  PLUGIN_REALM_NAME_PREFIX + "`, i.e. `" + PLUGIN_REALM_NAME_PREFIX + "<AUTH_PROVIDER_NAME>`." )
    public static final Setting<String> auth_provider =
            setting( "dbms.security.auth_provider", STRING, NATIVE_REALM_NAME );

    @Description( "A list of security authentication and authorization providers containing the users and roles. " +
                  "They will be queried in the given order when login is attempted." )
    @Internal
    public static final Setting<List<String>> auth_providers =
            derivedSetting( "dbms.security.auth_providers", auth_provider, Arrays::asList, STRING_LIST );

    @Description( "Enable authentication via native authentication provider." )
    @Internal
    public static final Setting<Boolean> native_authentication_enabled =
            derivedSetting( "dbms.security.native.authentication_enabled", auth_providers,
                    providers -> providers.contains( NATIVE_REALM_NAME ), BOOLEAN );

    @Description( "Enable authorization via native authorization provider." )
    @Internal
    public static final Setting<Boolean> native_authorization_enabled =
            derivedSetting( "dbms.security.native.authorization_enabled", auth_providers,
                    providers -> providers.contains( NATIVE_REALM_NAME ), BOOLEAN );

    @Description( "Enable authentication via settings configurable LDAP authentication provider." )
    @Internal
    public static final Setting<Boolean> ldap_authentication_enabled =
            derivedSetting( "dbms.security.ldap.authentication_enabled", auth_providers,
                    providers -> providers.contains( LDAP_REALM_NAME ), BOOLEAN );

    @Description( "Enable authorization via settings configurable LDAP authorization provider." )
    @Internal
    public static final Setting<Boolean> ldap_authorization_enabled =
            derivedSetting( "dbms.security.ldap.authorization_enabled", auth_providers,
                    providers -> providers.contains( LDAP_REALM_NAME ), BOOLEAN );

    @Description( "Enable authentication via plugin authentication providers." )
    @Internal
    public static final Setting<Boolean> plugin_authentication_enabled =
            derivedSetting( "dbms.security.plugin.authentication_enabled", auth_providers,
                    providers -> providers.stream().anyMatch( r -> r.startsWith( PLUGIN_REALM_NAME_PREFIX ) ),
                    BOOLEAN );

    @Description( "Enable authorization via plugin authorization providers." )
    @Internal
    public static final Setting<Boolean> plugin_authorization_enabled =
            derivedSetting( "dbms.security.plugin.authorization_enabled", auth_providers,
                    providers -> providers.stream().anyMatch( r -> r.startsWith( PLUGIN_REALM_NAME_PREFIX ) ),
                    BOOLEAN );

    //=========================================================================
    // LDAP settings
    //=========================================================================

    @Description( "URL of LDAP server to use for authentication and authorization. " +
                  "The format of the setting is `<protocol>://<hostname>:<port>`, where hostname is the only " +
                  "required field. The supported values for protocol are `ldap` (default) and `ldaps`. " +
                  "The default port for `ldap` is 389 and for `ldaps` 636. For example: " +
                  "`ldaps://ldap.example.com:10389`.\n\n" +
                  "NOTE: You may want to consider using STARTTLS (`dbms.security.ldap.use_starttls`) instead of LDAPS " +
                  "for secure connections, in which case the correct protocol is `ldap`." )
    public static final Setting<String> ldap_server =
            setting( "dbms.security.ldap.host", STRING, "localhost" );

    @Description( "Use secure communication with the LDAP server using opportunistic TLS. " +
            "First an initial insecure connection will be made with the LDAP server, and a STARTTLS command will be " +
            "issued to negotiate an upgrade of the connection to TLS before initiating authentication." )
    public static final Setting<Boolean> ldap_use_starttls =
            setting( "dbms.security.ldap.use_starttls", BOOLEAN, FALSE );

    @Description(
            "The LDAP referral behavior when creating a connection. This is one of `follow`, `ignore` or `throw`.\n" +
            "* `follow` automatically follows any referrals\n" +
            "* `ignore` ignores any referrals\n" +
            "* `throw` throws an exception, which will lead to authentication failure" )
    public static final Setting<String> ldap_referral =
            setting( "dbms.security.ldap.referral", STRING, "follow" );

    @Description( "The timeout for establishing an LDAP connection. If a connection with the LDAP server cannot be " +
                  "established within the given time the attempt is aborted. " +
                  "A value of 0 means to use the network protocol's (i.e., TCP's) timeout value." )
    public static Setting<Duration> ldap_connection_timeout =
            setting( "dbms.security.ldap.connection_timeout", DURATION, "30s" );

    @Description( "The timeout for an LDAP read request (i.e. search). If the LDAP server does not respond within " +
                  "the given time the request will be aborted. A value of 0 means wait for a response indefinitely." )
    public static Setting<Duration> ldap_read_timeout =
            setting( "dbms.security.ldap.read_timeout", DURATION, "30s" );

    //-----------------------------------------------------
    // LDAP authentication settings
    //-----------------------------------------------------

    @Description( "LDAP authentication mechanism. This is one of `simple` or a SASL mechanism supported by JNDI, " +
                  "for example `DIGEST-MD5`. `simple` is basic username" +
                  " and password authentication and SASL is used for more advanced mechanisms. See RFC 2251 LDAPv3 " +
                  "documentation for more details." )
    public static final Setting<String> ldap_authentication_mechanism =
            setting( "dbms.security.ldap.authentication.mechanism", STRING, "simple" );

    @Description(
            "LDAP user DN template. An LDAP object is referenced by its distinguished name (DN), and a user DN is " +
            "an LDAP fully-qualified unique user identifier. This setting is used to generate an LDAP DN that " +
            "conforms with the LDAP directory's schema from the user principal that is submitted with the " +
            "authentication token when logging in. The special token {0} is a " +
            "placeholder where the user principal will be substituted into the DN string." )
    public static final Setting<String> ldap_authentication_user_dn_template =
            setting( "dbms.security.ldap.authentication.user_dn_template", STRING, "uid={0},ou=users,dc=example,dc=com" );

    @Description( "Determines if the result of authentication via the LDAP server should be cached or not. " +
                  "Caching is used to limit the number of LDAP requests that have to be made over the network " +
                  "for users that have already been authenticated successfully. A user can be authenticated against " +
                  "an existing cache entry (instead of via an LDAP server) as long as it is alive " +
                  "(see `dbms.security.auth_cache_ttl`).\n" +
                  "An important consequence of setting this to `true` is that " +
                  "Neo4j then needs to cache a hashed version of the credentials in order to perform credentials " +
                  "matching. This hashing is done using a cryptographic hash function together with a random salt. " +
                  "Preferably a conscious decision should be made if this method is considered acceptable by " +
                  "the security standards of the organization in which this Neo4j instance is deployed." )
    public static final Setting<Boolean> ldap_authentication_cache_enabled =
            setting( "dbms.security.ldap.authentication.cache_enabled", BOOLEAN, "true" );

    @Description( "Perform authentication with sAMAccountName instead of DN.\n" +
                  "Using this setting requires `dbms.security.ldap.authorization.system_username` and " +
                  "dbms.security.ldap.authorization.system_password to be used since there is no way to log in " +
                  "through ldap directly with the sAMAccountName, instead the login name will be resolved to a DN " +
                  "that will be used to log in with." )
    public static final Setting<Boolean> ldap_authentication_use_samaccountname =
            setting( "dbms.security.ldap.authentication.use_samaccountname", BOOLEAN, FALSE );

    //-----------------------------------------------------
    // LDAP authorization settings
    //-----------------------------------------------------

    @Description( "Perform LDAP search for authorization info using a system account instead of the user's own account.\n\n" +
                  "If this is set to `false` (default), the search for group membership will be performed " +
                  "directly after authentication using the LDAP context bound with the user's own account. " +
                  "The mapped roles will be cached for the duration of `dbms.security.auth_cache_ttl`, " +
                  "and then expire, requiring re-authentication. To avoid frequently having to re-authenticate " +
                  "sessions you may want to set a relatively long auth cache expiration time together with this " +
                  "option. NOTE: This option will only work if the users are permitted to search for their " +
                  "own group membership attributes in the directory.\n\n" +
                  "If this is set to `true`, the search will be performed using a special system account user " +
                  "with read access to all the users in the directory. " +
                  "You need to specify the username and password using the settings " +
                  "`dbms.security.ldap.authorization.system_username` and " +
                  "`dbms.security.ldap.authorization.system_password` with this option. " +
                  "Note that this account only needs read access to the relevant parts of the LDAP directory " +
                  "and does not need to have access rights to Neo4j, or any other systems." )
    public static final Setting<Boolean> ldap_authorization_use_system_account =
            setting( "dbms.security.ldap.authorization.use_system_account", BOOLEAN, FALSE );

    @Description(
            "An LDAP system account username to use for authorization searches when " +
            "`dbms.security.ldap.authorization.use_system_account` is `true`. " +
            "Note that the `dbms.security.ldap.authentication.user_dn_template` will not be applied to " +
            "this username, so you may have to specify a full DN." )
    public static final Setting<String> ldap_authorization_system_username =
            setting( "dbms.security.ldap.authorization.system_username", STRING, NO_DEFAULT );

    @Description(
            "An LDAP system account password to use for authorization searches when " +
            "`dbms.security.ldap.authorization.use_system_account` is `true`." )
    public static final Setting<String> ldap_authorization_system_password =
            setting( "dbms.security.ldap.authorization.system_password", STRING, NO_DEFAULT );

    @Description( "The name of the base object or named context to search for user objects when " +
                  "LDAP authorization is enabled. A common case is that this matches the last part " +
                  "of `dbms.security.ldap.authentication.user_dn_template`." )
    public static final Setting<String> ldap_authorization_user_search_base =
            setting( "dbms.security.ldap.authorization.user_search_base", STRING, "ou=users,dc=example,dc=com" );

    @Description( "The LDAP search filter to search for a user principal when LDAP authorization is " +
                  "enabled. The filter should contain the placeholder token {0} which will be substituted for the " +
                  "user principal." )
    public static final Setting<String> ldap_authorization_user_search_filter =
            setting( "dbms.security.ldap.authorization.user_search_filter", STRING, "(&(objectClass=*)(uid={0}))" );

    @Description( "A list of attribute names on a user object that contains groups to be used for mapping to roles " +
                  "when LDAP authorization is enabled." )
    public static final Setting<List<String>> ldap_authorization_group_membership_attribute_names =
            setting( "dbms.security.ldap.authorization.group_membership_attributes", STRING_LIST, "memberOf" );

    @Description( "An authorization mapping from LDAP group names to Neo4j role names. " +
                  "The map should be formatted as a semicolon separated list of key-value pairs, where the " +
                  "key is the LDAP group name and the value is a comma separated list of corresponding role names. " +
                  "For example: group1=role1;group2=role2;group3=role3,role4,role5\n\n" +
                  "You could also use whitespaces and quotes around group names to make this mapping more readable, " +
                  "for example: dbms.security.ldap.authorization.group_to_role_mapping=\\\n" +
                  "         \"cn=Neo4j Read Only,cn=users,dc=example,dc=com\"      = reader;    \\\n" +
                  "         \"cn=Neo4j Read-Write,cn=users,dc=example,dc=com\"     = publisher; \\\n" +
                  "         \"cn=Neo4j Schema Manager,cn=users,dc=example,dc=com\" = architect; \\\n" +
                  "         \"cn=Neo4j Administrator,cn=users,dc=example,dc=com\"  = admin" )
    public static final Setting<String> ldap_authorization_group_to_role_mapping =
            setting( "dbms.security.ldap.authorization.group_to_role_mapping", STRING, NO_DEFAULT );

    //=========================================================================
    // Cache settings
    //=========================================================================

    @Description( "The time to live (TTL) for cached authentication and authorization info when using " +
                  "external auth providers (LDAP or plugin). Setting the TTL to 0 will disable auth caching. " +
                  "Disabling caching while using the LDAP auth provider requires the use of an LDAP system account " +
                  "for resolving authorization information." )
    public static final Setting<Duration> auth_cache_ttl =
            setting( "dbms.security.auth_cache_ttl", DURATION, "10m" );

    @Description( "The maximum capacity for authentication and authorization caches (respectively)." )
    public static final Setting<Integer> auth_cache_max_capacity =
            setting( "dbms.security.auth_cache_max_capacity", INTEGER, "10000" );

    //=========================================================================
    // Security log settings
    //=========================================================================

    @Description( "Path to the security log file." )
    public static final Setting<File> security_log_filename = derivedSetting( "dbms.logs.security.path",
            GraphDatabaseSettings.logs_directory,
            logs -> new File( logs, "security.log" ),
            PATH );

    @Description( "Security log level threshold." )
    public static final Setting<Level> security_log_level = setting( "dbms.logs.security.level",
            options( Level.class ), "INFO" );

    @Description( "Set to log successful authentication events to the security log. " +
                  "If this is set to `false` only failed authentication events will be logged, which " +
                  "could be useful if you find that the successful events spam the logs too much, " +
                  "and you do not require full auditing capability." )
    public static final Setting<Boolean> security_log_successful_authentication =
            setting( "dbms.security.log_successful_authentication", BOOLEAN, "true" );

    @Description( "Threshold for rotation of the security log." )
    public static final Setting<Long> store_security_log_rotation_threshold =
            buildSetting( "dbms.logs.security.rotation.size", BYTES, "20m" ).constraint( range( 0L, Long.MAX_VALUE ) ).build();

    @Description( "Minimum time interval after last rotation of the security log before it may be rotated again." )
    public static final Setting<Duration> store_security_log_rotation_delay =
            setting( "dbms.logs.security.rotation.delay", DURATION, "300s" );

    @Description( "Maximum number of history files for the security log." )
    public static final Setting<Integer> store_security_log_max_archives =
            buildSetting( "dbms.logs.security.rotation.keep_number", INTEGER, "7" ).constraint( min(1) ).build();

    //=========================================================================
    // Procedure security settings
    //=========================================================================

    @Description( "The default role that can execute all procedures and user-defined functions that are not covered " +
                  "by the `" + PROC_ALLOWED_SETTING_ROLES + "` setting. If the `" + PROC_ALLOWED_SETTING_DEFAULT_NAME +
                  "` setting is the empty string (default), procedures will be executed according to the same security " +
                  "rules as normal Cypher statements." )
    public static final Setting<String> default_allowed = setting( PROC_ALLOWED_SETTING_DEFAULT_NAME, STRING, "" );

    @Description( "This provides a finer level of control over which roles can execute procedures than the " +
                  "`" + PROC_ALLOWED_SETTING_DEFAULT_NAME + "` setting. For example: `dbms.security.procedures.roles=" +
                  "apoc.convert.*:reader;apoc.load.json*:writer;apoc.trigger.add:TriggerHappy` will allow the role " +
                  "`reader` to execute all procedures in the `apoc.convert` namespace, the role `writer` to execute " +
                  "all procedures in the `apoc.load` namespace that starts with `json` and the role `TriggerHappy` " +
                  "to execute the specific procedure `apoc.trigger.add`. Procedures not matching any of these " +
                  "patterns will be subject to the `" + PROC_ALLOWED_SETTING_DEFAULT_NAME + "` setting." )
    public static final Setting<String> procedure_roles = setting( PROC_ALLOWED_SETTING_ROLES, STRING, "" );

    //=========================================================================
    // Misc settings
    //=========================================================================

    @Internal
    @Description( "Set to true if connection pooling should be used for authorization searches using the " +
                  "system account." )
    public static final Setting<Boolean> ldap_authorization_connection_pooling =
            setting( "unsupported.dbms.security.ldap.authorization.connection_pooling", BOOLEAN, "true" );
}
