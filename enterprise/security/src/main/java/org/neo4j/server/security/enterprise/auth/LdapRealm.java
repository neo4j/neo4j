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

import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.authz.SimpleRole;
import org.apache.shiro.authz.permission.RolePermissionResolver;
import org.apache.shiro.realm.ldap.JndiLdapContextFactory;
import org.apache.shiro.realm.ldap.JndiLdapRealm;
import org.apache.shiro.realm.ldap.LdapContextFactory;
import org.apache.shiro.subject.PrincipalCollection;

import java.util.Collection;
import java.util.Collections;
import javax.naming.NamingException;

import org.neo4j.kernel.configuration.Config;

/**
 * Shiro realm for LDAP based on configuration settings
 */
public class LdapRealm extends JndiLdapRealm
{
    public LdapRealm( Config config )
    {
        super();
        setRolePermissionResolver( rolePermissionResolver );
        configureRealm( config );
    }

    @Override
    protected AuthorizationInfo queryForAuthorizationInfo(PrincipalCollection principals,
            LdapContextFactory ldapContextFactory) throws NamingException
    {
        // TODO: This is just temporary
        return new SimpleAuthorizationInfo( Collections.singleton( PredefinedRolesBuilder.READER ) );
    }

    private final RolePermissionResolver rolePermissionResolver = new RolePermissionResolver()
    {
        @Override
        public Collection<Permission> resolvePermissionsInRole( String roleString )
        {
            SimpleRole role = PredefinedRolesBuilder.roles.get( roleString );
            if ( role != null )
            {
                return role.getPermissions();
            }
            else
            {
                return Collections.emptyList();
            }
        }
    };

    private void configureRealm( Config config )
    {
        JndiLdapContextFactory contextFactory = new JndiLdapContextFactory();
        contextFactory.setUrl( "ldap://" + config.get( SecuritySettings.ldap_server ) );
        contextFactory.setAuthenticationMechanism( config.get( SecuritySettings.ldap_auth_mechanism ) );
        contextFactory.setReferral( config.get( SecuritySettings.ldap_referral ) );
        contextFactory.setSystemUsername( config.get( SecuritySettings.ldap_system_username ) );
        contextFactory.setSystemPassword( config.get( SecuritySettings.ldap_system_password ) );

        setContextFactory( contextFactory );
        setUserDnTemplate( config.get( SecuritySettings.ldap_user_dn_template ) );
    }
}
