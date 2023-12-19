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

import org.apache.shiro.authz.SimpleRole;
import org.apache.shiro.authz.permission.RolePermissionResolver;
import org.apache.shiro.authz.permission.WildcardPermission;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;


public class PredefinedRolesBuilder implements RolesBuilder
{
    private static final WildcardPermission SCHEMA = new WildcardPermission( "schema:*" );
    private static final WildcardPermission FULL = new WildcardPermission( "*" );
    private static final WildcardPermission TOKEN = new WildcardPermission( "token:*" );
    private static final WildcardPermission READ_WRITE = new WildcardPermission( "data:*" );
    private static final WildcardPermission READ = new WildcardPermission( "data:read" );

    private static final Map<String,SimpleRole> innerRoles = staticBuildRoles();
    public static final Map<String,SimpleRole> roles = Collections.unmodifiableMap( innerRoles );

    private static Map<String,SimpleRole> staticBuildRoles()
    {
        Map<String,SimpleRole> roles = new ConcurrentHashMap<>( 4 );

        SimpleRole admin = new SimpleRole( ADMIN );
        admin.add( FULL );
        roles.put( ADMIN, admin );

        SimpleRole architect = new SimpleRole( ARCHITECT );
        architect.add( SCHEMA );
        architect.add( READ_WRITE );
        architect.add( TOKEN );
        roles.put( ARCHITECT, architect );

        SimpleRole publisher = new SimpleRole( PUBLISHER );
        publisher.add( READ_WRITE );
        publisher.add( TOKEN );
        roles.put( PUBLISHER, publisher );

        SimpleRole editor = new SimpleRole( EDITOR );
        editor.add( READ_WRITE );
        roles.put( EDITOR, editor );

        SimpleRole reader = new SimpleRole( READER );
        reader.add( READ );
        roles.put( READER, reader );

        return roles;
    }

    public static final RolePermissionResolver rolePermissionResolver = roleString ->
    {
        if ( roleString == null )
        {
            return Collections.emptyList();
        }
        SimpleRole role = roles.get( roleString );
        if ( role != null )
        {
            return role.getPermissions();
        }
        else
        {
            return Collections.emptyList();
        }
    };

    @Override
    public Map<String,SimpleRole> buildRoles()
    {
        return roles;
    }
}
