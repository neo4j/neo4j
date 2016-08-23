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

import org.apache.shiro.authz.SimpleRole;
import org.apache.shiro.authz.permission.WildcardPermission;

import java.util.LinkedHashMap;
import java.util.Map;

public class PredefinedRolesBuilder implements RolesBuilder
{
    public static final String ADMIN = "admin";
    public static final String READ_WRITE_SCHEMA = "readWriteSchema";
    public static final String READ_WRITE = "readWrite";
    public static final String READ = "read";

    public static final Map<String,SimpleRole> roles = staticBuildRoles();

    public static Map<String,SimpleRole> staticBuildRoles()
    {
        Map<String, SimpleRole> roles = new LinkedHashMap<>( 4 );

        SimpleRole admin = new SimpleRole( ADMIN );
        admin.add( new WildcardPermission( "*" ) );
        roles.put( ADMIN, admin );

        SimpleRole readWriteSchema = new SimpleRole( READ_WRITE_SCHEMA );
        readWriteSchema.add( new WildcardPermission( "schema:*" ) );
        readWriteSchema.add( new WildcardPermission( "data:*" ) );
        roles.put( READ_WRITE_SCHEMA, readWriteSchema );

        SimpleRole readWrite = new SimpleRole( READ_WRITE );
        readWrite.add( new WildcardPermission( "data:*" ) );
        roles.put( READ_WRITE, readWrite );

        SimpleRole read = new SimpleRole( READ );
        read.add( new WildcardPermission( "data:read" ) );
        roles.put( READ, read );

        return roles;
    }

    @Override
    public Map<String,SimpleRole> buildRoles()
    {
        return staticBuildRoles();
    }

}
