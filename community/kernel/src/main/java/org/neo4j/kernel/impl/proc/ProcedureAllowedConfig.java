/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.proc;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.kernel.configuration.Config;

import static java.util.Arrays.stream;

public class ProcedureAllowedConfig
{
    public static final String PROC_ALLOWED_SETTING_DEFAULT_NAME = "dbms.security.procedures.default_allowed";
    public static final String PROC_ALLOWED_SETTING_ROLES = "dbms.security.procedures.roles";

    private static final String ROLES_DELIMITER = ",";
    private static final String SETTING_DELIMITER = ";";
    private static final String MAPPING_DELIMITER = ":";

    private final String defaultValue;
    private final List<ProcMatcher> matchers;

    private ProcedureAllowedConfig()
    {
        this.defaultValue = "";
        this.matchers = Collections.emptyList();
    }

    public ProcedureAllowedConfig( Config config )
    {
        Map<String,String> params = config.getParams();
        this.defaultValue = params.get( PROC_ALLOWED_SETTING_DEFAULT_NAME );
        if ( params.containsKey( PROC_ALLOWED_SETTING_ROLES ) )
        {
            this.matchers = Stream.of( params.get( PROC_ALLOWED_SETTING_ROLES ).split( SETTING_DELIMITER ) )
                    .map( procToRoleSpec ->
                    {
                        String[] spec = procToRoleSpec.split( MAPPING_DELIMITER );
                        String[] roles = stream( spec[1].split( ROLES_DELIMITER ) )
                                .map( String::trim ).toArray( String[]::new );
                        return new ProcMatcher( spec[0].trim(), roles );
                    } ).collect( Collectors.toList() );
        }
        else
        {
            this.matchers = Collections.emptyList();
        }
    }

    String[] rolesFor( String procedureName )
    {
        String[] wildCardRoles = matchers.stream().filter( matcher -> matcher.matches( procedureName ) )
                .map( ProcMatcher::roles ).reduce( new String[0],
                        ( acc, next ) -> Stream.concat( stream( acc ), stream( next ) ).toArray( String[]::new ) );
        if ( wildCardRoles.length > 0 )
        {
            return wildCardRoles;
        }
        else
        {
            return getDefaultValue();
        }
    }

    private String[] getDefaultValue()
    {
        return defaultValue == null || defaultValue.isEmpty() ? new String[0] : new String[]{defaultValue};
    }

    static final ProcedureAllowedConfig DEFAULT = new ProcedureAllowedConfig();

    private static class ProcMatcher
    {
        private final Pattern pattern;
        private final String[] roles;

        private ProcMatcher( String procedurePattern, String[] roles )
        {
            this.pattern = Pattern.compile( procedurePattern.replaceAll( "\\.", "\\\\." ).replaceAll( "\\*", ".*" ) );
            this.roles = roles;
        }

        boolean matches( String procedureName )
        {
            return pattern.matcher( procedureName ).matches();
        }

        String[] roles()
        {
            return roles;
        }
    }
}
