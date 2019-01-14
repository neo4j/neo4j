/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.configuration.ssl;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.config.SettingGroup;
import org.neo4j.kernel.configuration.Group;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Optional.empty;

public class SslPolicyConfigValidator implements SettingGroup<Object>
{
    @Override
    public Map<String,String> validate( Map<String,String> params, Consumer<String> warningConsumer ) throws InvalidSettingException
    {
        Map<String,String> validatedParams = new HashMap<>();

        Set<String> validShortKeys = extractValidShortKeys();
        String groupSettingPrefix = groupPrefix();

        Pattern groupSettingPattern = Pattern.compile(
                Pattern.quote( groupSettingPrefix ) + "\\.([^.]+)\\.?(.+)?" );

        Set<String> policyNames = new HashSet<>();

        for ( Map.Entry<String,String> paramsEntry : params.entrySet() )
        {
            String settingName = paramsEntry.getKey();
            Matcher matcher = groupSettingPattern.matcher( settingName );
            if ( !matcher.matches() )
            {
                continue;
            }

            policyNames.add( matcher.group( 1 ) );
            String shortKey = matcher.group( 2 );

            if ( !validShortKeys.contains( shortKey ) )
            {
                throw new InvalidSettingException( "Invalid setting name: " + settingName );
            }

            validatedParams.put( settingName, paramsEntry.getValue() );
        }

        for ( String policyName : policyNames )
        {
            SslPolicyConfig policy = new SslPolicyConfig( policyName );

            if ( !params.containsKey( policy.base_directory.name() ) )
            {
                throw new InvalidSettingException( "Missing mandatory setting: " + policy.base_directory.name() );
            }
        }

        return validatedParams;
    }

    private String groupPrefix()
    {
        return SslPolicyConfig.class.getDeclaredAnnotation( Group.class ).value();
    }

    private Set<String> extractValidShortKeys()
    {
        Set<String> validSettingNames = new HashSet<>();

        String policyName = "example";
        int prefixLength = groupPrefix().length() + 1 + policyName.length() + 1; // dbms.ssl.policy.example.

        SslPolicyConfig examplePolicy = new SslPolicyConfig( policyName );
        Field[] fields = examplePolicy.getClass().getDeclaredFields();
        for ( Field field : fields )
        {
            if ( Modifier.isStatic( field.getModifiers() ) )
            {
                continue;
            }

            try
            {
                Object obj = field.get( examplePolicy );
                if ( obj instanceof Setting )
                {
                    String longKey = ((Setting) obj).name();
                    String shortKey = longKey.substring( prefixLength );
                    validSettingNames.add( shortKey );
                }
            }
            catch ( IllegalAccessException e )
            {
                throw new RuntimeException( e );
            }
        }
        return validSettingNames;
    }

    @Override
    public Map<String,Object> values( Map<String,String> validConfig )
    {
        return emptyMap();
    }

    @Override
    public List<Setting<Object>> settings( Map<String,String> params )
    {
        return emptyList();
    }

    @Override
    public boolean deprecated()
    {
        return false;
    }

    @Override
    public Optional<String> replacement()
    {
        return empty();
    }

    @Override
    public boolean internal()
    {
        return false;
    }

    @Override
    public boolean secret()
    {
        return false;
    }

    @Override
    public Optional<String> documentedDefaultValue()
    {
        return empty();
    }

    @Override
    public String valueDescription()
    {
        return "SSL policy configuration";
    }

    @Override
    public Optional<String> description()
    {
        return empty();
    }

    @Override
    public boolean dynamic()
    {
        return false;
    }
}
