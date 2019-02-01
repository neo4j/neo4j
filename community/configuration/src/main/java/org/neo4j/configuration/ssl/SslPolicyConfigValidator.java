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
package org.neo4j.configuration.ssl;

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

import org.neo4j.configuration.Group;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.config.SettingGroup;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Optional.empty;

public class SslPolicyConfigValidator implements SettingGroup<Object>
{
    @Override
    public Map<String,String> validate( Map<String,String> params, Consumer<String> warningConsumer ) throws InvalidSettingException
    {
           return _validate( params, warningConsumer );
    }

    private Map<String,String> _validate( Map<String,String> params, Consumer<String> warningConsumer ) throws InvalidSettingException
    {
        Map<String,String> validatedParams = new HashMap<>();

        String groupSettingPrefix = groupPrefix();

        Pattern groupSettingPattern = Pattern.compile(
                Pattern.quote( groupSettingPrefix ) + "\\.([^.]+)\\.?(.+)?" );

        Map<String,Set<String>> policyNames = new HashMap<>();

        for ( Map.Entry<String,String> paramsEntry : params.entrySet() )
        {
            String settingName = paramsEntry.getKey();
            Matcher matcher = groupSettingPattern.matcher( settingName );
            if ( !matcher.matches() )
            {
                continue;
            }

            String policyName = matcher.group( 1 );
            String shortKey = matcher.group( 2 );

            String formatString = getFormatAsString( params, policyName );

            BaseSslPolicyConfig.Format format = getFormat( formatString );

            Set<String> validShortKeys = policyNames.computeIfAbsent( policyName, ignored -> validKeys( format ) );

            if ( !validShortKeys.contains( shortKey ) )
            {
                throw new InvalidSettingException( "Invalid setting name: " + settingName );
            }

            validatedParams.put( settingName, paramsEntry.getValue() );
        }

        for ( String policyName : policyNames.keySet() )
        {
            PemSslPolicyConfig policy = new PemSslPolicyConfig( policyName );

            if ( !params.containsKey( policy.base_directory.name() ) )
            {
                throw new InvalidSettingException( "Missing mandatory setting: " + policy.base_directory.name() );
            }
        }

        return validatedParams;
    }

    private Set<String> validKeys( BaseSslPolicyConfig.Format format )
    {
        Set<String> validShortKeys;
        if ( format.equals( BaseSslPolicyConfig.Format.PEM ) )
        {
            validShortKeys = extractValidPemShortKeys();
        }
        else
        {
            validShortKeys = extractValidKeyStoreShortKeys();
        }
        return validShortKeys;
    }

    private BaseSslPolicyConfig.Format getFormat( String formatString )
    {
        BaseSslPolicyConfig.Format format;
        try
        {
            format = BaseSslPolicyConfig.Format.valueOf( formatString );
        }
        catch ( IllegalArgumentException e )
        {
            throw new InvalidSettingException( "Unrecognised format: " + formatString );
        }
        return format;
    }

    private String getFormatAsString( Map<String,String> params, String policyName )
    {
        String formatKey = "dbms.ssl.policy." + policyName + ".format";
        String formatStringRaw = params.get( formatKey );
        if ( formatStringRaw == null )
        {
            throw new InvalidSettingException( "Missing format: " + formatKey );
        }
        return formatStringRaw.toUpperCase();
    }

    private String groupPrefix()
    {
        return PemSslPolicyConfig.class.getDeclaredAnnotation( Group.class ).value();
    }

    private Set<String> extractValidPemShortKeys()
    {
        String policyName = "example";
        int prefixLength = groupPrefix().length() + 1 + policyName.length() + 1; // dbms.ssl.policy.example.

        PemSslPolicyConfig examplePolicy = new PemSslPolicyConfig( policyName );

        return extractValidShortKeys( examplePolicy, prefixLength );
    }

    private Set<String> extractValidKeyStoreShortKeys()
    {
        String policyName = "example";
        int prefixLength = groupPrefix().length() + 1 + policyName.length() + 1; // dbms.ssl.policy.example.

        KeyStoreSslPolicyConfig examplePolicy = new KeyStoreSslPolicyConfig( policyName );

        return extractValidShortKeys( examplePolicy, prefixLength );
    }

    private Set<String> extractValidShortKeys( BaseSslPolicyConfig examplePolicy, int prefixLength )
    {
        Set<String> validSettingNames = new HashSet<>();

        Field[] fields = examplePolicy.getClass().getFields();
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
