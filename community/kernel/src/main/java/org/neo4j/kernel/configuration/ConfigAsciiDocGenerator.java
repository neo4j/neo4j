/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ResourceBundle;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.graphdb.factory.SettingsResourceBundle;
import org.neo4j.helpers.Triplet;

/**
 * Generates AsciiDoc by using subclasses of {@link org.neo4j.graphdb.factory.SettingsResourceBundle},
 * to pick up localized versions of the documentation.
 */
public class ConfigAsciiDocGenerator
{
    private static final Pattern CONFIG_SETTING_PATTERN = Pattern.compile( "[a-z0-9]+((\\.|_)[a-z0-9]+)+" );
    private static final Pattern NUMBER_OR_IP = Pattern.compile( "[0-9\\.]+" );
    private static final List<String> CONFIG_NAMES_BLACKLIST = Arrays.asList( "round_robin", "keep_all", "keep_last",
            "keep_none" );

    public String generateDocsFor(
            Class<? extends SettingsResourceBundle> settingsResource )
    {
        return generateDocsFor( settingsResource.getName() );
    }

    public String generateDocsFor( String settingsResource )
    {
        ResourceBundle bundle;
        try
        {
            bundle = new SettingsResourceBundle(
                    Class.forName( settingsResource ) );
        }
        catch ( ClassNotFoundException e )
        {
            throw new RuntimeException( "Couldn't load settings class: ", e );
        }
        System.out.println( "Generating docs for: " + settingsResource );

        String settingsResourceId = "config-" + settingsResource;
        String bundleDescription = "List of configuration settings";
        if ( bundle.containsKey( SettingsResourceBundle.CLASS_DESCRIPTION ) )
        {
            bundleDescription = bundle.getString( SettingsResourceBundle.CLASS_DESCRIPTION );
        }

        StringBuilder details = new StringBuilder();
        List<Triplet<String, String, String>> beanList = new ArrayList<>();
        List<Triplet<String, String, String>> deprecatedBeansList = new ArrayList<>();

        AsciiDocListGenerator listGenerator = new AsciiDocListGenerator( settingsResourceId, bundleDescription, true );
        AsciiDocListGenerator deprecatedBeanslistGenerator = 
                new AsciiDocListGenerator( settingsResourceId + "-deprecated", "Deprecated settings", true );

        List<String> keys = new ArrayList<String>( bundle.keySet() );
        Collections.sort( keys );

        for ( String property : keys )
        {
            if ( property.endsWith( SettingsResourceBundle.DESCRIPTION ) )
            {
                String name = property.substring( 0, property.lastIndexOf( "." ) );
                String monospacedName = "+" + name + "+";
                String internalKey = name + SettingsResourceBundle.INTERNAL;
                if ( bundle.containsKey( internalKey ) )
                {
                    continue;
                }
                String id = "config_" + name;
                details.append( "[[" )
                        .append( id )
                        .append( "]]\n" )
                        .append( '.' )
                        .append( name )
                        .append( '\n' )
                        .append( "[cols=\"<1h,<4\"]\n" )
                        .append( "|===\n" );

                String defaultKey = name + SettingsResourceBundle.DEFAULT;
                String description = linkifyConfigSettings( bundle.getString( property ) );
                details.append( "|Description a|" );
                addWithDotAtEndAsNeeeded( details, description );
                
                String validationKey = name + SettingsResourceBundle.VALIDATIONMESSAGE;
                if ( bundle.containsKey( validationKey ) )
                {
                    String validation = bundle.getString( validationKey );
                    validation = validation.replace( name, monospacedName );
                    details.append( "|Valid values a|" );
                    addWithDotAtEndAsNeeeded( details, linkifyConfigSettings( validation, name ) );
                }

                if ( bundle.containsKey( defaultKey ) )
                {
                    String defaultValue = bundle.getString( defaultKey );
                    if ( !defaultValue.equals( "__DEFAULT__" ) )
                    {
                        details.append( "|Default value m|" )
                          .append( defaultValue )
                          .append( '\n' );
                    }
                }
                
                String mandatorykey = name + SettingsResourceBundle.MANDATORY;
                if ( bundle.containsKey( mandatorykey ) )
                {
                    details.append( "|Mandatory a|" );
                    addWithDotAtEndAsNeeeded( details, bundle.getString( mandatorykey ).replace( name, monospacedName ) );
                }

                Triplet<String, String, String> beanSummary = Triplet.of( id, name, description );

                String deprecatedKey = name + SettingsResourceBundle.DEPRECATED;
                String obsoletedKey = name + SettingsResourceBundle.OBSOLETED;
                if ( bundle.containsKey( deprecatedKey ) || bundle.containsKey( obsoletedKey ) )
                {
                    details.append( "|Deprecated a|" );
                    if ( bundle.containsKey( obsoletedKey ) )
                    {
                        addWithDotAtEndAsNeeeded( details, linkifyConfigSettings( bundle.getString( obsoletedKey ) ) );
                    }
                    else
                    {
                        addWithDotAtEndAsNeeeded( details, linkifyConfigSettings( bundle.getString( deprecatedKey ) ) );
                    }
                    deprecatedBeansList.add( beanSummary );
                }
                else
                {
                    beanList.add( beanSummary );
                }

                details.append( "|===\n\n" );
            }
        }
        return listGenerator.generateListAndTableCombo( beanList )
               + ( deprecatedBeansList.isEmpty() ? ""
                       : deprecatedBeanslistGenerator.generateListAndTableCombo( deprecatedBeansList ) )
               + details.toString();
    }
    
    private void addWithDotAtEndAsNeeeded( StringBuilder sb, String message )
    {
        sb.append( message );
        if ( !message.endsWith( "." ) && !message.endsWith( ". " ) )
        {
            sb.append( '.' );
        }
        sb.append( '\n' );
    }

    private String linkifyConfigSettings( String text, String nameToNotLink )
    {
        Matcher matcher = CONFIG_SETTING_PATTERN.matcher( text );
        StringBuffer result = new StringBuffer( 256 );
        while ( matcher.find() )
        {
            String match = matcher.group();
            if ( match.endsWith( ".log" ) )
            {
                // a filenamne
                match = "_" + match + "_";
            }
            else if ( match.equals( nameToNotLink ) )
            {
                // don't link to the settings we're describing
                match = "`" + match + "`";
            }
            else if ( CONFIG_NAMES_BLACKLIST.contains( match ) )
            {
                // an option value; do nothing
            }
            else if ( NUMBER_OR_IP.matcher( match ).matches() )
            {
                // number or ip; do nothing
            }
            else
            {
                // replace setting name with link to setting
                match = makeConfigXref( match );
            }
            matcher.appendReplacement( result, match );
        }
        matcher.appendTail( result );
        return result.toString();
    }

    private String linkifyConfigSettings( String text )
    {
        return linkifyConfigSettings( text, null );
    }

    private String makeConfigXref( String settingName )
    {
        return "+<<config_" + settingName + "," + settingName + ">>+";
    }
}
