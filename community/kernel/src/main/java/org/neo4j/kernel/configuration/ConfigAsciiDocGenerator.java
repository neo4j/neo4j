/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
    private static final String DEFAULT_MARKER = "__DEFAULT__";
    private static final Pattern CONFIG_SETTING_PATTERN = Pattern.compile( "[a-z0-9]+((\\.|_)[a-z0-9]+)+" );
    private static final Pattern NUMBER_OR_IP = Pattern.compile( "[0-9\\.]+" );
    private static final List<String> CONFIG_NAMES_BLACKLIST = Arrays.asList( "round_robin", "keep_all", "keep_last",
            "keep_none", "metrics.neo4j", "i.e", "e.g" );
    static final String IFDEF_HTMLOUTPUT = "ifndef::nonhtmloutput[]\n";
    static final String IFDEF_NONHTMLOUTPUT = "ifdef::nonhtmloutput[]\n";
    static final String ENDIF = "endif::nonhtmloutput[]\n\n";

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
                String internalKey = name + SettingsResourceBundle.INTERNAL;
                if ( !bundle.containsKey( internalKey ) )
                {
                    String id = "config_" + name;
                    String descriptionKey = name + SettingsResourceBundle.DESCRIPTION;
                    String description = linkifyConfigSettings( bundle.getString( descriptionKey ), name, false );
                    Triplet<String,String,String> beanSummary = Triplet.of( id, name, description );
                    String deprecatedKey = name + SettingsResourceBundle.DEPRECATED;
                    String obsoletedKey = name + SettingsResourceBundle.OBSOLETED;
                    if ( bundle.containsKey( deprecatedKey ) || bundle.containsKey( obsoletedKey ) )
                    {
                        deprecatedBeansList.add( beanSummary );
                    }
                    else
                    {
                        beanList.add( beanSummary );
                    }
                    // add normal and pdf output
                    details.append( addDocsForOneSetting( bundle, id, name, description, false ) );
                    details.append( addDocsForOneSetting( bundle, id, name, description, true ) );
                }
            }
        }
        return listGenerator.generateListAndTableCombo( beanList )
               + ( deprecatedBeansList.isEmpty() ? ""
                       : deprecatedBeanslistGenerator.generateListAndTableCombo( deprecatedBeansList ) )
               + details.toString();
    }

    private String addDocsForOneSetting( ResourceBundle bundle, String id, String name, String description, boolean pdfOutput )
    {
        StringBuilder table = new StringBuilder( 1024 );
        if ( pdfOutput )
        {
            table.append( IFDEF_NONHTMLOUTPUT );
        }
        else
        {
            table.append( IFDEF_HTMLOUTPUT );
        }
        String monospacedName = "`" + name + "`";
        table.append( "[[" )
                .append( id )
                .append( "]]\n" )
                .append( '.' )
                .append( name )
                .append( '\n' )
                .append( "[cols=\"<1h,<4\"]\n" )
                .append( "|===\n" );

        table.append( "|Description a|" );
        addWithDotAtEndAsNeeded( table, description );
        
        String validationKey = name + SettingsResourceBundle.VALIDATIONMESSAGE;
        if ( bundle.containsKey( validationKey ) )
        {
            String validation = bundle.getString( validationKey );
            table.append( "|Valid values a|" );
            addWithDotAtEndAsNeeded( table, linkifyConfigSettings( validation, name, pdfOutput ) );
        }

        String defaultKey = name + SettingsResourceBundle.DEFAULT;
        if ( bundle.containsKey( defaultKey ) )
        {
            String defaultValue = bundle.getString( defaultKey );
            if ( !defaultValue.equals( DEFAULT_MARKER ) )
            {
                table.append( "|Default value m|" )
                  .append( defaultValue )
                  .append( '\n' );
            }
        }
        
        String mandatorykey = name + SettingsResourceBundle.MANDATORY;
        if ( bundle.containsKey( mandatorykey ) )
        {
            table.append( "|Mandatory a|" );
            addWithDotAtEndAsNeeded( table, bundle.getString( mandatorykey ).replace( name, monospacedName ) );
        }

        String deprecatedKey = name + SettingsResourceBundle.DEPRECATED;
        String obsoletedKey = name + SettingsResourceBundle.OBSOLETED;
        if ( bundle.containsKey( deprecatedKey ) || bundle.containsKey( obsoletedKey ) )
        {
            table.append( "|Deprecated a|" );
            if ( bundle.containsKey( obsoletedKey ) )
            {
                addWithDotAtEndAsNeeded( table,
                        linkifyConfigSettings( bundle.getString( obsoletedKey ), name, pdfOutput ) );
            }
            else
            {
                addWithDotAtEndAsNeeded( table,
                        linkifyConfigSettings( bundle.getString( deprecatedKey ), name, pdfOutput ) );
            }
        }

        table.append( "|===\n" )
                .append( ENDIF );
        return table.toString();
    }
    
    private void addWithDotAtEndAsNeeded( StringBuilder sb, String message )
    {
        sb.append( message );
        if ( !message.endsWith( "." ) && !message.endsWith( ". " ) )
        {
            sb.append( '.' );
        }
        sb.append( '\n' );
    }

    private String linkifyConfigSettings( String text, String nameToNotLink, boolean pdfOutput )
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
                // replace setting name with link to setting, if not pdf output
                if ( pdfOutput )
                {
                    match = "`" + match + "`";
                }
                else
                {
                    match = makeConfigXref( match );
                }
            }
            matcher.appendReplacement( result, match );
        }
        matcher.appendTail( result );
        return result.toString();
    }

    private String makeConfigXref( String settingName )
    {
        return "+<<config_" + settingName + "," + settingName + ">>+";
    }
}
