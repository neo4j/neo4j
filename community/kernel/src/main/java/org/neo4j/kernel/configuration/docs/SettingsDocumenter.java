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
package org.neo4j.kernel.configuration.docs;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.neo4j.kernel.configuration.docs.SettingsDescription.describe;

public class SettingsDocumenter
{
    private static final Predicate<SettingDescription> REGULAR_SETTINGS = ( s ) -> !s.isDeprecated();
    private static final Predicate<SettingDescription> DEPRECATED_SETTINGS = SettingDescription::isDeprecated;

    private static final Pattern CONFIG_SETTING_PATTERN = Pattern.compile( "[a-z0-9]+((\\.|_)[a-z0-9]+)+" );
    // TODO: This one, and the blacklist below, exist because we try and infer what is a config name
    //       in prose text. This is fraught with accidental error. We should instead look into
    //       adopting a convention for how we mark references to other config options in the @Description
    //       et cetera, for instance using back-ticks: "`my.setting`".
    private static final Pattern NUMBER_OR_IP = Pattern.compile( "[0-9\\.]+" );
    private static final List<String> CONFIG_NAMES_BLACKLIST = Arrays.asList( "round_robin", "keep_all", "keep_last",
            "keep_none", "metrics.neo4j", "i.e", "e.g", "fixed_ascending", "fixed_descending", "high_limit" );

    public static final String IFDEF_HTMLOUTPUT = String.format("ifndef::nonhtmloutput[]%n");
    public static final String IFDEF_NONHTMLOUTPUT = String.format("ifdef::nonhtmloutput[]%n");
    public static final String ENDIF = String.format("endif::nonhtmloutput[]%n%n");

    private PrintStream out;

    /**
     * Document a set of configuration classes together as a combined asciidoc section.
     * @param settings one or more settings classes.
     * @return asciidoc content
     * @throws Exception
     */
    public String document( Stream<Class<?>> settings ) throws Exception
    {
        SettingsDescription combinedDescription = settings
                .map( SettingsDescription::describe )
                .reduce( SettingsDescription::union )
                .get();
        return document( combinedDescription );
    }

    public String document( Class<?> settings ) throws Exception
    {
        return document( describe( settings ) );
    }

    public String document( SettingsDescription desc )
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        out = new PrintStream( baos );

        createOverviewTable( REGULAR_SETTINGS, desc.id(), desc.description(), desc );
        createOverviewTable( DEPRECATED_SETTINGS, desc.id() + "-deprecated", "Deprecated settings", desc );

        desc.settings().forEach( (setting) -> {
            documentForHTML( setting );
            documentForPDF( setting );
        });

        out.flush();
        return baos.toString();
    }

    /**
     * This renders an overview table for the settings, where each item in the table
     * links to a more in-detail description of the setting. This link works by convention,
     * as in we depend on a {@link #documentForHTML(SettingDescription)} and {@link #documentForPDF(SettingDescription)}
     * to render details about the setting with a matching tag.
     */
    private void createOverviewTable( Predicate<SettingDescription> filter, String tableId, String tableDescription,
            SettingsDescription desc )
    {
        List<SettingDescription> settings = desc.settings().filter( filter ).collect( toList() );
        if( settings.size() > 0 )
        {
            out.print( new AsciiDocListGenerator( tableId, tableDescription, true )
                    .generateListAndTableCombo( settings ) );
        }
    }

    /**
     * Produce a detailed overview of a single setting for HTML use, this is what the links in
     * {@link #createOverviewTable(Predicate, String, String, SettingsDescription)} lead to.
     */
    private void documentForHTML( SettingDescription item )
    {
        out.print( IFDEF_HTMLOUTPUT );
        document( item.formatted( (p) -> formatParagraph( item.name(), p, this::settingReferenceForHTML ) )  );
        out.print( ENDIF );
    }

    /**
     * Produce a detailed overview of a single setting for PDF use, this is what the links in
     * {@link #createOverviewTable(Predicate, String, String, SettingsDescription)} lead to.
     */
    private void documentForPDF( SettingDescription item )
    {
        out.print( IFDEF_NONHTMLOUTPUT );
        document( item.formatted( (p) -> formatParagraph( item.name(), p, this::settingReferenceForPDF ) ) );
        out.print( ENDIF );
    }

    private void document( SettingDescription item )
    {
        out.printf("[[%s]]%n" +
                   ".%s%n" +
                   "[cols=\"<1h,<4\"]%n" +
                   "|===%n" +
                   "|Description a|%s%n" +
                   "|Valid values a|%s%n",
                item.id(), item.name(),
                item.description(), item.validationMessage() );

        if ( item.hasDefault() )
        {
            out.printf("|Default value m|%s%n", item.defaultValue() );
        }

        if ( item.isMandatory() )
        {
            out.printf( "|Mandatory a|%s%n", item.mandatoryDescription() );
        }

        if ( item.isDeprecated() )
        {
            out.printf( "|Deprecated a|%s%n", item.deprecationMessage() );
        }

        out.printf( "|===%n" );
    }

    /**
     * Cleans up a prose paragraph from a setting (for instance, the setting description)
     * and replaces raw references to other settings with medium-appropriate links or
     * highlights.
     *
     * @param settingName the setting currently being rendered, as this is formatted specially
     * @param paragraph the prose text to "clean up"
     * @param renderReferenceToOtherSetting how to render references to *other* settings
     * @return the formatted paragraph
     */
    private String formatParagraph( String settingName, String paragraph,
            Function<String,String> renderReferenceToOtherSetting )
    {
        return ensureEndsWithPeriod( transformSettingNames( paragraph, settingName, renderReferenceToOtherSetting ) );
    }

    /**
     * Takes a blob of text, finds references to settings in the text, and transforms
     * them with the passed-in lambda.
     */
    private String transformSettingNames( String text, String settingBeingRendered, Function<String,String> transform )
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
            else if ( match.equals( settingBeingRendered ) )
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
                // If all fall through, assume this key refers to a setting name,
                // and render it as requested by the caller.
                match = transform.apply( match );
            }
            matcher.appendReplacement( result, match );
        }
        matcher.appendTail( result );
        return result.toString();
    }

    private String ensureEndsWithPeriod( String message )
    {
        if ( !message.endsWith( "." ) && !message.endsWith( ". " ) )
        {
            message += ".";
        }
        return message;
    }

    private String settingReferenceForHTML( String settingName )
    {
        return "<<config_" + settingName + "," + settingName + ">>";
    }

    private String settingReferenceForPDF( String settingName )
    {
        return "`" + settingName + "`";
    }
}
