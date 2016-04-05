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

import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.kernel.configuration.Group;
import org.neo4j.kernel.configuration.GroupSettingSupport;
import org.neo4j.kernel.configuration.Internal;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.setting;

public class SettingsDocumenterTest
{
    @Test
    public void shouldDocumentBasicSettingsClass() throws Throwable
    {
        // when
        String result = new SettingsDocumenter().document( SimpleSettings.class );

        // then
        // Note, I got the text below from invoking the existing un-tested
        // config documenter implementation, and running this on it:
        //
        // for ( String line : result.split( "\\n" ) )
        // {
        //    System.out.println("\"" + line
        //            .replace( "\\", "\\\\" )
        //            .replace( "\"", "\\\"" ) +"%n\" +");
        // }
        //
        // My intent here is to add tests and refactor the code, it could be
        // that there are errors in the original implementation that I've missed,
        // in which case you should trust your best judgement, and change the assertion
        // below accordingly.
        assertThat( result, equalTo( String.format(
            "[[config-org.neo4j.kernel.configuration.docs.SettingsDocumenterTest-SimpleSettings]]%n" +
            ".List of configuration settings%n" +
            "ifndef::nonhtmloutput[]%n" +
            "%n" +
            "[options=\"header\"]%n" +
            "|===%n" +
            "|Name|Description%n" +
            "|<<config_public.default,public.default>>|Public with default.%n" +
            "|<<config_public.nodefault,public.nodefault>>|Public nodefault.%n" +
            "|===%n" +
            "endif::nonhtmloutput[]%n" +
            "%n" +
            "ifdef::nonhtmloutput[]%n" +
            "%n" +
            "* <<config_public.default,public.default>>: Public with default.%n" +
            "* <<config_public.nodefault,public.nodefault>>: Public nodefault.%n" +
            "endif::nonhtmloutput[]%n" +
            "%n" +
            "%n" +
            "[[config-org.neo4j.kernel.configuration.docs.SettingsDocumenterTest-SimpleSettings-deprecated]]%n" +
            ".Deprecated settings%n" +
            "ifndef::nonhtmloutput[]%n" +
            "%n" +
            "[options=\"header\"]%n" +
            "|===%n" +
            "|Name|Description%n" +
            "|<<config_public.deprecated,public.deprecated>>|Public deprecated.%n" +
            "|===%n" +
            "endif::nonhtmloutput[]%n" +
            "%n" +
            "ifdef::nonhtmloutput[]%n" +
            "%n" +
            "* <<config_public.deprecated,public.deprecated>>: Public deprecated.%n" +
            "endif::nonhtmloutput[]%n" +
            "%n" +
            "%n" +
            "ifndef::nonhtmloutput[]%n" +
            "[[config_public.default]]%n" +
            ".public.default%n" +
            "[cols=\"<1h,<4\"]%n" +
            "|===%n" +
            "|Description a|Public with default.%n" +
            "|Valid values a|public.default is an integer%n" +
            "|Default value m|1%n" +
            "|===%n" +
            "endif::nonhtmloutput[]%n" +
            "%n" +
            "ifdef::nonhtmloutput[]%n" +
            "[[config_public.default]]%n" +
            ".public.default%n" +
            "[cols=\"<1h,<4\"]%n" +
            "|===%n" +
            "|Description a|Public with default.%n" +
            "|Valid values a|public.default is an integer%n" +
            "|Default value m|1%n" +
            "|===%n" +
            "endif::nonhtmloutput[]%n" +
            "%n" +
            "ifndef::nonhtmloutput[]%n" +
            "[[config_public.deprecated]]%n" +
            ".public.deprecated%n" +
            "[cols=\"<1h,<4\"]%n" +
            "|===%n" +
            "|Description a|Public deprecated.%n" +
            "|Valid values a|public.deprecated is a boolean%n" +
            "|Default value m|false%n" +
            "|Deprecated a|The `public.deprecated` configuration setting has been deprecated.%n" +
            "|===%n" +
            "endif::nonhtmloutput[]%n" +
            "%n" +
            "ifdef::nonhtmloutput[]%n" +
            "[[config_public.deprecated]]%n" +
            ".public.deprecated%n" +
            "[cols=\"<1h,<4\"]%n" +
            "|===%n" +
            "|Description a|Public deprecated.%n" +
            "|Valid values a|public.deprecated is a boolean%n" +
            "|Default value m|false%n" +
            "|Deprecated a|The `public.deprecated` configuration setting has been deprecated.%n" +
            "|===%n" +
            "endif::nonhtmloutput[]%n" +
            "%n" +
            "ifndef::nonhtmloutput[]%n" +
            "[[config_public.nodefault]]%n" +
            ".public.nodefault%n" +
            "[cols=\"<1h,<4\"]%n" +
            "|===%n" +
            "|Description a|Public nodefault.%n" +
            "|Valid values a|public.nodefault is a string%n" +
            "|===%n" +
            "endif::nonhtmloutput[]%n" +
            "%n" +
            "ifdef::nonhtmloutput[]%n" +
            "[[config_public.nodefault]]%n" +
            ".public.nodefault%n" +
            "[cols=\"<1h,<4\"]%n" +
            "|===%n" +
            "|Description a|Public nodefault.%n" +
            "|Valid values a|public.nodefault is a string%n" +
            "|===%n" +
            "endif::nonhtmloutput[]%n%n" ) ));
    }

    @Test
    public void shouldDocumentGroupConfiguration() throws Throwable
    {
        // when
        String result = new SettingsDocumenter().document( Giraffe.class );

        // then
        assertThat( result, equalTo( String.format(
                "[[config-org.neo4j.kernel.configuration.docs.SettingsDocumenterTest-Giraffe]]%n" +
                ".Use this group to configure giraffes%n" +
                "ifndef::nonhtmloutput[]%n" +
                "%n" +
                "[options=\"header\"]%n" +
                "|===%n" +
                "|Name|Description%n" +
                "|<<config_group.key.spot_count,group.(key).spot_count>>|Number of spots this giraffe has, in " +
                "number.%n" +
                "|<<config_group.key.type,group.(key).type>>|Animal type.%n" +
                "|===%n" +
                "endif::nonhtmloutput[]%n" +
                "%n" +
                "ifdef::nonhtmloutput[]%n" +
                "%n" +
                "* <<config_group.key.spot_count,group.(key).spot_count>>: Number of spots this giraffe has, in " +
                "number.%n" +
                "* <<config_group.key.type,group.(key).type>>: Animal type.%n" +
                "endif::nonhtmloutput[]%n" +
                "%n" +
                "%n" +
                "ifndef::nonhtmloutput[]%n" +
                "[[config_group.key.spot_count]]%n" +
                ".group.(key).spot_count%n" +
                "[cols=\"<1h,<4\"]%n" +
                "|===%n" +
                "|Description a|Number of spots this giraffe has, in number.%n" +
                "|Valid values a|spot_count is an integer%n" +
                "|Default value m|12%n" +
                "|===%n" +
                "endif::nonhtmloutput[]%n" +
                "%n" +
                "ifdef::nonhtmloutput[]%n" +
                "[[config_group.key.spot_count]]%n" +
                ".group.(key).spot_count%n" +
                "[cols=\"<1h,<4\"]%n" +
                "|===%n" +
                "|Description a|Number of spots this giraffe has, in number.%n" +
                "|Valid values a|spot_count is an integer%n" +
                "|Default value m|12%n" +
                "|===%n" +
                "endif::nonhtmloutput[]%n" +
                "%n" +
                "ifndef::nonhtmloutput[]%n" +
                "[[config_group.key.type]]%n" +
                ".group.(key).type%n" +
                "[cols=\"<1h,<4\"]%n" +
                "|===%n" +
                "|Description a|Animal type.%n" +
                "|Valid values a|type is a string%n" +
                "|Default value m|giraffe%n" +
                "|===%n" +
                "endif::nonhtmloutput[]%n" +
                "%n" +
                "ifdef::nonhtmloutput[]%n" +
                "[[config_group.key.type]]%n" +
                ".group.(key).type%n" +
                "[cols=\"<1h,<4\"]%n" +
                "|===%n" +
                "|Description a|Animal type.%n" +
                "|Valid values a|type is a string%n" +
                "|Default value m|giraffe%n" +
                "|===%n" +
                "endif::nonhtmloutput[]%n%n"
        ) ));
    }

    public interface SimpleSettings
    {
        @Description("Public nodefault")
        Setting<String> public_nodefault = setting( "public.nodefault", STRING, NO_DEFAULT );

        @Description("Public with default")
        Setting<Integer> public_with_default = setting("public.default", INTEGER, "1");

        @Deprecated
        @Description("Public deprecated")
        Setting<Boolean> public_deprecated = setting("public.deprecated", BOOLEAN, "false");

        @Internal
        @Description("Internal with default")
        Setting<String> internal_with_default = setting("internal.default", STRING, "something");
    }

    @Group( "group" )
    public static class Animal
    {
        @Description( "Animal type" )
        public final Setting<String> type;

        protected final GroupSettingSupport group;

        protected Animal( String key, String typeDefault )
        {
            group = new GroupSettingSupport( Animal.class, key );
            type = group.scope( setting( "type", STRING, typeDefault ) );
        }
    }

    @Description( "Use this group to configure giraffes" )
    public static class Giraffe extends Animal
    {
        @Description( "Number of spots this giraffe has, in number." )
        public final Setting<Integer> number_of_spots;

        public Giraffe()
        {
            this("(key)");
        }

        public Giraffe(String key)
        {
            super(key, /* type=*/"giraffe");
            number_of_spots = group.scope( setting( "spot_count", INTEGER, "12" ));
        }
    }
}
