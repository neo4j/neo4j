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
package cypher;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;
import org.opencypher.tools.tck.TCKCucumberTemplate;

import static cypher.SpecSuiteConstants.BLACKLIST_PLUGIN;
import static cypher.SpecSuiteConstants.DB_CONFIG;
import static cypher.SpecSuiteConstants.GLUE_PATH;
import static cypher.SpecSuiteConstants.HTML_REPORT;
import static cypher.SpecSuiteConstants.JSON_REPORT;

@RunWith( CompatibilitySpecSuiteResources.class )
public class CompatibilitySpecSuiteTest
{

    // These two constants are only used to make testing and debugging easier.
    // If you want to run only a single feature, put the name of the feature file in `FEATURE_TO_RUN` (including .feature)
    // If you want to run only a single scenario, put (part of) its name in the `SCENARIO_NAME_REQUIRED` constant
    // Do not forget to clear these strings to empty strings before you commit!!
    public static final String FEATURE_TO_RUN = "";
    public static final String SCENARIO_NAME_REQUIRED = "";

    public static final boolean REPLACE_EXISTING = false;

    @RunWith( Cucumber.class )
    @CucumberOptions(
            plugin = {
                    DB_CONFIG + "rule.json",
                    HTML_REPORT + SUITE_NAME + "/rule",
                    JSON_REPORT + SUITE_NAME + "/rule",
                    BLACKLIST_PLUGIN + "rule.txt"
            },
            glue = { GLUE_PATH },
            features = { FEATURE_PATH + FEATURE_TO_RUN },
            tags = { "~@pending" },
            strict = true
    )
    public static class Rule
    {
    }

    @RunWith( Cucumber.class )
    @CucumberOptions(
            plugin = {
                    DB_CONFIG + "cost.json",
                    HTML_REPORT + SUITE_NAME + "/cost",
                    JSON_REPORT + SUITE_NAME + "/cost",
                    BLACKLIST_PLUGIN + "cost.txt"
            },
            glue = { GLUE_PATH },
            features = { FEATURE_PATH + FEATURE_TO_RUN },
            tags = { "~@pending" },
            strict = true
    )
    public static class Cost
    {
    }

    @RunWith( Cucumber.class )
    @CucumberOptions(
            plugin = {
                    DB_CONFIG + "cost-compiled.json",
                    HTML_REPORT + SUITE_NAME + "/cost-compiled",
                    JSON_REPORT + SUITE_NAME + "/cost-compiled",
                    BLACKLIST_PLUGIN + "cost-compiled.txt"
            },
            glue = { GLUE_PATH },
            features = { FEATURE_PATH + FEATURE_TO_RUN },
            tags = { "~@pending" },
            strict = true
    )
    public static class CostCompiled
    {
    }

    @RunWith( Cucumber.class )
    @CucumberOptions(
            plugin = {
                    DB_CONFIG + "compatibility-23.json",
                    HTML_REPORT + SUITE_NAME + "/compatibility-23",
                    JSON_REPORT + SUITE_NAME + "/compatibility-23",
                    BLACKLIST_PLUGIN + "compatibility-23.txt"
            },
            glue = { GLUE_PATH },
            features = { FEATURE_PATH + FEATURE_TO_RUN },
            tags = { "~@pending" },
            strict = true
    )
    public static class Compatibility23
    {
    }

    @RunWith( Cucumber.class )
    @CucumberOptions(
            plugin = {
                    DB_CONFIG + "compatibility-30.json",
                    HTML_REPORT + SUITE_NAME + "/compatibility-30",
                    JSON_REPORT + SUITE_NAME + "/compatibility-30",
                    BLACKLIST_PLUGIN + "compatibility-30.txt"
            },
            glue = { GLUE_PATH },
            features = { FEATURE_PATH + FEATURE_TO_RUN },
            tags = { "~@pending" },
            strict = true
    )
    public static class Compatibility30
    {
    }

    // constants for TCK configuration

    public static final String SUITE_NAME = "compatibility-spec-suite";

    @SuppressWarnings( "unused" )
    public static final Class<?> RESOURCE_CLASS = TCKCucumberTemplate.class;

    private static final String FEATURE_PATH = "target/" + SUITE_NAME + "/features/";
}
