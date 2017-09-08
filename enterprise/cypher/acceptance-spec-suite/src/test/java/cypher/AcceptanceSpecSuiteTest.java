/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package cypher;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import cypher.cucumber.BlacklistPlugin;
import org.junit.AfterClass;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.Set;

import static cypher.SpecSuiteConstants.BLACKLIST_PLUGIN;
import static cypher.SpecSuiteConstants.CYPHER_OPTION_PLUGIN;
import static cypher.SpecSuiteConstants.DB_CONFIG;
import static cypher.SpecSuiteConstants.GLUE_PATH;
import static cypher.SpecSuiteConstants.HTML_REPORT;
import static cypher.SpecSuiteConstants.JSON_REPORT;
import static junit.framework.TestCase.fail;

@RunWith( Enclosed.class )
public class AcceptanceSpecSuiteTest
{

    // These two constants are only used to make testing and debugging easier.
    // If you want to run only a single feature, put the name of the feature file in `FEATURE_TO_RUN` (including .feature)
    // If you want to run only a single scenario, put (part of) its name in the `SCENARIO_NAME_REQUIRED` constant
    // Do not forget to clear these strings to empty strings before you commit!!
    public static final String FEATURE_TO_RUN = "";
    public static final String SCENARIO_NAME_REQUIRED = "";

    private AcceptanceSpecSuiteTest()
    {
    }

    public abstract static class Base
    {
        @AfterClass
        public static void teardown()
        {
            if ( FEATURE_TO_RUN.isEmpty() && SCENARIO_NAME_REQUIRED.isEmpty() )
            {
                Set<String> diff = BlacklistPlugin.getDiffBetweenBlacklistAndUsedScenarios();
                if ( !diff.isEmpty() )
                {
                    fail( "The following scenarios were blacklisted but no test corresponds to that name:\n" + String.join( "\n", diff ) );
                }
            }
        }
    }

    @RunWith( Cucumber.class )
    @CucumberOptions(
            plugin = {
                    DB_CONFIG + "default.json",
                    HTML_REPORT + SUITE_NAME + "/default",
                    JSON_REPORT + SUITE_NAME + "/default",
                    BLACKLIST_PLUGIN + "default.txt"
            },
            glue = { GLUE_PATH },
            features = { FEATURE_PATH + FEATURE_TO_RUN },
            tags = { "~@pending" },
            strict = true
    )
    public static class Default extends Base
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
    public static class Cost extends Base
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
    public static class CostCompiled extends Base
    {
    }

    @RunWith( Cucumber.class )
    @CucumberOptions(
            plugin = {
                    DB_CONFIG + "cost-compiled.json",
                    HTML_REPORT + SUITE_NAME + "/cost-compiled-sourcecode",
                    JSON_REPORT + SUITE_NAME + "/cost-compiled-sourcecode",
                    BLACKLIST_PLUGIN + "cost-compiled.txt",
                    CYPHER_OPTION_PLUGIN + "cost-compiled-sourcecode.txt"
            },
            glue = { GLUE_PATH },
            features = { FEATURE_PATH + FEATURE_TO_RUN },
            tags = { "~@pending" },
            strict = true
    )
    public static class CostCompiledSourceCode extends Base
    {
    }

    @RunWith( Cucumber.class )
    @CucumberOptions(
            plugin = {
                    DB_CONFIG + "cost-slotted.json",
                    HTML_REPORT + SUITE_NAME + "/cost-slotted",
                    JSON_REPORT + SUITE_NAME + "/cost-slotted",
                    BLACKLIST_PLUGIN + "cost-slotted.txt"
            },
            glue = { GLUE_PATH },
            features = { FEATURE_PATH + FEATURE_TO_RUN },
            tags = { "~@pending" },
            strict = true
    )
    public static class CostSlotted extends Base
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
    public static class Compatibility23 extends Base
    {
    }

    @RunWith( Cucumber.class )
    @CucumberOptions(
            plugin = {
                    DB_CONFIG + "compatibility-31.json",
                    HTML_REPORT + SUITE_NAME + "/compatibility-31",
                    JSON_REPORT + SUITE_NAME + "/compatibility-31",
                    BLACKLIST_PLUGIN + "compatibility-31.txt"
            },
            glue = { GLUE_PATH },
            features = { FEATURE_PATH + FEATURE_TO_RUN },
            tags = { "~@pending" },
            strict = true
    )
    public static class Compatibility31 extends Base
    {
    }

    @RunWith( Cucumber.class )
    @CucumberOptions(
            plugin = {
                    DB_CONFIG + "compatibility-32.json",
                    HTML_REPORT + SUITE_NAME + "/compatibility-32",
                    JSON_REPORT + SUITE_NAME + "/compatibility-32",
                    BLACKLIST_PLUGIN + "compatibility-32.txt"
            },
            glue = { GLUE_PATH },
            features = { FEATURE_PATH + FEATURE_TO_RUN },
            tags = { "~@pending" },
            strict = true
    )
    public static class Compatibility32 extends Base
    {
    }

    // constants for TCK configuration

    public static final String SUITE_NAME = "acceptance-spec-suite";

    @SuppressWarnings( "unused" )
    public static final Class<?> RESOURCE_CLASS = AcceptanceSpecSuiteTest.class;

    private static final String FEATURE_PATH = "src/test/resources/cypher/features/";
}
