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
package cypher;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

@RunWith( Enclosed.class )
public class FeatureSuiteTest
{
    @RunWith( Cucumber.class )
    @CucumberOptions( plugin = {
            "pretty", "html:target/rule-interpreted",
            "cypher.cucumber.reporter.CypherResultReporter:target/rule-interpreted",
            "cypher.cucumber.db.DatabaseProvider:target/dbs",
            "cypher.cucumber.db.DatabaseConfigProvider:/cypher/db/config/rule.json",
    } )
    public static class RuleInterpreted
    {
    }

    @RunWith( Cucumber.class )
    @CucumberOptions( plugin = {
            "pretty", "html:target/cost-interpreted",
            "cypher.cucumber.reporter.CypherResultReporter:target/cost-interpreted",
            "cypher.cucumber.db.DatabaseProvider:target/dbs",
            "cypher.cucumber.db.DatabaseConfigProvider:/cypher/db/config/cost-interpreted.json",
    } )
    public static class CostInterpreted
    {
    }
}
