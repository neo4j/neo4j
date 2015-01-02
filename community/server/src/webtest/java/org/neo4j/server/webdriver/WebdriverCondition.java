/**
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
package org.neo4j.server.webdriver;

import org.hamcrest.Matcher;
import org.openqa.selenium.WebDriver;

/**
 * Adds a HTML dump to the exception message when the condition times out.
 */
public class WebdriverCondition<T> extends Condition<T>
{

    private final WebDriver d;
    
    public WebdriverCondition( WebDriver d, Matcher<T> matcher, T state )
    {
        super( matcher, state );
        this.d = d;
    }

    public void waitUntilFulfilled(long timeout, String errorMessage) {
        try {
            super.waitUntilFulfilled( timeout, errorMessage );
        } catch(WebdriverConditionTimeoutException e) {
            throw e;
        } catch(Exception e) {
            e.printStackTrace();
            throw new WebdriverConditionTimeoutException("Webdriver condition failed ("+ e.getMessage() +"), see nested exception. HTML dump follows:\n\n" + d.getPageSource() + "\n\n", d.getPageSource(), e);
        }
    }
}
