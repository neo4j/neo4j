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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;

public class WebdriverSauceLabsDriver
{
    public static final String WEBDRIVER_SAUCE_LABS_URL = "webdriver.sauce.labs.url";

    public static WebDriver createDriver( DesiredCapabilities capabilities ) throws MalformedURLException
    {
        String sauceLabsUrl = System.getProperty( WEBDRIVER_SAUCE_LABS_URL );
        if (sauceLabsUrl == null)
        {
            throw new IllegalArgumentException( String.format( "To use SauceLabs, %s system property must be provided",
                    WEBDRIVER_SAUCE_LABS_URL) );
        }

        WebDriver driver = new RemoteWebDriver(
           new URL( sauceLabsUrl ),
           capabilities);
        driver.manage().timeouts().implicitlyWait(30, TimeUnit.SECONDS);
        return driver;
    }
}
