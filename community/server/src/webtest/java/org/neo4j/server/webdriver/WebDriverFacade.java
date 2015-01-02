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

import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;

import org.openqa.selenium.Platform;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.remote.DesiredCapabilities;

public class WebDriverFacade
{
    private WebDriver browser;

    public WebDriver getWebDriver() throws InvocationTargetException, IllegalAccessException, InstantiationException
    {
        if ( browser == null )
        {
            String driverName = lookupDriverImplementation();
            try
            {
                browser = WebDriverImplementation.valueOf( driverName ).createInstance();
            }
            catch ( Exception problem )
            {
                throw new RuntimeException( "Couldn't instantiate the selected selenium driver. See nested exception.", problem );
            }
        }
        return browser;
    }

    private String lookupDriverImplementation()
    {
        String driverName = System.getProperty( "webdriver.implementation", WebDriverImplementation.Firefox.name() );
        System.out.println( "Using " + driverName );
        return driverName;
    }

    public enum WebDriverImplementation {
        Firefox()
            {
                public WebDriver createInstance()
                {
                    return new FirefoxDriver();
                }
            },
        Chrome() {
            public WebDriver createInstance()
            {
                WebdriverChromeDriver.ensurePresent();
                return new ChromeDriver();
            }
        },
        SauceLabsFirefoxWindows() {
            public WebDriver createInstance() throws MalformedURLException
            {
                DesiredCapabilities capabilities = DesiredCapabilities.firefox();
                capabilities.setCapability( "version", "5" );
                capabilities.setCapability( "platform", Platform.VISTA );
                capabilities.setCapability( "name", "Neo4j Web Testing" );

                return WebdriverSauceLabsDriver.createDriver( capabilities );
            }
        },
        SauceLabsChromeWindows() {
            public WebDriver createInstance() throws MalformedURLException
            {
                DesiredCapabilities capabilities = DesiredCapabilities.chrome();
                capabilities.setCapability( "platform", Platform.VISTA );
                capabilities.setCapability( "name", "Neo4j Web Testing" );

                return WebdriverSauceLabsDriver.createDriver( capabilities );
            }
        },
        SauceLabsInternetExplorerWindows() {
            public WebDriver createInstance() throws MalformedURLException
            {
                DesiredCapabilities capabilities = DesiredCapabilities.internetExplorer();
                capabilities.setCapability( "platform", Platform.VISTA );
                capabilities.setCapability( "name", "Neo4j Web Testing" );

                return WebdriverSauceLabsDriver.createDriver( capabilities );
            }
        };

        public abstract WebDriver createInstance() throws Exception;
    }

    public void quitBrowser() throws IllegalAccessException, InvocationTargetException, InstantiationException
    {
        if ( browser != null )
        {
            browser.quit();
        }
    }
}

