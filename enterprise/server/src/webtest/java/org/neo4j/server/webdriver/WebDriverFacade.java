/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

package org.neo4j.server.webdriver;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.firefox.FirefoxDriver;

import cuke4duke.annotation.After;

public class WebDriverFacade {
    private static Constructor<WebDriver> driverConstructor = getDriverConstructor();

    @SuppressWarnings("unchecked")
    private static Constructor<WebDriver> getDriverConstructor() {
        String driverName = System.getProperty("webdriver.impl", "org.openqa.selenium.htmlunit.HtmlUnitDriver");
        try {
            return (Constructor<WebDriver>) Thread.currentThread().getContextClassLoader().loadClass(driverName).getConstructor();
        } catch (Throwable problem) {
            throw new RuntimeException("Couldn't load " + driverName, problem);
        }
    }

    private WebDriver browser;

    public WebDriver getWebDriver() throws InvocationTargetException, IllegalAccessException, InstantiationException {
        if (browser == null) {
            browser = driverConstructor.newInstance();
        }
        return browser;
    }

    @After
    public void closeBrowser() throws IllegalAccessException, InvocationTargetException, InstantiationException {
        if (browser != null) {
            browser.close();
            browser.quit();
        }
    }
}

