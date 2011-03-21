package org.neo4j.server.ext.visualization.features;

import cuke4duke.annotation.After;
import org.openqa.selenium.WebDriver;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class WebDriverFacade {
    private static Constructor<WebDriver> driverConstructor = getDriverConstructor();

    @SuppressWarnings("unchecked")
    private static Constructor<WebDriver> getDriverConstructor() {
        String driverName = System.getProperty("webdriver.impl", "org.openqa.selenium.htmlunit.HtmlUnitDriver");
        try {
            return (Constructor<WebDriver>) Thread.currentThread().getContextClassLoader().loadClass(driverName).getConstructor();
        } catch (Throwable problem) {
            problem.printStackTrace();
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

