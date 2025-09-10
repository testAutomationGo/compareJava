package com.testsuite.webdriverUtils;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

import com.testsuite.utils.Config;

import io.github.bonigarcia.wdm.WebDriverManager;

public class webdriverUtils {
    protected WebDriver driver;

    @BeforeMethod
    @Parameters({"env"})
    public void setUp(@Optional("local") String env) {
        String browser = Config.get("browser");
        boolean headless = Boolean.parseBoolean(Config.get("headless"));
        System.out.println("Running tests in " + env + " environment with browser: " + browser + ", headless: " + headless);
        if ("chrome".equalsIgnoreCase(browser)) {
            WebDriverManager.chromedriver().setup();
            ChromeOptions opts = new ChromeOptions();
            if (headless) opts.addArguments("--headless=old");
            opts.addArguments("--no-sandbox");
            opts.addArguments("--disable-dev-shm-usage");
            opts.addArguments("--disable-gpu");
            opts.addArguments("--window-size=1920,1080");
            opts.setCapability("goog:loggingPrefs", java.util.Map.of("browser", "ALL"));
            driver = new ChromeDriver(opts);
        } else {
            throw new RuntimeException("Unsupported browser: " + browser);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        if (driver != null) driver.quit();
    }
}

