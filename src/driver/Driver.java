package driver;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class Driver {

	private static final Properties prop = new Properties();

	static {
		try {
			prop.load(new FileInputStream(new File("driver.properties")));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Properties getProp() {
		return prop;
	}

	public static boolean isActivated(String feature) {
		return getProp().getProperty(feature).equals("true");
	}
}
