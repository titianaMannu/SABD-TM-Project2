package utils;

import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.IOException;


public class FolderUtils {

	/**
	 * Flink output filepath: query1 && query2
	 */
	public static final String[] FLINK_OUTPUT_FILES = {
			ConfStrings.QUERY1_CSV_WEEKLY_OUT_PATH.getString(),
			ConfStrings.QUERY1_CSV_MONTHLY_OUT_PATH.getString(),
			ConfStrings.QUERY2_CSV_WEEKLY_OUT_PATH.getString(),
			ConfStrings.QUERY2_CSV_MONTHLY_OUT_PATH.getString()
	};

	public  static final String[] FLINK_HEADERS = {ConfStrings.CSV_HEADER_QUERY1.getString(),
			ConfStrings.CSV_HEADER_QUERY1.getString(),
			ConfStrings.CSV_HEADER_QUERY2.getString(),
			ConfStrings.CSV_HEADER_QUERY2.getString()
	};

	/**
	 * Scope: Global
	 * Used to clean Results directory
	 */
	public static void cleanResultsFolder() {
		try {
			FileUtils.cleanDirectory(new File(ConfStrings.RESULTS_DIR.getString()));
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("Could not clean Results directory");
		}
	}

}
