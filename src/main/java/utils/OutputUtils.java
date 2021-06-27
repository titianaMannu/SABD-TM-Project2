package utils;

import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.IOException;

/**
 * Class used to format the queries' outcomes into string
 */
public class OutputUtils {

	private static final String RESULTS_DIRECTORY = "Results";


	/**
	 * Flink output filepath: query1 && query2
	 */
	public static final String[] FLINK_OUTPUT_FILES = {
			ConfStrings.QUERY1_CSV_WEEKLY_OUT_PATH.getString(),
			ConfStrings.QUERY1_CSV_MONTHLY_OUT_PATH.getString()
	};

	public  static final String[] FLINK_HEADERS = {ConfStrings.CSV_HEADER_QUERY1.getString(), ConfStrings.CSV_HEADER_QUERY1.getString()};

	/**
	 * Scope: Global
	 * Used to remove old files in Results directory
	 */
	public static void cleanResultsFolder() {
		try {
			FileUtils.cleanDirectory(new File(RESULTS_DIRECTORY));
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("Could not clean Results directory");
		}
	}

}
