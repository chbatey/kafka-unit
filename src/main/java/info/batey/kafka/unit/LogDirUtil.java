package info.batey.kafka.unit;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogDirUtil {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(LogDirUtil.class);

	/**
	 * Creates a temp directory with the specified prefix and attaches a
	 * shutdown hook to delete the directory on jvm exit.
	 * 
	 * @param prefix
	 * @return
	 */
	public static File prepareLogDir(String prefix) {
		final File logDir;
		try {
			logDir = Files.createTempDirectory(prefix).toFile();
		} catch (IOException e) {
			throw new RuntimeException(
					"Unable to create temp folder with prefix " + prefix, e);
		}
		logDir.deleteOnExit();
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					FileUtils.deleteDirectory(logDir);
				} catch (IOException e) {
					LOGGER.warn("Problems deleting temporary directory "
							+ logDir.getAbsolutePath(), e);
				}
			}
		}));
		return logDir;
	}

}
