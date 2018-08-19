package testOutputWriter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class TestOutputWriter<T extends TestOutput> {
	private CSVPrinter csvPrinter;
	private boolean writeHeader;

	public TestOutputWriter() {
		this("scheduling-decisions.csv");
	}

	public TestOutputWriter(String pathString) {
		try {
			String fullPathString = "../csv/" + pathString;
			File f = new File(fullPathString);

			writeHeader = createDirsAndFileIfNeeded(f);

			if(!writeHeader) {
				writeHeader = f.length() == 0;
			}

			BufferedWriter bufferedWriter = Files.newBufferedWriter(Paths.get(fullPathString), StandardOpenOption.APPEND);
			csvPrinter = new CSVPrinter(bufferedWriter, CSVFormat.DEFAULT);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @return false if the file existed
	 * */
	private boolean createDirsAndFileIfNeeded(File f) throws IOException {
		f.getParentFile().mkdirs();
		return f.createNewFile();
	}

	public void write(T testOutput) {
		try {
			if(writeHeader) {
				csvPrinter.printRecord((Object[]) testOutput.getFieldNames());
				writeHeader = false;
			}
			csvPrinter.printRecord(testOutput.getFields());
			csvPrinter.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


}
