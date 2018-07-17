package testOutputWriter;

import org.junit.Before;
import org.junit.Test;

public class TestOutputWriterTest {

	private TestOutputWriter writer;

	@Before
	public void setup() {
		writer = new TestOutputWriter("test-csv.csv");
	}

	@Test
	public void write() {
		writer.write(new TestOutputImpl(0.0, 0.0, "aScheduler", "aJobGraph", "anInstanceSet", 1L));
	}
}
