package snowflake.dummy;

import java.io.File;

import org.knime.snowflake.h2o.companion.udf.MojoPredictor;
import org.knime.snowflake.h2o.companion.udf.MojoPredictorClassification;


public class KNIMEH2O {

	private final MojoPredictor m_predictor = new MojoPredictorClassification();

	public String predict(final Double p0, final Double p1, final Double p2, final Double p3) throws Exception {
		final String importDirectory = System.getProperty("com.snowflake.import_directory");
		final String pathname = importDirectory + "c43c8f99-9b79-42c7-be65-ee7156fae6b6";
		m_predictor.init(new File(pathname));
		m_predictor.predict(p0, p1, p2, p3);
		return m_predictor.getLabel();
	}
}
