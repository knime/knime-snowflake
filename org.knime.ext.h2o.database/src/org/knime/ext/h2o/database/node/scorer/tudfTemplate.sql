create or replace temporary function <$functionName$>(<$inSQLArguments$>)
returns table(<$outSQLArguments$>)
language java
called on null input
imports = (<$imports$>)
handler='KNIMEH2O'
target_path='@<$stageName$>/<$functionName$>.jar'
as 
$$

import java.io.File;
import java.util.stream.Stream;
import org.knime.snowflake.h2o.companion.udf.MojoPredictor;
import org.knime.snowflake.h2o.companion.udf.util.PredictionResult;
import hex.genmodel.easy.exception.PredictException;

public class KNIMEH2O {

    public static class OutputRow {

		<$fieldDeclarations$>

        public OutputRow() {
        }

		public OutputRow(final PredictionResult<<$javaResultType$>> prediction) {
            <$fieldInitializations$>
		}
	}

    public static Class getOutputClass() {
        return OutputRow.class;
    }

    private final MojoPredictor m_predictor = new <$mojoClass$>();
    
    public KNIMEH2O() throws Exception {
        final String importDirectory = System.getProperty("com.snowflake.import_directory");
        final String pathname = importDirectory + "<$fileName$>";
        m_predictor.init(new File(pathname), <$convertUnknownCategoricalLevelsToNa$>, <$columnNames$>);
    }

    public Stream<OutputRow> process(<$inParameters$>) throws Exception {
        try {
            final PredictionResult<<$javaResultType$>> prediction = m_predictor.predict(<$inVariables$>);
            return Stream.of(new OutputRow(prediction)); 
            
        } catch (PredictException e) {
            if (<$failOnPredictException$>) {
                throw new IllegalArgumentException("PredictException: " + e.getMessage());
            } else {
                return Stream.of(new OutputRow());
            }
        }    
    }
}
$$