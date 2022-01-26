-- inline function with import and exception
-- temporary function lives as long as the session lives but the model and jar file are not deleted!!!

create or replace temporary function <$functionName$>(<$arguments$>)
returns <$sqlReturnType$>
language java
imports = (<$imports$>)
handler='KNIMEH2O.predict'
target_path='@<$stageName$>/<$fileName$>.jar'
as 
$$

import java.io.File;
import org.knime.snowflake.h2o.companion.udf.MojoPredictor;

public class KNIMEH2O {

    private final MojoPredictor m_predictor = new <$mojoClass$>();
    
    public <$javaReturnType$> predict(<$parameter$>) throws Exception {
        final String importDirectory = System.getProperty("com.snowflake.import_directory");
        final String pathname = importDirectory + "<$fileName$>";
        m_predictor.init(new File(pathname));
        m_predictor.predict(<$variables$>);
        return (<$javaReturnType$>) m_predictor.getResult(); 
    }      
}

$$