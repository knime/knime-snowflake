Whenever you change code in this plugin you have to execute mvn clean verify to build the jar and upload it to the
lib folder of the org.knime.ext.h2o.database plugin.
Do not forget to update the COMPANION_JAR constant in the UDFObject class if the version has changed!