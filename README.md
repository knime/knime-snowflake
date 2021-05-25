# KNIME database and big data modules

## KNIME database

### Maven build 

* Compile project and run integration tests. Checkstyle rules are enabled by default.   

    `mvn clean verify`

* Compile project and run integration tests, plus enable Spotbugs and Javadocs validations.
    
    `mvn clean verify -Dspotbugs.skip=false -Dmaven.javadoc.skip=false`

### For Eclipse users

##### To format XML files to match what Checkstyle mandates, please follow these steps:
1. Open the *Window* menu.
1. Select *Preferences* at the bottom.
1. In the *Preferences* window, within the left-side tree navigate to *XML* → *XML Files* → *Editor*.
1. On the *Editor* panel change the radio button from *Indent using tabs* to *Indent using spaces*.
1. On the same panel, set *Indentation size* to `4`.
1. Before closing the window, don't forget to apply the changes.
