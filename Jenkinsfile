#!groovy
BN = BRANCH_NAME == "master" || BRANCH_NAME.startsWith("releases/") ? BRANCH_NAME : "master"

library "knime-pipeline@$BN"

properties([
    pipelineTriggers([
        // knime-database -> knime-cloud -> knime-database-proprietary
        upstream("knime-cloud/${env.BRANCH_NAME.replaceAll('/', '%2F')}")
    ]),
    parameters(workflowTests.getConfigurationsAsParameters()),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

buildConfigurations = [
    Tycho: {
        knimetools.defaultTychoBuild('org.knime.update.snowflake')
    },
    Snowflake: {
         dbTest()
    }
]

try {
    // parallel build steps
   parallel buildConfigurations

    withCredentials([usernamePassword(credentialsId: 'SNOWFLAKE_TESTING_CREDENTIALS', passwordVariable: 'KNIME_SNOWFLAKE_PASSWORD', usernameVariable: 'KNIME_SNOWFLAKE_USER')]) {
        withEnv([ "KNIME_SNOWFLAKE_ADDRESS=knimepartner.eu-central-1:1234" ]) {
            workflowTests.runTests(
                dependencies: [
                    repositories: [
                        'knime-snowflake',
                        'knime-bigdata-externals',
                        'knime-bigdata',
                        'knime-cloud',
                        'knime-database-proprietary',
                        'knime-database',
                        'knime-datageneration',
                        'knime-distance',
                        'knime-distance',
                        'knime-ensembles',
                        'knime-expressions',
                        'knime-filehandling',
                        'knime-h2o',
                        'knime-jep',
                        'knime-jfreechart',
                        'knime-js-base',
                        'knime-kerberos',
                        'knime-office365',
                        'knime-pmml-translation',
                        'knime-pmml',
                        'knime-testing-internal',
                        'knime-textprocessing',
                        'knime-timeseries',
                        'knime-virtual',
                    ],
                    ius: [ 'org.knime.snowflake.testing.janitor']
                ],

            )
        }
    }


    // finally run sonarqube
    stage('Sonarqube analysis') {
        env.lastStage = env.STAGE_NAME
        workflowTests.runSonar()
    }
} catch (ex) {
    currentBuild.result = 'FAILURE'
    throw ex
} finally {
    notifications.notifyBuild(currentBuild.result);
}


def dbTest() {
    node("maven && java11") {
    
        try {
            // verification
            stage("Testing Snowflake: ") {
                env.lastStage = env.STAGE_NAME

                checkout([
                    $class: 'GitSCM',
                    branches: [[name: BRANCH_NAME]],
                    extensions: [[$class: 'RelativeTargetDirectory', relativeTargetDir: 'knime-snowflake'], [$class: 'GitLFSPull']],
                    userRemoteConfigs: [[ credentialsId: 'bitbucket-jenkins', url: 'https://bitbucket.org/KNIME/knime-snowflake' ]]
                ])

                checkout([
                    $class: 'GitSCM',
                    branches: [[name: BN ]],
                    extensions: [[$class: 'RelativeTargetDirectory', relativeTargetDir: 'knime-database'], [$class: 'GitLFSPull']],
                    userRemoteConfigs: [[ credentialsId: 'bitbucket-jenkins', url: 'https://bitbucket.org/KNIME/knime-database' ]]
                ])
     
                withMaven(options: [artifactsPublisher(disabled: true)]) {
                    withCredentials([usernamePassword(credentialsId: 'ARTIFACTORY_CREDENTIALS', passwordVariable: 'ARTIFACTORY_PASSWORD', usernameVariable: 'ARTIFACTORY_LOGIN'), 
                        usernamePassword(credentialsId: 'SNOWFLAKE_TESTING_CREDENTIALS', passwordVariable: 'KNIME_SNOWFLAKE_PASSWORD', usernameVariable: 'KNIME_SNOWFLAKE_USER')
                    ]) {
                        // define maven properties to override
                       testParams = "-Dknime.snowflake.enable=true -Dknime.snowflake.account=knimepartner.eu-central-1 -Dknime.snowflake.password=${KNIME_SNOWFLAKE_PASSWORD}"

                        // run tests
                        withEnv(["TEST_PARAMS=${testParams}"]) {
                            sh '''
                               cd knime-snowflake
                               mvn -Dmaven.test.failure.ignore=true -Dknime.p2.repo=${P2_REPO} ${TEST_PARAMS} clean verify -P snowflake-test -X -e
                            '''
                        }

                        // Collect test results
                        junit allowEmptyResults: true, testResults: '**/target/surefire-reports/TEST-*.xml'
                    }
                }
            }
        } catch (ex) {
            currentBuild.result = 'FAILURE'
            throw ex
        }
    }
}
/* vim: set shiftwidth=4 expandtab smarttab: */
