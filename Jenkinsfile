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
        dbTest(
            dbName: "mssqlserver",
            containerImage: "${dockerTools.ECR}/knime/mssql-server",
            envArgs: [ "ACCEPT_EULA=Y", "SA_PASSWORD=@SaPassword123", "MSSQL_DB=knime01", "MSSQL_USER=knime01", "MSSQL_PASSWORD=My@Super@Secret" ],
            ports: [1433],
        )
    }
]

try {
    // parallel build steps
    parallel buildConfigurations

    withEnv([ "KNIME_SNOWFLAKE_USER=sa", "KNIME_MSSQLSERVER_PASSWORD=@SaPassword123",
              "KNIME_ORACLE_USER=SYSTEM", "KNIME_ORACLE_PASSWORD=password"
    ]) {
        workflowTests.runTests(
            dependencies: [
                repositories: [
                    'knime-snowflake',
                    'knime-database',
                    'knime-database-proprietary',
                    'knime-kerberos',
                    'knime-testing-internal',
                    'knime-filehandling',
                    'knime-jep',
                    'knime-virtual',
                    'knime-datageneration',
                    'knime-timeseries',
                    'knime-cloud',
                    'knime-textprocessing',
                    'knime-jfreechart',
                    'knime-distance',
                    'knime-js-base',
                    'knime-expressions'
                ]
            ]
        )
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


def dbTest(Map args = [:]) {
    node("maven && java11") {
    
        try {
            // verification
            stage("Testing Snowflake: "){
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
                    withCredentials([usernamePassword(credentialsId: 'ARTIFACTORY_CREDENTIALS', passwordVariable: 'ARTIFACTORY_PASSWORD', usernameVariable: 'ARTIFACTORY_LOGIN')]) {
                        // define maven properties to override
                        def testParams
//add Snowflake properties                        
                       testParams = "-Dknime.snowflake.enable=true -Dknime.snowflake.account.....=${container.getContainerIP()}"

                        // run tests
                        withEnv(["TEST_PARAMS=${testParams}"]) {
                            sh '''
                               cd knime-snowflake
                               mvn -Dmaven.test.failure.ignore=true -Dknime.p2.repo=${P2_REPO} ${TEST_PARAMS} clean verify -P test -X -e
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
        } finally {
            // Stop and remove all sidecar containers
            sidecars.close()
        }
    }
}
/* vim: set shiftwidth=4 expandtab smarttab: */
