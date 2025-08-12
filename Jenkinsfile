#!groovy
def BN = (BRANCH_NAME == 'master' || BRANCH_NAME.startsWith('releases/')) ? BRANCH_NAME : 'releases/2025-07'

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

void collectCoverageData(String dbName) {
    withEnv(["DB_NAME=${dbName}"]) {
        sh(script: '''
            cd knime-database
            dirs_with_runs=$(find . -name jacoco*.exec -exec dirname {} \\;)

            # copy coverage.xml to root dir for non-empty reports
            if [[ -n $dirs_with_runs ]]; then
                reports=$(find . -name jacoco.xml | grep "${dirs_with_runs}" | cut -c3-)

                for r in $reports
                do
                    cp "$r" "jacoco-junit-${DB_NAME}-${r//\\//-}"
                done
            fi
            ''', label: 'collect coverage files')
    }
    // stash coverage data
    stash name: dbName, includes: 'knime-database/jacoco-*.xml'
}

try {
    // parallel build steps
    parallel buildConfigurations

    withCredentials([usernamePassword(credentialsId: 'SNOWFLAKE_TESTING_CREDENTIALS', passwordVariable: 'KNIME_SNOWFLAKE_PASSWORD', usernameVariable: 'KNIME_SNOWFLAKE_USER')]) {
        withEnv([ 'KNIME_SNOWFLAKE_ADDRESS=knimepartner.eu-central-1:1234' ]) {
            workflowTests.runTests(
                dependencies: [
                    repositories: [
                        'knime-snowflake',
                        'knime-bigdata-externals',
                        'knime-bigdata',
                        'knime-aws',
                        'knime-cloud',
                        'knime-credentials-base',
                        'knime-database-proprietary',
                        'knime-database',
                        'knime-datageneration',
                        'knime-distance',
                        'knime-ensembles',
                        'knime-expressions',
                        'knime-filehandling',
                        'knime-gateway',
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
        configs = workflowTests.ALL_CONFIGURATIONS.collect() + ['snowflake']

        workflowTests.runSonar()
    }
} catch (ex) {
    currentBuild.result = 'FAILURE'
    throw ex
} finally {
    notifications.notifyBuild(currentBuild.result)
}

def dbTest() {
    node('maven && java17') {
        try {
            stage('Checkout Sources') {
                def branchName = env.CHANGE_BRANCH ?: env.BRANCH_NAME
                checkout([
                    $class: 'GitSCM',
                    branches: [[name: branchName]],
                    extensions: [[$class: 'RelativeTargetDirectory', relativeTargetDir: 'knime-snowflake'], [$class: 'GitLFSPull']],
                    userRemoteConfigs: [[ credentialsId: 'bitbucket-jenkins', url: 'https://bitbucket.org/KNIME/knime-snowflake' ]]
                ])
            }

            // verification
            stage('Testing Snowflake: ') {
                def branchName = env.CHANGE_BRANCH ?: env.BRANCH_NAME ?: 'master'

                def repoName = 'knime-database'
                env.lastStage = env.STAGE_NAME

                def branchExists = knimetools.checkIfBranchExistsInRemote(repoName: repoName, branchName: branchName)

                if (!branchExists) {
                    echo "Branch '${branchName}' does not exist. Falling back to 'master'."
                    branchName = 'master'
                }

                checkout([
                    $class: 'GitSCM',
                    branches: [[name: branchName ]],
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
                            sh(script: '''
                                cd knime-snowflake
                                mvn -Dmaven.test.failure.ignore=true -Dknime.p2.repo=${P2_REPO} ${TEST_PARAMS} clean verify -P snowflake-test,test  -X -e
                            ''', label: 'run snowflake db tests')
                        }
                        collectCoverageData('snowflake')

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
