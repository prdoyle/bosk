@Library('jenkins-common-pipeline')
import org.vena.shared.VenaCommon

def STATUS_BUILD = 'bosk.build'
def STATUS_SPOTLESS_CHECK = 'bosk.spotless.check'

pipeline {
    // Runs this job on a Jenkins worker machine (not the primary)
    agent {
        label 'linux-builder'
    }

    // By default each stage will do a git checkout, this would skip that
    options {
        skipDefaultCheckout true
        datadog(tags: [
            'service:boskLibrary',
            "SHA:${params.SHA}"
        ])
    }

    parameters {
        string(name: 'SHA',
               defaultValue: '',
               description: 'Build the sha'
        )
    }

    // Making a comment on a pull request with follow exact wording will trigger the build on that PR.
    triggers {
        issueCommentTrigger('full test')
    }

    stages {
        stage('Checkout Branch') {
            steps {
                script {
                    env.CURRENT_COMMIT_SHA = VenaCommon.checkoutMainSource(this)

                    // Setting job build information for better high level context of build
                    currentBuild.description = "bosk : ${env.CURRENT_COMMIT_SHA}"

                    VenaCommon.setupGitReferences(this)
                }
            }
        }

        stage('Build & Test') {
            steps {
                script {
                    VenaCommon.publishCustomStatus(this, 'pending', STATUS_BUILD, 'Running...', 'display/redirect')
                }

                sh './gradlew build'
                jacoco(
                    execPattern: '**/build/jacoco/**.exec',
                    classPattern: '**/build/classes',
                    sourcePattern: '**/src/main/java',
                    exclusionPattern: '**/src/test*'
                )

                script { VenaCommon.publishCustomStatus(this, 'success', STATUS_BUILD, 'Passed', 'display/redirect') }
            }

            post {
                always {
                    junit testResults: '**/build/test-results/test/*.xml', allowEmptyResults: false
                    script {
                        VenaCommon.publishTestResults(this)
                        VenaCommon.runSpotBugsAnalysis(this, '**/build/reports/spotbugs/**.xml')
                    }
                }

                failure {
                    script { VenaCommon.publishCustomStatus(this, 'failure', STATUS_BUILD, 'Failed', 'display/redirect') }
                }
            }
        }

        stage('Spotless') {
            steps {
                script { VenaCommon.publishCustomStatus(this, 'pending', STATUS_SPOTLESS_CHECK, 'Running...', 'display/redirect') }
                sh './gradlew spotlessCheck'
                script { VenaCommon.publishCustomStatus(this, 'success', STATUS_SPOTLESS_CHECK, 'Passed', 'display/redirect') }
            }

            post {
                failure {
                    script { VenaCommon.publishCustomStatus(this, 'failure', STATUS_SPOTLESS_CHECK, 'Failed', 'display/redirect') }
                }
            }
        }

        stage('Publish to Artifactory') {
            // when { branch 'main' }
            steps {
                rtServer(
                    id: 'vena-artifactory-server',
                    url: 'http://artifactory.devops.vena.vpn:8081/artifactory',
                    credentialsId: 'artifactory-automation-credentials'
                )

                rtGradleDeployer(
                    id: 'GRADLE_DEPLOYER',
                    serverId: 'vena-artifactory-server',
                    releaseRepo: 'libs-release-local',
                    snapshotRepo: 'libs-snapshot-local'
                )

                rtGradleResolver(
                    id: 'GRADLE_RESOLVER',
                    serverId: 'vena-artifactory-server',
                )

                rtGradleRun (
                    useWrapper: true,
                    usesPlugin: true,
                    buildFile: 'build.gradle',
                    tasks: 'clean artifactoryPublish',
                    deployerId: "GRADLE_DEPLOYER",
                    resolverId: "GRADLE_RESOLVER"
                )

                rtPublishBuildInfo(
                    serverId: 'vena-artifactory-server'
                )
            }
        }

    }

    post {
        success {
            script{
                VenaCommon.publishCustomStatus(this, 'success', 'Pre-Merge Build & Tests', 'All tests are passing', 'testResults')
            }
        }

        failure {
            script {
                VenaCommon.publishCustomStatus(this, 'failure', 'Pre-Merge Build & Tests', 'One or more tests failed', 'testResults')
            }
        }

        always {
            script {
                VenaCommon.cleanUpWorkspace(this)
            }
        }
    }
}
