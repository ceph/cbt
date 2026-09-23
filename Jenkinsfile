#!/usr/bin/env groovy

pipeline {
  agent {
    label 'python3'
  }
  options {
    timeout(time: 1, unit: 'HOURS')
  }
  stages {
    stage ('flake8') {
      steps {
        sh '''#!/usr/bin/env bash
          virtualenv -q --python python3 venv
          . venv/bin/activate
          pip install tox
          tox -e pep8 -- --tee --output-file=flake8.txt
        '''
      }
    }
  }
  post {
    failure {
      script {
        def flake8_issues = scanForIssues tool: flake8(pattern:'flake8.txt')
        publishIssues id: 'cbt-flake8', name: 'cbt-flake8', issues: [flake8_issues]
      }
    }
  }
}
