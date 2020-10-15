#!/usr/bin/env python3

import datetime
import logging

import github3
import github3.exceptions

log = logging.getLogger("cephacheck.check")


class UTC(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return datetime.timedelta(0)


utc = UTC()


class Check:
    def __init__(self, owner, project, context, pem, app_id, install_id,
                 sha, details_url=None, external_id=None):
        self.owner = owner
        self.project = project
        self.context = context
        self.sha = sha
        self.details_url = details_url
        self.external_id = external_id
        self.github = github3.GitHub()
        self.github.login_as_app_installation(pem, app_id, install_id)
        self.check_run = None

    def start(self, status, output=None):
        repo = self.github.repository(self.owner, self.project)
        started_at = datetime.datetime.now(utc).isoformat()
        if output is None:
            output = {'title': 'Summary',
                      'summary': 'started',
                      'text': 'details'}
        try:
            self.check_run = repo.create_check_run(
                name=self.context,
                head_sha=self.sha,
                status=status,
                started_at=started_at,
                output=output,
                details_url=self.details_url,
                external_id=self.external_id,
            )
        except github3.exceptions.GitHubException as e:
            log.error(f"failed to create check run {self.context} for #{self.sha}:"
                      f" {e}")

    def update(self, output):
        # update an existing one
        log.debug(f"updating existing check run {self.context} for #{self.sha}")
        try:
            self.check_run.update(
                status='in_progress',
                output=output,
                details_url=self.details_url,
                external_id=self.external_id,
            )
        except github3.exceptions.GitHubException as e:
            log.error(f"failed to update check run {self.context} for "
                      f"#{self.sha}: {e}")

    # conclusion: string: one of
    #   success, failure, neutral, cancelled, skipped, timed_out, or
    #   action_required
    #   see https://developer.github.com/v3/checks/runs/#create-a-check-run
    # output: dict
    #   see https://developer.github.com/v3/checks/runs/#output-object
    def complete(self, conclusion, output):
        repo = self.github.repository(self.owner, self.project)
        status = 'completed'
        completed_at = datetime.datetime.now(utc).isoformat()
        try:
            self.check_run = next(c for c in repo.commit(self.sha).check_runs()
                                  if c.name == self.context)
        except github3.exceptions.GitHubException as e:
            log.error(f"could not retrieve existing check runs for #{self.sha}:"
                      f" {e}")
        except StopIteration:
            # create a new one
            log.debug(f"could not find existing check runs {self.context} for "
                      f"#{self.sha}, creating a new one")
            try:
                self.check_run = repo.create_check_run(
                    name=self.context,
                    head_sha=self.sha,
                    status=status,
                    conclusion=conclusion,
                    completed_at=completed_at,
                    output=output,
                    details_url=self.details_url,
                    external_id=self.external_id,
                )
            except github3.exceptions.GitHubException as e:
                log.error(f"failed to create check run {self.context} for "
                          f"#{self.sha}: {e}")
        else:
            # update an existing one
            log.debug(f"updating existing check run {self.context} for #{self.sha} "
                      f"with status {status}")
            try:
                self.check_run.update(
                    status=status,
                    conclusion=conclusion,
                    completed_at=completed_at,
                    output=output,
                    name=self.context,
                    details_url=self.details_url,
                    external_id=self.external_id,
                )
            except github3.exceptions.GitHubException as e:
                log.error(f"failed to update check run {self.context} for "
                          f"#{self.sha}: {e}")
