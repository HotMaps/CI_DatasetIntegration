# python 3.5
import datetime
import gitlab
import os.path
import git
from git import Repo

repositories_base_path = '/var/hotmaps/repositories'
repository_name = ''
repository_path = os.path.join(repositories_base_path, repository_name)

# check repository on gitlab
# date = datetime.datetime.utcnow()-datetime.timedelta(days=1)
date = datetime.datetime(2010, 1, 1, 0, 0, 0)
dateStr = date.isoformat(sep='T', timespec='seconds')+'Z'
gl = gitlab.Gitlab('https://gitlab.com', private_token='f-JzjmRRnxzwqC5o3zsQ')

group = gl.groups.get('1354895')

projects = group.projects.list(all=True)

for project in projects:

    proj = gl.projects.get(id=project.id)

    commits = proj.commits.list(since=dateStr)
    if len(commits) == 0:
        print('No commit')
    else:
        repository_name = proj.name
        repository_path = os.path.join(repositories_base_path, repository_name)
        print(repository_name)
        if os.path.exists(repository_path):
            # git pull
            print('update repository')
            g = git.cmd.Git(repository_path)
            g.pull()
            print('successfuly updated repository')

        else:
            # git clone
            print('clone repository')
            url = proj.ssh_url_to_repo
            Repo.clone_from(url, repository_path)
            print('successfuly cloned repository')