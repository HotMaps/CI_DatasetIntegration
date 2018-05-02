# python 3.5
from datetime import datetime
import gitlab
import os.path
import git
from git import Repo

repositories_base_path = '/var/hotmaps/repositories'
repository_name = ''
repository_path = os.path.join(repositories_base_path, repository_name)

# check repository on gitlab
#repo_date = datetime.utcnow()-timedelta(days=1)
repo_date = datetime(2010, 1, 1, 0, 0, 0)
dateStr = repo_date.isoformat(sep='T')+'Z'
gl = gitlab.Gitlab('https://gitlab.com', private_token='f-JzjmRRnxzwqC5o3zsQ')

hotmapsGroups = []
listOfRepositories = []

allGroups = gl.groups.list()

group = gl.groups.get('1354895')
hotmapsGroups.append(group)
#print(group.id)

subgroups = group.subgroups.list()


# Add all subgroups in the groups list as groups
for subgroup in subgroups:
    hotmapsGroups.append(gl.groups.get(subgroup.id, lazy=True))


for group in hotmapsGroups:
        projects = group.projects.list(all=True)
        #print(projects)

        for project in projects:

            proj = gl.projects.get(id=project.id)

            commits = proj.commits.list(since=dateStr)
            #print(proj)
            try:
               #f = proj.files.get(file_path='datapackage.json', ref='master')
               #print(f.content)

               if len(commits) == 0:
                   print('No commit')
               else:
                   repository_name = proj.name
                   repository_path = os.path.join(repositories_base_path, repository_name)
                   listOfRepositories.append(proj.name)
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
                       url = proj.http_url_to_repo
                       #print(url)
                       Repo.clone_from(url, repository_path)
                       print('successfuly cloned repository')
            except:
                print("No datapackage.json or in the wrong place")


listOfRepositories.append('.git')
print(listOfRepositories)
print(len(listOfRepositories))