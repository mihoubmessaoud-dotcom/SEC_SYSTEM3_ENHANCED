# Git Operations Guide

## Current state
- Repository initialized
- Baseline tag exists: `v4-definitive`

## 1) Verify local state
```powershell
git status
git log --oneline --decorate -n 5
git tag --list
```

## 2) Push project to remote (GitHub/GitLab)
```powershell
.\tools\push_release.ps1 -RemoteUrl "<REMOTE_URL>" -Branch "master" -Tag "v4-definitive"
```

## 3) Future release flow
1. Implement changes
2. Run baseline tests
   - `python -m unittest tests.test_release_v4_baseline -v`
3. Commit
4. Create new tag
   - `git tag -a vX.Y.Z -m "release vX.Y.Z"`
5. Push branch + tags
   - `git push origin master`
   - `git push origin --tags`
