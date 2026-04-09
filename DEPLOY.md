## On the VPS

```bash
cd ~/bare
git clone --bare git@github.com:glostis/veuillez-nous-excuser-pour-la-gene-occasionnee.git
cd veuillez-nous-excuser-pour-la-gene-occasionnee.git
```

Put the following content in `hooks/post-receive`:
```bash
#!/usr/bin/env bash
WORK_TREE=$(pwd | sed 's@/bare/@/worktree/@' | sed 's@\.git@@')
mkdir -p $WORK_TREE

# Checkout code
git --work-tree=$WORK_TREE --git-dir="$(pwd)" checkout -f

COMMIT_SHA=$(git --git-dir="$(pwd)" rev-parse HEAD)
COMMIT_DATE=$(git --git-dir="$(pwd)" show --no-patch --format=%ci HEAD)

# Deploy with Docker
cd $WORK_TREE || exit
docker compose build --build-arg COMMIT_SHA="$COMMIT_SHA" --build-arg COMMIT_DATE="$COMMIT_DATE"
docker compose up -d
```

```bash
chmod +x hooks/post-receive
```

Add `DOMAIN=<your-domain-name>` in the `~/worktree/repo/.env` file.

## Locally

```bash
git remote add all origin
git remote set-url --add --push all git@github.com-perso:glostis/veuillez-nous-excuser-pour-la-gene-occasionnee.git
git remote set-url --add --push all user@vps-ip:/home/glostis/bare/veuillez-nous-excuser-pour-la-gene-occasionnee.git
```

with the following content in `~/.ssh/config`:
```
Host github.com-perso
    HostName github.com
    User git
    IdentityFile /github/perso/ssh/key

Host vps-ip
    HostName vps-ip
    User user
    IdentityFile /vps/ssh/key
```
