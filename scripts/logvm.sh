# ./logvm.sh $vmid

# #!/bin/bash

vmid=$1
HOST="fa24-cs425-73${vmid}.cs.illinois.edu"

USERNAME="abandal2"
PASSWORD_FILE="/Users/adityabandal/password.txt"
SCRIPT_INPUT=
if [[ "$vmid" == "01" ]]; then
    SCRIPT_INPUT=0
else
    SCRIPT_INPUT=1
fi

if [[ -f "$PASSWORD_FILE" ]]; then
    PASSWORD=$(<"$PASSWORD_FILE")
else
    echo "Password file not found!"
    exit 1
fi

echo $HOST

sshpass -p "$PASSWORD" ssh "$USERNAME@$HOST" bash -c "'
    cd g73_mp2
    git stash
    git fetch
    git checkout mp4-aditya
    git pull
    make cfiles
    make clean
    make
    bash scripts/rrun.sh $SCRIPT_INPUT
'"


