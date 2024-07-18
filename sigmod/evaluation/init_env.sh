#!/bin/bash

# server lists
SERVERS=(
    "ubuntu@iotdb1"
    "ubuntu@iotdb2"
    "ubuntu@iotdb3"
    "ubuntu@iotdb4"
    "ubuntu@iotdb5"
    "ubuntu@iotdb6"
    "ubuntu@iotdb7"
    "ubuntu@iotdb8"
    "ubuntu@iotdb9"
    "ubuntu@iotdb10"
    "ubuntu@iotdb11"
    "ubuntu@iotdb12"
    "ubuntu@iotdb13"
    "ubuntu@iotdb14"
    "ubuntu@iotdb15"
    "ubuntu@iotdb16"
    "ubuntu@iotdb17"
    "ubuntu@iotdb18"
    "ubuntu@iotdb19"
    "ubuntu@iotdb20"
    "ubuntu@iotdb21"
)
PASSWORD=""
IdentityFile="/home/ubuntu/.ssh/id_rsa.pub"

# ssh
for SERVER in "${SERVERS[@]}"; do
    echo "Copying public key to $SERVER..."
    sshpass -p "$PASSWORD" ssh-copy-id -i "$IdentityFile" "$SERVER"
    if [ $? -eq 0 ]; then
        echo "Public key copied successfully to $SERVER"
    else
        echo "Failed to copy public key to $SERVER"
    fi
done


for i in {0..1}; do  
    # new hostname
    NEW_HOSTNAME="iotdb$((i+20))"
    
    USERNAME=$(echo ${SERVERS[$i]} | cut -d'@' -f1)
    OLD_HOSTNAME=$(echo ${SERVERS[$i]} | cut -d'@' -f2)

    # change hostname
    echo "Changing hostname for ${USERNAME}@${OLD_HOSTNAME} to ${NEW_HOSTNAME}..."
    ssh ${USERNAME}@${OLD_HOSTNAME} "sudo hostnamectl set-hostname ${NEW_HOSTNAME}"

    ssh ${USERNAME}@${OLD_HOSTNAME} "sudo sed -i 's/^${OLD_HOSTNAME}/${NEW_HOSTNAME}/' /etc/hostname"
    ssh ${USERNAME}@${OLD_HOSTNAME} "sudo sed -i 's/${OLD_HOSTNAME}/${NEW_HOSTNAME}/g' /etc/hosts"

    if [ $? -eq 0 ]; then
        echo "Hostname changed successfully for ${USERNAME}@${OLD_HOSTNAME}"
    else
        echo "Failed to change hostname for ${USERNAME}@${OLD_HOSTNAME}"
    fi
done


# install Java 11
install_java() {
    ssh -o StrictHostKeyChecking=no $1 << EOF
        echo "Installing Java 11 on $(hostname)"
        sudo apt install -y openjdk-11-jdk
        java -version
EOF
}

for server in "${SERVERS[@]}"; do
    install_java $server
done
