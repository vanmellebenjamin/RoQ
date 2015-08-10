#!/usr/bin/env bash

cd $(dirname $0)

# Kill running ROQ processes
kill -9 `ps -ef | grep roq-management | awk '{print $2}'`
kill -9 `ps -ef | grep roq-demonstration | awk '{print $2}'`
kill -9 `ps -ef | grep roq-simulation | awk '{print $2}'`

# Kill ZK container
sudo docker stop ROQZK
sudo docker rm ROQZK

# Create inventory for Ansible
echo [localSystem] > ../ansible/testInventory
echo localhost ansible_connection=local >> ../ansible/testInventory
echo [demo] >> ../ansible/testInventory

# Build the zookeeper docker image
sudo docker build -t debian:zookeeper ../docker/zookeeper

echo Starting zookeeper container
# Launch Zookeeper container
sudo docker run --name ROQZK -d debian:zookeeper

# Update inventory
echo GCM.roq.org ansible_ssh_host=127.0.0.1 >> ../ansible/testInventory
echo ZK.roq.org ansible_ssh_host=$(sudo docker inspect --format '{{ .NetworkSettings.IPAddress }}' ROQZK) \
		ansible_ssh_user=root >> ../ansible/testInventory

# Launch the ansible playbook that update Zk address in the GCM.properties
ansible-playbook -i ../ansible/testInventory ../ansible/gimmeROQ.yml --tags "update-gcm,update-hcm"

mvn install -DskipTests -f ../../pom.xml

echo starting GCM
# Start GCM
java -Djava.library.path=/usr/local/lib \
		-cp ../../roq-management/target/roq-management-1.0-SNAPSHOT-jar-with-dependencies.jar \
		org.roqmessaging.management.launcher.GlobalConfigurationLauncher \
		~/.roq/GCM.properties > GCM.log &

sleep 10

echo starting HCM
# Start HCM
java -Djava.library.path=/usr/local/lib \
		-cp ../../roq-management/target/roq-management-1.0-SNAPSHOT-jar-with-dependencies.jar \
		org.roqmessaging.management.launcher.HostConfigManagerLauncher \
		~/.roq/HCM.properties > HCM.log &