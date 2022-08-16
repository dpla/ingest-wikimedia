$script = <<-'SCRIPT'

echo "These are my \"quotes\"! I am provisioning my guest."

sudo add-apt-repository ppa:deadsnakes/ppa

echo "apt update"
sudo apt-get update

echo "apt install python3.8"
sudo apt-get install -y python3.8
sudo apt-get install -y python3.8-dev
sudo apt-get install -y libsnappy-dev

echo "install python3-setuptools"
sudo apt-get -y install python3-setuptools
echo "install pip"
sudo apt-get -y install python3-pip python3-dev
echo "install virtualenv"
sudo pip3 install virtualenv  # will be py3
echo "upgrade pip"
python3 -m pip install --upgrade pip

echo "checkout ingest-wikimedia"
git clone https://github.com/dpla/ingest-wikimedia.git
cd ./ingest-wikimedia

echo "install virtualenv"
virtualenv -p /usr/bin/python3.8 venv
source venv/bin/activate
pip3 install --use-feature=2020-resolver -r requirements.txt

SCRIPT

class Hash
  def slice(*keep_keys)
    h = {}
    keep_keys.each { |key| h[key] = fetch(key) if has_key?(key) }
    h
  end unless Hash.method_defined?(:slice)
  def except(*less_keys)
    slice(*keys - less_keys)
  end unless Hash.method_defined?(:except)
end

Vagrant.configure("2") do |config|
  # config.vm.box = "ubuntu/bionic64"
  config.vm.box = "dummy"
  config.vm.provider :aws do |aws, override|
    aws.access_key_id = ENV['AWS_ACCESS_KEY_ID']
    aws.secret_access_key = ENV['AWS_SECRET_ACCESS_KEY']
    aws.ami = 'ami-00ddb0e5626798373'
    aws.iam_instance_profile_name = 'ingestion3-spark'
    aws.security_groups = ['sg-58f1803d', 'sg-07cfcb840697354e3'] # default, staff-jenkins-flintrock-access
    aws.subnet_id = "subnet-e8c8f3c0"
    aws.ssh_host_attribute = :private_ip_address
    aws.keypair_name = "general"
    aws.instance_type = "r5.large"

    override.ssh.username = "ubuntu"
    override.vm.synced_folder ".", "/vagrant", disabled: true
    override.ssh.private_key_path = "~/.ssh/aws_general.pem"
  end
  config.vm.provision "shell", inline: $script
end
