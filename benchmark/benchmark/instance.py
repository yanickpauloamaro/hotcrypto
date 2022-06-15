import boto3
from botocore.exceptions import ClientError
from collections import defaultdict, OrderedDict
from time import sleep

from datetime import datetime, timedelta
from benchmark.utils import Print, BenchError, progress_bar
from benchmark.settings import Settings, SettingsError
from typing import NamedTuple

class InstanceIp(NamedTuple):
    public: str
    private: str

class AWSError(Exception):
    def __init__(self, error):
        assert isinstance(error, ClientError)
        self.message = error.response['Error']['Message']
        self.code = error.response['Error']['Code']
        super().__init__(self.message)


class InstanceManager:
    def __init__(self, settings):
        assert isinstance(settings, Settings)
        self.settings = settings
        self.clients = OrderedDict()
        Print.info(f'Regions: {settings.aws_regions}')
        for region in settings.aws_regions:
            self.clients[region] = boto3.client('ec2', region_name=region)

    @classmethod
    def make(cls, settings_file='settings.json'):
        try:
            return cls(Settings.load(settings_file))
        except SettingsError as e:
            raise BenchError('Failed to load settings', e)

    # Returns both public and private IPs of the instances
    def _get(self, state):
        # Possible states are: 'pending', 'running', 'shutting-down',
        # 'terminated', 'stopping', and 'stopped'.
        ids, ips = defaultdict(list), defaultdict(list)

        for region, client in self.clients.items():
            r = client.describe_instances(
                Filters=[
                    {
                        'Name': 'instance-state-name',
                        'Values': state
                    },
                    {
                        'Name': 'key-name',
                        'Values': [self.settings.key_name]
                    },
                    {
                        'Name': 'instance-lifecycle',
                        'Values': ['spot']
                    },
                ]
            )
            instances = [y for x in r['Reservations'] for y in x['Instances']]

            for x in instances:
                ids[region] += [x['InstanceId']]
                if 'PublicIpAddress' in x and 'PrivateIpAddress' in x:
                    public = x['PublicIpAddress']
                    private = x['PrivateIpAddress']
                    ips[region] += [InstanceIp(public, private)]
                    
        return ids, ips
        
    def _wait(self, state):
        # Possible states are: 'pending', 'running', 'shutting-down',
        # 'terminated', 'stopping', and 'stopped'.
        while True:
            sleep(1)
            ids, _ = self._get(state)
            if sum(len(x) for x in ids.values()) == 0:
                break

    def _create_security_group(self, client):
        client.create_security_group(
            Description='HotStuff node',
            GroupName=self.settings.testbed,
        )

        client.authorize_security_group_ingress(
            GroupName=self.settings.testbed,
            IpPermissions=[
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 22,
                    'ToPort': 22,
                    'IpRanges': [{
                        'CidrIp': '0.0.0.0/0',
                        'Description': 'Debug SSH access',
                    }],
                    'Ipv6Ranges': [{
                        'CidrIpv6': '::/0',
                        'Description': 'Debug SSH access',
                    }],
                },
                {
                    'IpProtocol': 'tcp',
                    'FromPort': self.settings.consensus_port,
                    'ToPort': self.settings.consensus_port,
                    'IpRanges': [{
                        'CidrIp': '0.0.0.0/0',
                        'Description': 'Consensus port',
                    }],
                    'Ipv6Ranges': [{
                        'CidrIpv6': '::/0',
                        'Description': 'Consensus port',
                    }],
                },
                {
                    'IpProtocol': 'tcp',
                    'FromPort': self.settings.mempool_port,
                    'ToPort': self.settings.mempool_port,
                    'IpRanges': [{
                        'CidrIp': '0.0.0.0/0',
                        'Description': 'Mempool port',
                    }],
                    'Ipv6Ranges': [{
                        'CidrIpv6': '::/0',
                        'Description': 'Mempool port',
                    }],
                },
                {
                    'IpProtocol': 'tcp',
                    'FromPort': self.settings.front_port,
                    'ToPort': self.settings.front_port,
                    'IpRanges': [{
                        'CidrIp': '0.0.0.0/0',
                        'Description': 'Front end to accept clients transactions',
                    }],
                    'Ipv6Ranges': [{
                        'CidrIpv6': '::/0',
                        'Description': 'Front end to accept clients transactions',
                    }],
                },
                {
                    'IpProtocol': 'tcp',
                    'FromPort': self.settings.request_port,
                    'ToPort': self.settings.request_port,
                    'IpRanges': [{
                        'CidrIp': '0.0.0.0/0',
                        'Description': 'Front end to accept clients requests',
                    }],
                    'Ipv6Ranges': [{
                        'CidrIpv6': '::/0',
                        'Description': 'Front end to accept clients requests',
                    }],
                },
            ]
        )

    def _get_ami(self, client, default_ami='ami-0c6ebbd55ab05f070'):

        # The AMI changes with regions.
        response = client.describe_images(
            Filters=[{
                'Name': 'description',
                'Values': ['Canonical, Ubuntu, 20.04 LTS, amd64 focal image build on 2020-10-26']
            }]
        )
        return response['Images'][0]['ImageId']


    def create_instances(self, nodes):
        assert isinstance(nodes, int) and nodes > 0

        ## NB: Default security group has all ports open already
        # # Create the security group in every region.
        # for client in self.clients.values():
        #     try:
        #         self._create_security_group(client)
        #     except ClientError as e:
        #         error = AWSError(e)
        #         if error.code != 'InvalidGroup.Duplicate':
        #             raise BenchError('Failed to create security group', error)

        try:
            # Create all instances.
            size = nodes * len(self.clients)

            if size > 100:
                res = input(f"Are you sure you want to create {size} {self.settings.instance_type} instances? (yes or no)")
                if res.lower() != 'yes':
                    print("Canceling instance creation")
                    return

            progress = progress_bar(
                self.clients.values(), prefix=f'Creating {size} instances ({nodes} nodes per region)'
            )

            start = datetime.now()
            duration = self.settings.validity_duration
            end = start + timedelta(hours=duration)

            formatted = end.strftime('%d-%m-%Y %H:%M')
            Print.info(f'Spot instances will be valid until {formatted}')

            Print.info(f'Making a spot instance request')
            for client in progress:
                response = client.request_spot_instances(
                    # DryRun = True,
                    # Type='one-time',
                    InstanceCount = nodes,
                    ValidUntil = end,
                    TagSpecifications=[{
                        'ResourceType': 'spot-instances-request',
                        'Tags': [{
                            'Key': 'Name',
                            'Value': self.settings.testbed
                        }]
                    }],
                    LaunchSpecification = {
                        'BlockDeviceMappings': [{
                            'DeviceName': '/dev/sda1',
                            'Ebs': {
                                'VolumeType': 'gp2',
                                'VolumeSize': 200,
                                'DeleteOnTermination': True
                            }
                        }],
                        # 'EbsOptimized': True,
                        'ImageId': self._get_ami(client),
                        'InstanceType': self.settings.instance_type,
                        'KeyName': self.settings.key_name,
                    },
                )

            # Wait for the instances to boot.
            Print.info('Waiting for all instances to boot...')
            self._wait(['pending'])
            Print.heading(f'Successfully created {size} new instances')
        except ClientError as e:
            raise BenchError('Failed to create AWS instances', AWSError(e))

    def terminate_instances(self):
        try:
            ids, _ = self._get(['pending', 'running', 'stopping', 'stopped'])
            size = sum(len(x) for x in ids.values())
            if size == 0:
                Print.heading(f'All instances are shut down')
                return

            # Terminate instances.
            for region, client in self.clients.items():
                if ids[region]:
                    client.terminate_instances(InstanceIds=ids[region])

            # Wait for all instances to properly shut down.
            Print.info('Waiting for all instances to shut down...')
            self._wait(['shutting-down'])
            
            ## NB: No security group was created
            # for client in self.clients.values():
            #     client.delete_security_group(
            #         GroupName=self.settings.testbed
            #     )

            Print.heading(f'Testbed of {size} instances destroyed')
        except ClientError as e:
            raise BenchError('Failed to terminate instances', AWSError(e))

    def reduce_instances(self, per_region = lambda n : n):
        destroyed = 0
        try:
            ids, _ = self._get(['pending', 'running'])
            size = sum(len(x) for x in ids.values())
            if size == 0:
                Print.heading(f'All instances are shut down')
                return

            # Terminate instances.
            for region, client in self.clients.items():
                if ids[region]:
                    nb = int(per_region(len(ids[region])))
                    to_terminate = ids[region][:nb]
                    
                    client.terminate_instances(InstanceIds=to_terminate)
                    destroyed += len(to_terminate)

            # Wait for all instances to properly shut down.
            Print.info('Waiting for all instances to shut down...')
            self._wait(['shutting-down'])
            
            ## NB: No security group was created
            # for client in self.clients.values():
            #     client.delete_security_group(
            #         GroupName=self.settings.testbed
            #     )

            Print.heading(f'Testbed size was reduced by {destroyed}')
        except ClientError as e:
            raise BenchError('Failed to terminate instances', AWSError(e))

    # Cancel openned spot requests
    def cancel_spot_request(self):
        try:
            for region, client in self.clients.items():
                response = client.describe_spot_instance_requests(
                    Filters=[
                        {
                            'Name': 'launch.key-name',
                            'Values': [self.settings.key_name]
                        },
                    ]
                )
                
                request_ids = [request['SpotInstanceRequestId'] for request in response['SpotInstanceRequests']]

                response = client.cancel_spot_instance_requests(
                    SpotInstanceRequestIds=request_ids
                )

                Print.info(f'Canceling the following spot requests in {region}: {request_ids}')
        except ClientError as e:
            raise BenchError('Failed to cancel spot requests', AWSError(e))

    def start_instances(self, max):
        size = 0
        try:
            ids, _ = self._get(['stopping', 'stopped'])
            for region, client in self.clients.items():
                if ids[region]:
                    target = ids[region]
                    target = target if len(target) < max else target[:max]
                    size += len(target)
                    client.start_instances(InstanceIds=target)
            Print.heading(f'Starting {size} instances')
        except ClientError as e:
            raise BenchError('Failed to start instances', AWSError(e))

    def stop_instances(self):
        try:
            ids, _ = self._get(['pending', 'running'])
            for region, client in self.clients.items():
                if ids[region]:
                    client.stop_instances(InstanceIds=ids[region])
            size = sum(len(x) for x in ids.values())
            Print.heading(f'Stopping {size} instances')
        except ClientError as e:
            raise BenchError('Failed to stop instances', AWSError(e))

    def hosts(self, flat=False):
        try:
            _, ips = self._get(['pending', 'running'])
            return [x for y in ips.values() for x in y] if flat else ips
        except ClientError as e:
            raise BenchError('Failed to gather instances IPs', AWSError(e))

    def print_info(self):
        hosts = self.hosts()
        key = self.settings.key_path
        text = ''
        for region, ips in hosts.items():
            text += f'\n Region: {region.upper()}\n'
            for i, ip in enumerate(ips):
                new_line = '\n' if (i+1) % 6 == 0 else ''
                text += f'{new_line} {i}\tssh -i {key} ubuntu@{ip.public}\n'
        print(
            '\n'
            '----------------------------------------------------------------\n'
            ' INFO:\n'
            '----------------------------------------------------------------\n'
            f' Available machines: {sum(len(x) for x in hosts.values())}\n'
            f'{text}'
            '----------------------------------------------------------------\n'
        )
