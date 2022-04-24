from json import load, JSONDecodeError


class SettingsError(Exception):
    pass


class Settings:
    def __init__(self, testbed, key_name, key_path, consensus_port, mempool_port,
                 front_port, request_port, repo_name, repo_url, branch, instance_type, aws_regions, validity_duration):
        if isinstance(aws_regions, list):
            regions = aws_regions
        else:
            [aws_regions]

        inputs_str = [
            testbed, key_name, key_path, repo_name, repo_url, branch, instance_type
        ]
        inputs_str += regions
        inputs_int = [consensus_port, mempool_port, front_port, request_port, validity_duration]
        ok = all(isinstance(x, str) for x in inputs_str)
        ok &= all(isinstance(x, int) for x in inputs_int)
        ok &= len(regions) > 0
        if not ok:
            raise SettingsError('Invalid settings types')

        self.testbed = testbed

        self.key_name = key_name
        self.key_path = key_path

        self.consensus_port = consensus_port
        self.mempool_port = mempool_port
        self.front_port = front_port
        self.request_port = request_port

        self.repo_name = repo_name
        self.repo_url = repo_url
        self.branch = branch

        self.diem_name = 'diem'
        self.diem_url = 'https://github.com/diem/diem.git'
        self.diem_branch = 'main'

        self.instance_type = instance_type
        self.aws_regions = regions

        self.validity_duration = validity_duration

    @classmethod
    def load(cls, filename):
        try:
            with open(filename, 'r') as f:
                data = load(f)

            return cls(
                data['testbed'],
                data['key']['name'],
                data['key']['path'],
                data['ports']['consensus'],
                data['ports']['mempool'],
                data['ports']['front'],
                data['ports']['request'],
                data['repo']['name'],
                data['repo']['url'],
                data['repo']['branch'],
                data['instances']['type'],
                data['instances']['regions'],
                data['validity']['duration'],
            )
        except (OSError, JSONDecodeError) as e:
            raise SettingsError(str(e))

        except KeyError as e:
            raise SettingsError(f'Malformed settings: missing key {e}')
