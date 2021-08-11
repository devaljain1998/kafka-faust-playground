import random
import json
from pprint import pprint

class JobImpressionJSONEncoder((json.JSONEncoder)):
    def default(self, impression):
        return impression.__dict__
    
# helpers:
def get_user_ids():
    user_ids = []
    with open('users.json', 'r') as json_file:
        users = json.load(json_file)
        user_ids = list(map(lambda user: user['id'], users))

    return user_ids

class JobImpression:
    def __init__(self, user_id, job_id, idx: str = '1'):
        self.id = str(idx)
        self.user_id = user_id
        self.job_id = job_id
        self.event = 'event'
        self.key1 = f'k1{idx}'
        self.key2 = f'k2{idx}'
        self.key3 = f'k3{idx}'
                
    @classmethod
    def generate_random_impressions(cls, num: int = 1) -> 'JobImpression':

        impressions = []
        user_ids = get_user_ids()
        size = len(user_ids)
        
        for idx in range(num + 1):
            random_int = random.randint(0, size) % size
            user_id = user_ids[random_int]
            impression = cls(idx=idx, user_id=user_id, job_id=random.randint(1, 10101))
            impressions.append(impression)
            
        return impressions
    
    def __json__(self):
        return self.__dict__
    
    def __repr__(self):
        return str(self.__dict__)

    def __str__(self):
        return str(self.__dict__)

def main():
    impressions = JobImpression.generate_random_impressions(10000)
    with open('job_impressions.json', 'w') as json_file:
        json.dump(impressions, json_file, cls=JobImpressionJSONEncoder)
        print('Complete!')

if __name__ == '__main__':
    main()