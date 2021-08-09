import random
import names
import json

class UserJSONEncoder((json.JSONEncoder)):
    def default(self, user):
        print('inside json encoder', user)
        return user.__dict__

class User:
    def __init__(self, id, username, email):
        self.id = id
        self.username = username
        self.email = email
                
    @classmethod
    def generate_random_users(cls, num) -> 'User':
        if num is None or num <= 0:
            raise AttributeError("'num' is invalid.")

        users = []
        for i in range(num):
            name = ''.join(names.get_full_name().split(' '))
            id = random.randint(1, 1000000)
            user = cls(id=id, username=name, email=name + f'_{id}@gmail.com')
            
            users.append(user)
            
        return users
    
    def __json__(self):
        return {'id':self.id, 'username':self.username, 'email':self.email}
    
    def __repr__(self):
        return str(self.__dict__)

    def __str__(self):
        return str(self.__dict__)

def main():
    users = User.generate_random_users(100)    
    with open('users.json', 'w') as json_file:
        json.dump(users, json_file, cls=UserJSONEncoder)

if __name__ == '__main__':
    main()