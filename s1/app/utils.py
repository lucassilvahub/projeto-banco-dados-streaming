from faker import Faker
from random import randint, choice

faker = Faker()

def generate_fake_user():
    return {
        "id": faker.uuid4(),
        "name": faker.name(),
        "email": faker.email(),
        "created_at": faker.iso8601(),
        "address": faker.address(),
        "phone_number": faker.phone_number()
    }

def generate_fake_rating():
    return {
        "user_id": faker.uuid4(),
        "movie_id": faker.uuid4(),
        "rating": randint(1, 5),
        "comment": faker.sentence(),
        "timestamp": faker.iso8601(),
        "genre": choice(["Action", "Comedy", "Drama", "Horror"]), 
    }
