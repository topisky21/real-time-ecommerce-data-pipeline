import csv
from faker import Faker
import random
import string


# Specify number of employees to generate
num_employees = 100

# Create Faker instance
fake = Faker()

employee_data=[]

# Define the character set for the password
password_characters = string.ascii_letters + string.digits + 'm'


for _ in range(num_employees):
    employee = {
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "job_title": fake.job(),
        "department": fake.job(),  # Generate department-like data using the job() method
        "email": fake.email(),
        "address": fake.city(),
        "phone_number": fake.phone_number(),
        "salary": fake.random_number(digits=5),  # Generate a random 5-digit salary
        "password": ''.join(random.choice(password_characters) for _ in range(8))  # Generate an 8-character password with 'm'
    }
    employee_data.append(employee)

print(employee_data)


