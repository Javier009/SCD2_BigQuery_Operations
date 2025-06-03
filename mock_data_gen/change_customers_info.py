import json
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker('en_US')

customer_attributes = ["customer_id",
                       "customer_name",
                       "customer_address",
                       "customer_city",
                       "customer_state",
                       "customer_zip",
                       "customer_tier",
                       "customer_email"]

import random
from faker import Faker

fake = Faker()


def customers_info_change(customers_info: list) -> list:
    updated_customers = []

    CHANGEABLE_ATTRIBUTES = [
        "customer_state",
        "customer_city",
        "customer_address",
        "customer_zip",
        "customer_tier",
        "customer_email",
    ]

    for cust in customers_info:
        current_customer = cust.copy()

        num_attrs_to_target = random.randint(1, max(1, len(CHANGEABLE_ATTRIBUTES)))

        targeted_attributes = random.sample(CHANGEABLE_ATTRIBUTES, num_attrs_to_target)

        if "customer_state" in targeted_attributes:
            current_customer["customer_state"] = fake.state_abbr()
            current_customer["customer_city"] = fake.city()
            current_customer["customer_address"] = fake.street_address()
            current_customer["customer_zip"] = fake.postcode()
            handled_geo_attributes = ["customer_state", "customer_city", "customer_address", "customer_zip"]
            targeted_attributes = [attr for attr in targeted_attributes if attr not in handled_geo_attributes]

        elif "customer_city" in targeted_attributes:
            current_customer["customer_city"] = fake.city()
            current_customer["customer_address"] = fake.street_address()
            current_customer["customer_zip"] = fake.postcode()
            handled_geo_attributes = ["customer_city", "customer_address", "customer_zip"]
            targeted_attributes = [attr for attr in targeted_attributes if attr not in handled_geo_attributes]

        elif "customer_address" in targeted_attributes:
            current_customer["customer_address"] = fake.street_address()
            current_customer["customer_zip"] = fake.postcode()
            handled_geo_attributes = ["customer_address", "customer_zip"]
            targeted_attributes = [attr for attr in targeted_attributes if attr not in handled_geo_attributes]

        elif "customer_zip" in targeted_attributes:
            current_customer["customer_zip"] = fake.postcode()
            targeted_attributes = [attr for attr in targeted_attributes if attr != "customer_zip"]

        for att in targeted_attributes:
            if att == "customer_tier":
                current_customer[att] = random.choice(['Bronze', 'Silver', 'Gold', 'Platinum'])
            elif att == "customer_email":
                current_customer[att] = fake.email()

        updated_customers.append(current_customer)

    return updated_customers

# def customers_info_change(customers_info:list) -> list:
#     updated_customers = []
#     for cust in customers_info:
#         atributes_to_change = random.sample(customer_attributes, random.randint(2, len(customer_attributes)))
#         atributes_to_change = [a for a in atributes_to_change if a not in ['customer_id','customer_name', 'customer_city', 'customer_zip', "customer_address"] ]
#         # -- Change attributes to something else --
#         staging_attributes = {"customer_name": fake.name(),
#                               "customer_address": fake.street_address(),
#                               "customer_city": fake.city(),
#                               "customer_state": fake.state_abbr(),
#                               "customer_zip": fake.postcode(),
#                               "customer_tier": random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
#                               "customer_email": fake.email()
#         }
#         for att in atributes_to_change:
#             cust[att] = staging_attributes[att]

#         updated_customers.append(cust)
#     return updated_customers
